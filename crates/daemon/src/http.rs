//! Optional HTTP endpoints for orchestrator probes and Prometheus scraping.
//! They serve the same local observations as `status` and `metrics.prom`;
//! nothing here is recovery authority or changes service state.
use crate::config::Config;
use anyhow::{Context, Result};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
};

const MAX_REQUEST_BYTES: usize = 8 << 10;
/// Probes and scrapers send a small head at once; slow or idle clients must
/// release their connection slot quickly.
const REQUEST_HEAD_TIMEOUT: Duration = Duration::from_secs(1);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_CONNECTIONS: usize = 256;

/// Aborts the listener when the command that started it returns.
pub(crate) struct Server(tokio::task::JoinHandle<()>);

impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Bind before starting work so a port conflict fails the command immediately.
pub(crate) async fn start(config: &Config) -> Result<Option<Server>> {
    let Some(http) = &config.http else {
        return Ok(None);
    };
    let listener = TcpListener::bind(http.listen)
        .await
        .with_context(|| format!("bind HTTP listener {}", http.listen))?;
    tracing::info!(listen = %listener.local_addr()?, "HTTP endpoints listening");
    Ok(Some(Server(tokio::spawn(serve(config.clone(), listener)))))
}

async fn serve(config: Config, listener: TcpListener) {
    let config = Arc::new(config);
    let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(error) => {
                tracing::warn!(%error, "HTTP accept failed");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        // Excess connections are closed rather than queued behind slow clients.
        let Ok(permit) = permits.clone().try_acquire_owned() else {
            continue;
        };
        let config = config.clone();
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(error) = tokio::time::timeout(REQUEST_TIMEOUT, handle(&config, stream)).await
            {
                tracing::debug!(%error, "HTTP request timed out");
            }
        });
    }
}

async fn handle(config: &Config, mut stream: TcpStream) {
    let Ok(request) = tokio::time::timeout(REQUEST_HEAD_TIMEOUT, read_request(&mut stream)).await
    else {
        // Close without a response; the client has not finished a request.
        return;
    };
    let Some((method, path)) = request else {
        let _ = respond(&mut stream, 400, "text/plain", b"bad request\n", true).await;
        return;
    };
    let head = method == "HEAD";
    let (code, content_type, body) = if method != "GET" && !head {
        (405, "text/plain", b"method not allowed\n".to_vec())
    } else {
        route(config, &path)
    };
    let _ = respond(&mut stream, code, content_type, &body, !head).await;
}

fn route(config: &Config, path: &str) -> (u16, &'static str, Vec<u8>) {
    match path {
        // The process is serving requests; stuck work is reported by /readyz.
        "/healthz" | "/livez" => (200, "text/plain", b"ok\n".to_vec()),
        "/readyz" => match crate::lifecycle::read(config) {
            Ok(status) => {
                let ready = status.ready_in(std::process::id());
                let body = serde_json::to_vec_pretty(&status).unwrap_or_default();
                (if ready { 200 } else { 503 }, "application/json", body)
            }
            Err(_) => (503, "text/plain", b"no status observation yet\n".to_vec()),
        },
        "/metrics" => match std::fs::read(config.state_dir.join("metrics.prom")) {
            Ok(body) => (200, "text/plain; version=0.0.4; charset=utf-8", body),
            Err(_) => (503, "text/plain", b"metrics not exported yet\n".to_vec()),
        },
        _ => (404, "text/plain", b"not found\n".to_vec()),
    }
}

/// Read one bounded request head and return its method and query-free path.
async fn read_request(stream: &mut TcpStream) -> Option<(String, String)> {
    let mut buffer = Vec::with_capacity(1024);
    let mut chunk = [0; 1024];
    while !buffer.windows(4).any(|window| window == b"\r\n\r\n") {
        if buffer.len() >= MAX_REQUEST_BYTES {
            return None;
        }
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
    let line = std::str::from_utf8(&buffer)
        .ok()?
        .split("\r\n")
        .next()?
        .to_owned();
    let mut parts = line.split(' ');
    let (method, target, version) = (parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() || !version.starts_with("HTTP/1.") {
        return None;
    }
    let path = target.split('?').next().unwrap_or_default();
    Some((method.to_owned(), path.to_owned()))
}

async fn respond(
    stream: &mut TcpStream,
    code: u16,
    content_type: &str,
    body: &[u8],
    include_body: bool,
) -> std::io::Result<()> {
    let reason = match code {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "Service Unavailable",
    };
    let head = format!(
        "HTTP/1.1 {code} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    if include_body {
        stream.write_all(body).await?;
    }
    stream.shutdown().await
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn get(address: std::net::SocketAddr, request: &str) -> String {
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        response
    }

    #[tokio::test]
    async fn serves_health_readiness_and_metrics_from_local_observations() {
        let directory = tempfile::tempdir().unwrap();
        let mut config: Config =
            toml::from_str(include_str!("../../../examples/flow.toml")).unwrap();
        config.state_dir = directory.path().to_owned();
        config.http = Some(crate::config::Http {
            listen: "127.0.0.1:0".parse().unwrap(),
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let _server = Server(tokio::spawn(serve(config.clone(), listener)));

        let health = get(address, "GET /healthz HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert!(health.starts_with("HTTP/1.1 200 OK\r\n") && health.ends_with("ok\n"));
        let missing = get(address, "GET /metrics HTTP/1.1\r\n\r\n").await;
        assert!(missing.starts_with("HTTP/1.1 503"));
        let unready = get(address, "GET /readyz HTTP/1.1\r\n\r\n").await;
        assert!(unready.starts_with("HTTP/1.1 503"));

        std::fs::write(
            directory.path().join("metrics.prom"),
            "# TYPE flow_up gauge\nflow_up 1\n",
        )
        .unwrap();
        let metrics = get(address, "GET /metrics?x=1 HTTP/1.1\r\n\r\n").await;
        assert!(metrics.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(metrics.contains("Content-Type: text/plain; version=0.0.4"));
        assert!(metrics.ends_with("flow_up 1\n"));
        let head = get(address, "HEAD /metrics HTTP/1.1\r\n\r\n").await;
        assert!(head.starts_with("HTTP/1.1 200 OK\r\n") && head.ends_with("\r\n\r\n"));

        let lifecycle = crate::lifecycle::Lifecycle::start(&config).unwrap();
        crate::lifecycle::emit(&config, None, None, true, None, None).unwrap();
        let ready = get(address, "GET /readyz HTTP/1.1\r\n\r\n").await;
        assert!(ready.starts_with("HTTP/1.1 200 OK\r\n"), "{ready}");
        drop(lifecycle);

        assert!(
            get(address, "POST /metrics HTTP/1.1\r\n\r\n")
                .await
                .starts_with("HTTP/1.1 405")
        );
        assert!(
            get(address, "GET /other HTTP/1.1\r\n\r\n")
                .await
                .starts_with("HTTP/1.1 404")
        );
        assert!(
            get(address, "garbage\r\n\r\n")
                .await
                .starts_with("HTTP/1.1 400")
        );
    }

    #[tokio::test]
    async fn idle_and_slow_clients_cannot_starve_probes() {
        let directory = tempfile::tempdir().unwrap();
        let mut config: Config =
            toml::from_str(include_str!("../../../examples/flow.toml")).unwrap();
        config.state_dir = directory.path().to_owned();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let _server = Server(tokio::spawn(serve(config, listener)));

        // More idle connections than the former 16-slot cap.
        let mut idle = Vec::new();
        for _ in 0..64 {
            idle.push(TcpStream::connect(address).await.unwrap());
        }
        let mut slow = TcpStream::connect(address).await.unwrap();
        slow.write_all(b"GET /healthz HTTP/1.1\r\n").await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let health = tokio::time::timeout(
            Duration::from_secs(1),
            get(address, "GET /healthz HTTP/1.1\r\n\r\n"),
        )
        .await
        .expect("probe served while other clients hold connections");
        assert!(health.starts_with("HTTP/1.1 200 OK\r\n"));

        // Clients that never finish a request head are closed after the head
        // timeout rather than holding a slot for the full request timeout.
        let started = std::time::Instant::now();
        for stream in idle.iter_mut().take(4).chain([&mut slow]) {
            let mut rest = Vec::new();
            let read = tokio::time::timeout(REQUEST_TIMEOUT, stream.read_to_end(&mut rest))
                .await
                .expect("incomplete request closed before the request timeout");
            assert!(read.is_err() || rest.is_empty());
        }
        assert!(started.elapsed() < REQUEST_TIMEOUT);
    }
}

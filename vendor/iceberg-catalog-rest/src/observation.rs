// Local addition by Embrasure Flow; see LOCAL_CHANGES.md.
// SPDX-License-Identifier: Apache-2.0

//! Opt-in observations at reqwest's execute/body boundaries. An execute may
//! include redirects or transport retries; these counters are not billed HTTP
//! request counts. Labels exclude URLs, table names, and credentials.
use reqwest::{Client, Method, Request, Response};
use std::time::Instant;

#[derive(Clone)]
struct RequestLabels {
    endpoint: &'static str,
    method: &'static str,
}

fn endpoint(request: &Request, oauth: bool) -> &'static str {
    if oauth {
        return "oauth";
    }
    let parts: Vec<_> = request
        .url()
        .path()
        .split('/')
        .filter(|part| !part.is_empty())
        .collect();
    // Match whole route suffixes, most specific first. Identifier segments
    // must never be searched for structural words such as `namespaces`.
    match parts.as_slice() {
        [.., "namespaces", _, "tables", _, "metrics"] => "table_metrics",
        [.., "namespaces", _, "tables", _] => "table",
        [.., "namespaces", _, "properties"] => "namespace_properties",
        [.., "namespaces", _, "tables"] => "tables",
        [.., "namespaces", _, "register"] => "register_table",
        [.., "tables", "rename"] => "rename_table",
        [.., "transactions", "commit"] => "commit_transaction",
        [.., "v1", "config"] => "config",
        [.., "namespaces", _] => "namespace",
        [.., "namespaces"] => "namespaces",
        _ => "other",
    }
}

pub(crate) async fn execute(
    client: &Client,
    request: Request,
    oauth: bool,
) -> reqwest::Result<Response> {
    let labels = RequestLabels {
        endpoint: endpoint(&request, oauth),
        method: match *request.method() {
            Method::GET => "GET",
            Method::POST => "POST",
            Method::DELETE => "DELETE",
            Method::HEAD => "HEAD",
            Method::PUT => "PUT",
            _ => "OTHER",
        },
    };
    metrics::counter!("flow_catalog_http_requests_total", "endpoint" => labels.endpoint, "method" => labels.method).increment(1);
    if let Some(body) = request.body().and_then(|body| body.as_bytes()) {
        metrics::counter!("flow_catalog_request_body_bytes_submitted_total", "endpoint" => labels.endpoint, "method" => labels.method).increment(body.len() as u64);
    }
    let started = Instant::now();
    let mut result = client.execute(request).await;
    let outcome = match &result {
        Ok(response) if response.status().is_success() => "success",
        Ok(response) if response.status().is_client_error() => "client_error",
        Ok(response) if response.status().is_server_error() => "server_error",
        Ok(_) => "other_status",
        Err(_) => "transport_error",
    };
    metrics::counter!("flow_catalog_http_results_total", "endpoint" => labels.endpoint, "method" => labels.method, "outcome" => outcome).increment(1);
    metrics::histogram!("flow_catalog_http_seconds", "endpoint" => labels.endpoint, "method" => labels.method, "outcome" => outcome).record(started.elapsed().as_secs_f64());
    if let Ok(response) = &mut result {
        response.extensions_mut().insert(labels);
    }
    result
}

pub(crate) async fn response_bytes(response: Response) -> reqwest::Result<bytes::Bytes> {
    let labels = response.extensions().get::<RequestLabels>().cloned();
    let started = Instant::now();
    let result = response.bytes().await;
    if let Some(labels) = labels {
        let outcome = if result.is_ok() { "success" } else { "error" };
        metrics::counter!("flow_catalog_body_reads_total", "endpoint" => labels.endpoint, "method" => labels.method, "outcome" => outcome).increment(1);
        metrics::histogram!("flow_catalog_body_read_seconds", "endpoint" => labels.endpoint, "method" => labels.method).record(started.elapsed().as_secs_f64());
        if let Ok(body) = &result {
            metrics::counter!("flow_catalog_response_body_bytes_total", "endpoint" => labels.endpoint, "method" => labels.method).increment(body.len() as u64);
        }
    }
    result
}

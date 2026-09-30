use anyhow::Result;
use std::{future::Future, time::Duration};

/// Only transport and explicitly temporary service errors are retryable.
/// Schema, authorization, lineage, and data validation failures stay visible.
pub(crate) fn transient(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        if let Some(error) = cause.downcast_ref::<iceberg::Error>() {
            return error.retryable();
        }
        if let Some(error) = cause.downcast_ref::<reqwest::Error>() {
            return error.is_timeout() || error.is_connect() || error.is_body();
        }
        if let Some(error) = cause.downcast_ref::<opendal::Error>() {
            return error.is_temporary() || error.is_persistent();
        }
        false
    })
}

/// The catalog and source outages that the running service retries. An
/// error that needs an operator is never retried, whatever it wraps.
pub(crate) fn startup_transient(error: &anyhow::Error) -> bool {
    !crate::exit::operator_action(error)
        && (transient(error) || crate::source::retryable_connection(error))
}

pub(crate) fn delay(attempt: u32) -> Duration {
    let ceiling_ms = 250u64.saturating_mul(1 << attempt.min(7)).min(30_000);
    let jitter = (uuid::Uuid::new_v4().as_u128() % u128::from(ceiling_ms / 2)) as u64;
    Duration::from_millis(ceiling_ms / 2 + jitter)
}

/// A dependency still unavailable after this long exits with
/// `exit::UNAVAILABLE`, leaving further restarts to the supervisor.
pub(crate) const STARTUP_RETRY_WINDOW: Duration = Duration::from_secs(600);

/// Retry one idempotent startup step through outages the running service
/// would retry, with the same classification and capped backoff.
pub(crate) async fn startup<T, F, Fut>(step: &'static str, attempt_step: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    startup_within(step, STARTUP_RETRY_WINDOW, attempt_step).await
}

async fn startup_within<T, F, Fut>(
    step: &'static str,
    window: Duration,
    mut attempt_step: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let started = tokio::time::Instant::now();
    let mut attempt = 0u32;
    loop {
        match attempt_step().await {
            Ok(value) => return Ok(value),
            Err(error) if startup_transient(&error) && started.elapsed() < window => {
                let wait = delay(attempt);
                tracing::warn!(
                    event = "startup_retry",
                    step,
                    attempt,
                    retry_in_ms = wait.as_millis() as u64,
                    error = %format!("{error:#}"),
                    "startup dependency unavailable; retrying"
                );
                tokio::time::sleep(wait).await;
                attempt = attempt.saturating_add(1);
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn elapsed() -> anyhow::Error {
        tokio::time::timeout(Duration::ZERO, std::future::pending::<()>())
            .await
            .unwrap_err()
            .into()
    }

    #[tokio::test(start_paused = true)]
    async fn startup_retries_only_transient_errors_within_its_window() {
        let mut calls = 0;
        let value = startup_within("test", Duration::from_secs(60), || {
            calls += 1;
            let calls = calls;
            async move {
                if calls < 3 {
                    Err(elapsed().await)
                } else {
                    Ok(calls)
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(value, 3);

        let mut calls = 0;
        let error = startup_within("test", Duration::from_secs(60), || {
            calls += 1;
            async { Err::<(), _>(anyhow::anyhow!("authentication failed")) }
        })
        .await
        .unwrap_err();
        assert_eq!(
            (calls, error.to_string().as_str()),
            (1, "authentication failed")
        );

        let started = tokio::time::Instant::now();
        let mut calls = 0u32;
        let error = startup_within("test", Duration::from_secs(60), || {
            calls += 1;
            async { Err::<(), _>(elapsed().await) }
        })
        .await
        .unwrap_err();
        assert!(startup_transient(&error));
        assert!(calls > 1);
        let waited = started.elapsed();
        assert!(
            waited >= Duration::from_secs(60) && waited < Duration::from_secs(91),
            "{waited:?}"
        );
    }
}

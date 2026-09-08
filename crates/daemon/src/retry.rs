use std::time::Duration;

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

pub(crate) fn delay(attempt: u32) -> Duration {
    let ceiling_ms = 250u64.saturating_mul(1 << attempt.min(7)).min(30_000);
    let jitter = (uuid::Uuid::new_v4().as_u128() % u128::from(ceiling_ms / 2)) as u64;
    Duration::from_millis(ceiling_ms / 2 + jitter)
}

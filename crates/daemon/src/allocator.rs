//! Optional GNU/Linux process allocator diagnostics and idle page reclamation.
//! The RocksDB jemalloc feature links unprefixed malloc symbols; a Rust-only
//! global allocator would leave the C++ allocation path on the system allocator.

use tikv_jemalloc_ctl::{Result, background_thread, epoch, stats};

pub(crate) struct MemoryUsage {
    pub(crate) allocated_bytes: u64,
    pub(crate) active_bytes: u64,
    pub(crate) resident_bytes: u64,
}

/// These fields overlap; they are not additive and resident is not process RSS.
pub(crate) fn memory_usage() -> Result<MemoryUsage> {
    epoch::advance()?;
    Ok(MemoryUsage {
        allocated_bytes: stats::allocated::read()? as u64,
        active_bytes: stats::active::read()? as u64,
        resident_bytes: stats::resident::read()? as u64,
    })
}

pub(crate) fn initialize() {
    // Enable after process initialization, avoiding allocator bootstrap's
    // circular dependencies. Reclamation failure must not stop ingestion.
    match background_thread::write(true) {
        Ok(()) => tracing::info!("jemalloc background reclamation enabled"),
        Err(error) => tracing::warn!(%error, "jemalloc background reclamation unavailable"),
    }
    match memory_usage() {
        Ok(usage) => tracing::info!(
            allocated_bytes = usage.allocated_bytes,
            active_bytes = usage.active_bytes,
            resident_bytes = usage.resident_bytes,
            "jemalloc initialized"
        ),
        Err(error) => tracing::warn!(%error, "jemalloc memory sample unavailable"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initialization_enables_reclamation_and_allocator_stats() {
        initialize();
        assert!(background_thread::read().expect("jemalloc background-thread control"));
        memory_usage().expect("jemalloc allocation statistics");
    }
}

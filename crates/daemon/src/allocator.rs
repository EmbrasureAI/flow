//! Optional GNU/Linux process allocator diagnostics and idle page reclamation.
//! The RocksDB jemalloc feature links unprefixed malloc symbols; a Rust-only
//! global allocator would leave the C++ allocation path on the system allocator.

use tikv_jemalloc_ctl::{Result, background_thread, config, epoch, stats};

pub(crate) struct MemoryUsage {
    pub(crate) allocated_bytes: u64,
    pub(crate) active_bytes: u64,
    pub(crate) resident_bytes: u64,
    pub(crate) mapped_bytes: u64,
    pub(crate) retained_bytes: u64,
    pub(crate) metadata_bytes: u64,
}

/// These fields overlap; they are not additive and resident is not process RSS.
pub(crate) fn memory_usage() -> Result<MemoryUsage> {
    epoch::advance()?;
    Ok(MemoryUsage {
        allocated_bytes: stats::allocated::read()? as u64,
        active_bytes: stats::active::read()? as u64,
        resident_bytes: stats::resident::read()? as u64,
        mapped_bytes: stats::mapped::read()? as u64,
        retained_bytes: stats::retained::read()? as u64,
        metadata_bytes: stats::metadata::read()? as u64,
    })
}

/// Built-in options; `MALLOC_CONF` in the environment overrides them.
fn built_in_options() -> &'static str {
    // The control returns the C string with its terminating NUL.
    config::malloc_conf::read()
        .unwrap_or_default()
        .trim_end_matches('\0')
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
            malloc_conf = built_in_options(),
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

    #[test]
    fn jemalloc_keeps_transparent_huge_pages_off_its_memory() {
        let options = built_in_options();
        assert!(
            options.split(',').any(|option| option == "thp:never"),
            "{options:?}"
        );
        // Where the kernel has transparent huge pages and no override is set,
        // a fresh large allocation's mapping must exclude them.
        if std::env::var_os("MALLOC_CONF").is_some()
            || !std::path::Path::new("/sys/kernel/mm/transparent_hugepage/enabled").exists()
        {
            return;
        }
        let block = vec![1_u8; 64 << 20];
        let address = block.as_ptr() as u64;
        let smaps = std::fs::read_to_string("/proc/self/smaps").expect("process mappings");
        let mut inside = false;
        let mut flags = None;
        for line in smaps.lines() {
            let mut fields = line.split_whitespace();
            let first = fields.next().unwrap_or_default();
            if let Some((start, end)) = first.split_once('-')
                && let (Ok(start), Ok(end)) =
                    (u64::from_str_radix(start, 16), u64::from_str_radix(end, 16))
            {
                inside = (start..end).contains(&address);
            } else if inside && first == "VmFlags:" {
                flags = Some(fields.map(str::to_owned).collect::<Vec<_>>());
                break;
            }
        }
        let flags = flags.expect("mapping of the allocation");
        assert!(flags.iter().any(|flag| flag == "nh"), "{flags:?}");
        drop(block);
    }
}

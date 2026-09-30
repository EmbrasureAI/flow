use iceberg::spec::{DataFile, DataFileFormat};

/// Stable logical content identity. Multiple deletion vectors may share one
/// Puffin object, so its path alone is not a delete-file identity.
pub fn content_file_id(file: &DataFile) -> String {
    if file.file_format() == DataFileFormat::Puffin {
        // Length framing makes paths containing separators unambiguous. Keep
        // the identity printable for durable operation journals.
        format!(
            "dv:{}:{}:{}",
            file.file_path().len(),
            file.file_path(),
            file.content_offset().unwrap_or(-1)
        )
    } else {
        file.file_path().to_owned()
    }
}

/// Delete payload bytes admitted for scanning. A DV reads its blob range; its
/// Puffin footer is bounded separately by the reader's metadata limit.
pub fn delete_content_size(file: &DataFile) -> u64 {
    if file.file_format() == DataFileFormat::Puffin {
        file.content_size_in_bytes()
            .and_then(|size| u64::try_from(size).ok())
            .unwrap_or(u64::MAX)
    } else {
        file.file_size_in_bytes()
    }
}

/// Count newly written objects and their bytes once, even when several DV
/// descriptors refer to different blobs in the same Puffin file.
pub fn physical_file_stats(files: &[DataFile]) -> (usize, u64) {
    let mut seen = std::collections::BTreeSet::new();
    let bytes = files
        .iter()
        .filter(|file| seen.insert(file.file_path()))
        .fold(0u64, |bytes, file| {
            bytes.saturating_add(file.file_size_in_bytes())
        });
    (seen.len(), bytes)
}

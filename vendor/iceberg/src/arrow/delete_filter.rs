// Modified by Embrasure Flow; see LOCAL_CHANGES.md.
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};

use tokio::sync::Notify;
use tokio::sync::futures::OwnedNotified;
use tokio::sync::oneshot::Receiver;

use crate::delete_vector::DeleteVector;
use crate::expr::Predicate::AlwaysTrue;
use crate::expr::{Bind, BoundPredicate, Predicate};
use crate::runtime::Runtime;
use crate::scan::{FileScanTask, FileScanTaskDeleteFile};
use crate::spec::DataContentType;
use crate::{Error, ErrorKind, Result};

#[derive(Debug)]
enum EqDelState {
    Loading(Arc<Notify>),
    Loaded(Predicate),
    Failed(Arc<Error>),
}

/// State tracking for positional delete files.
/// Unlike equality deletes, positional deletes must be fully loaded before
/// the ArrowReader proceeds because retrieval is synchronous and non-blocking.
#[derive(Debug)]
enum PosDelState {
    /// The file is currently being loaded by a task.
    /// The notifier allows other tasks to wait for completion.
    Loading(Arc<Notify>),
    /// Vectors remain scoped to their input delete file. Applicability to a
    /// data file was decided by the scan planner, including sequence numbers.
    Loaded(HashMap<String, DeleteVector>),
    Failed(Arc<Error>),
}

#[derive(Debug)]
struct CachedDeleteVector {
    inputs: Vec<String>,
    vector: Arc<Mutex<DeleteVector>>,
}

#[derive(Debug, Default)]
struct DeleteFileFilterState {
    delete_vectors: HashMap<String, CachedDeleteVector>,
    equality_deletes: HashMap<String, EqDelState>,
    positional_deletes: HashMap<String, PosDelState>,
}

#[derive(Clone, Debug)]
pub(crate) struct DeleteFilter {
    state: Arc<RwLock<DeleteFileFilterState>>,
    runtime: Runtime,
}

/// Action to take when trying to start loading a positional delete file
pub(crate) enum PosDelLoadAction {
    /// The file is not loaded, the caller should load it.
    Load(PosDelLoadGuard),
    /// The previous owner failed or was cancelled.
    Failed(Error),
    /// The file is already loaded, nothing to do.
    AlreadyLoaded,
    /// The file is currently being loaded by another task.
    /// The caller *must* wait for this notifier to ensure data availability
    /// before returning, as subsequent access (get_delete_vector) is synchronous.
    WaitFor(Pin<Box<OwnedNotified>>),
}

/// Owns a claimed positional load through opening, parsing and publication.
/// Dropping a buffered future or cancelling its scan must also complete waiters.
pub(crate) struct PosDelLoadGuard {
    filter: DeleteFilter,
    file_path: String,
    finished: bool,
}

impl PosDelLoadGuard {
    pub(crate) fn finish(mut self, vectors: HashMap<String, DeleteVector>) {
        self.filter.finish_pos_del_load(&self.file_path, vectors);
        self.finished = true;
    }

    pub(crate) fn fail(&mut self, error: Error) -> Error {
        let error = Arc::new(error);
        self.filter
            .finish_pos_del_state(&self.file_path, PosDelState::Failed(error.clone()));
        self.finished = true;
        positional_load_error(error)
    }
}

impl Drop for PosDelLoadGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.fail(Error::new(
                ErrorKind::Unexpected,
                "Positional delete load cancelled",
            ));
        }
    }
}

fn positional_load_error(error: Arc<Error>) -> Error {
    Error::new(error.kind(), "Positional delete load failed")
        .with_retryable(error.retryable())
        .with_source(error)
}

impl DeleteFilter {
    /// Create a new DeleteFilter with the given runtime.
    pub(crate) fn new(runtime: Runtime) -> Self {
        Self {
            state: Arc::new(RwLock::new(DeleteFileFilterState::default())),
            runtime,
        }
    }

    /// Retrieve a delete vector for the data file associated with a given file scan task
    pub(crate) fn get_delete_vector(
        &self,
        file_scan_task: &FileScanTask,
    ) -> Option<Arc<Mutex<DeleteVector>>> {
        let mut inputs = file_scan_task
            .deletes
            .iter()
            .filter(|file| file.file_type == DataContentType::PositionDeletes)
            .map(FileScanTaskDeleteFile::positional_cache_key)
            .collect::<Vec<_>>();
        inputs.sort_unstable();
        inputs.dedup();
        let path = file_scan_task.data_file_path();
        let mut state = self.state.write().unwrap();
        if let Some(cached) = state.delete_vectors.get(path)
            && cached.inputs == inputs
        {
            return Some(cached.vector.clone());
        }
        let mut combined = None;
        for input in &inputs {
            if let Some(PosDelState::Loaded(vectors)) = state.positional_deletes.get(input)
                && let Some(vector) = vectors.get(path)
            {
                *combined.get_or_insert_with(DeleteVector::default) |= vector;
            }
        }
        let vector = Arc::new(Mutex::new(combined?));
        state.delete_vectors.insert(
            path.to_owned(),
            CachedDeleteVector {
                inputs,
                vector: vector.clone(),
            },
        );
        Some(vector)
    }

    pub(crate) fn try_start_eq_del_load(&self, file_path: &str) -> Option<Arc<Notify>> {
        let mut state = self.state.write().unwrap();

        // Skip if already loaded/loading - another task owns it
        if state.equality_deletes.contains_key(file_path) {
            return None;
        }

        // Mark as loading to prevent duplicate work
        let notifier = Arc::new(Notify::new());
        state
            .equality_deletes
            .insert(file_path.to_string(), EqDelState::Loading(notifier.clone()));

        Some(notifier)
    }

    /// Attempts to mark a positional delete file as "loading".
    ///
    /// Returns an action dictating whether the caller should load the file,
    /// wait for another task to load it, or do nothing.
    pub(crate) fn try_start_pos_del_load(&self, file_path: &str) -> PosDelLoadAction {
        let mut state = self.state.write().unwrap();

        if let Some(state) = state.positional_deletes.get(file_path) {
            match state {
                PosDelState::Loaded(_) => return PosDelLoadAction::AlreadyLoaded,
                PosDelState::Failed(error) => {
                    return PosDelLoadAction::Failed(positional_load_error(error.clone()));
                }
                PosDelState::Loading(notify) => {
                    let mut notified = Box::pin(notify.clone().notified_owned());
                    // Register while holding the state lock so completion
                    // cannot be lost before the waiter is first polled.
                    notified.as_mut().enable();
                    return PosDelLoadAction::WaitFor(notified);
                }
            }
        }

        let notifier = Arc::new(Notify::new());
        state
            .positional_deletes
            .insert(file_path.to_string(), PosDelState::Loading(notifier));

        PosDelLoadAction::Load(PosDelLoadGuard {
            filter: self.clone(),
            file_path: file_path.to_owned(),
            finished: false,
        })
    }

    /// Marks a positional delete file as successfully loaded and notifies any waiting tasks.
    pub(crate) fn finish_pos_del_load(
        &self,
        file_path: &str,
        vectors: HashMap<String, DeleteVector>,
    ) {
        self.finish_pos_del_state(file_path, PosDelState::Loaded(vectors));
    }

    fn finish_pos_del_state(&self, file_path: &str, result: PosDelState) {
        let notify = {
            let mut state = self.state.write().unwrap();
            if let Some(PosDelState::Loading(notify)) = state
                .positional_deletes
                .insert(file_path.to_string(), result)
            {
                Some(notify)
            } else {
                None
            }
        };

        if let Some(notify) = notify {
            notify.notify_waiters();
        }
    }

    /// Retrieve the equality delete predicate for a given eq delete file path
    pub(crate) async fn get_equality_delete_predicate_for_delete_file_path(
        &self,
        file_path: &str,
    ) -> Result<Option<Predicate>> {
        loop {
            let notified = {
                match self.state.read().unwrap().equality_deletes.get(file_path) {
                    None => return Ok(None),
                    Some(EqDelState::Loading(notify)) => {
                        let mut notified = Box::pin(notify.clone().notified_owned());
                        // Register before releasing the lock to avoid losing completion.
                        notified.as_mut().enable();
                        notified
                    }
                    Some(EqDelState::Loaded(predicate)) => {
                        return Ok(Some(predicate.clone()));
                    }
                    Some(EqDelState::Failed(error)) => {
                        return Err(Error::new(error.kind(), "Equality delete load failed")
                            .with_source(error.clone()));
                    }
                }
            };
            notified.await;
        }
    }

    /// Builds eq delete predicate for the provided task.
    pub(crate) async fn build_equality_delete_predicate(
        &self,
        file_scan_task: &FileScanTask,
    ) -> Result<Option<BoundPredicate>> {
        // * Filter the task's deletes into just the Equality deletes
        // * Retrieve the unbound predicate for each from self.state.equality_deletes
        // * Logical-AND them all together to get a single combined `Predicate`
        // * Bind the predicate to the task's schema to get a `BoundPredicate`

        let mut combined_predicate = AlwaysTrue;
        for delete in &file_scan_task.deletes {
            if !is_equality_delete(delete) {
                continue;
            }

            let Some(predicate) = self
                .get_equality_delete_predicate_for_delete_file_path(&delete.file_path)
                .await?
            else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "Missing predicate for equality delete file '{}'",
                        delete.file_path
                    ),
                ));
            };

            combined_predicate = combined_predicate.and(predicate);
        }

        if combined_predicate == AlwaysTrue {
            return Ok(None);
        }

        let bound_predicate = combined_predicate
            .bind(file_scan_task.schema.clone(), file_scan_task.case_sensitive)?;
        Ok(Some(bound_predicate))
    }

    pub(crate) fn insert_equality_delete(
        &self,
        delete_file_path: &str,
        eq_del: Receiver<Predicate>,
    ) {
        let notify = {
            let mut state = self.state.write().unwrap();
            // Preserve the notifier installed when this load was claimed.
            if let Some(EqDelState::Loading(notify)) = state.equality_deletes.get(delete_file_path)
            {
                notify.clone()
            } else {
                let notify = Arc::new(Notify::new());
                state.equality_deletes.insert(
                    delete_file_path.to_string(),
                    EqDelState::Loading(notify.clone()),
                );
                notify
            }
        };

        let state = self.state.clone();
        let delete_file_path = delete_file_path.to_string();
        self.runtime.cpu().spawn(async move {
            let result = match eq_del.await {
                Ok(predicate) => EqDelState::Loaded(predicate),
                Err(error) => EqDelState::Failed(Arc::new(
                    Error::new(
                        ErrorKind::Unexpected,
                        "Equality delete load did not complete",
                    )
                    .with_source(error),
                )),
            };
            {
                let mut state = state.write().unwrap();
                state.equality_deletes.insert(delete_file_path, result);
            }
            notify.notify_waiters();
        });
    }
}

pub(crate) fn is_equality_delete(f: &FileScanTaskDeleteFile) -> bool {
    matches!(f.file_type, DataContentType::EqualityDeletes)
}

#[cfg(test)]
pub(crate) mod tests {
    use std::fs::File;
    use std::path::Path;
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::Schema as ArrowSchema;
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    use super::*;
    use crate::arrow::caching_delete_file_loader::CachingDeleteFileLoader;
    use crate::expr::Reference;
    use crate::io::FileIO;
    use crate::spec::{DataFileFormat, Datum, NestedField, PrimitiveType, Schema, Type};

    type ArrowSchemaRef = Arc<ArrowSchema>;

    const FIELD_ID_POSITIONAL_DELETE_FILE_PATH: u64 = 2147483546;
    const FIELD_ID_POSITIONAL_DELETE_POS: u64 = 2147483545;

    #[tokio::test]
    async fn test_delete_file_filter_load_deletes() {
        let tmp_dir = TempDir::new().unwrap();
        let table_location = tmp_dir.path();
        let file_io = FileIO::new_with_fs();

        let delete_file_loader =
            CachingDeleteFileLoader::new(file_io.clone(), 10, Runtime::current());

        let file_scan_tasks = setup(table_location);

        let delete_filter = delete_file_loader
            .load_deletes(&file_scan_tasks[0].deletes, file_scan_tasks[0].schema_ref())
            .await
            .unwrap()
            .unwrap();

        let result = delete_filter
            .get_delete_vector(&file_scan_tasks[0])
            .unwrap();
        assert_eq!(result.lock().unwrap().len(), 12); // pos dels from pos del file 1 and 2

        let delete_filter = delete_file_loader
            .load_deletes(&file_scan_tasks[1].deletes, file_scan_tasks[1].schema_ref())
            .await
            .unwrap()
            .unwrap();

        let result = delete_filter
            .get_delete_vector(&file_scan_tasks[1])
            .unwrap();
        assert_eq!(result.lock().unwrap().len(), 8); // no pos dels for file 3
    }

    pub(crate) fn setup(table_location: &Path) -> Vec<FileScanTask> {
        let data_file_schema = Arc::new(Schema::builder().build().unwrap());
        let positional_delete_schema = create_pos_del_schema();

        let file_path_values = [
            vec![format!("{}/1.parquet", table_location.to_str().unwrap()); 8],
            vec![format!("{}/1.parquet", table_location.to_str().unwrap()); 8],
            vec![format!("{}/2.parquet", table_location.to_str().unwrap()); 8],
        ];
        let pos_values = [
            vec![0i64, 1, 3, 5, 6, 8, 1022, 1023],
            vec![0i64, 1, 3, 5, 20, 21, 22, 23],
            vec![0i64, 1, 3, 5, 6, 8, 1022, 1023],
        ];

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        for n in 1..=3 {
            let file_path_vals = file_path_values.get(n - 1).unwrap();
            let file_path_col = Arc::new(StringArray::from_iter_values(file_path_vals));

            let pos_vals = pos_values.get(n - 1).unwrap();
            let pos_col = Arc::new(Int64Array::from_iter_values(pos_vals.clone()));

            let positional_deletes_to_write = RecordBatch::try_new(
                positional_delete_schema.clone(),
                vec![file_path_col.clone(), pos_col.clone()],
            )
            .unwrap();

            let file = File::create(format!(
                "{}/pos-del-{}.parquet",
                table_location.to_str().unwrap(),
                n
            ))
            .unwrap();
            let mut writer = ArrowWriter::try_new(
                file,
                positional_deletes_to_write.schema(),
                Some(props.clone()),
            )
            .unwrap();

            writer
                .write(&positional_deletes_to_write)
                .expect("Writing batch");

            // writer must be closed to write footer
            writer.close().unwrap();
        }

        let pos_del_1 = FileScanTaskDeleteFile::builder()
            .with_file_path(format!(
                "{}/pos-del-1.parquet",
                table_location.to_str().unwrap()
            ))
            .with_file_size_in_bytes(
                std::fs::metadata(format!(
                    "{}/pos-del-1.parquet",
                    table_location.to_str().unwrap()
                ))
                .unwrap()
                .len(),
            )
            .with_file_type(DataContentType::PositionDeletes)
            .with_partition_spec_id(0)
            .build();

        let pos_del_2 = FileScanTaskDeleteFile::builder()
            .with_file_path(format!(
                "{}/pos-del-2.parquet",
                table_location.to_str().unwrap()
            ))
            .with_file_size_in_bytes(
                std::fs::metadata(format!(
                    "{}/pos-del-2.parquet",
                    table_location.to_str().unwrap()
                ))
                .unwrap()
                .len(),
            )
            .with_file_type(DataContentType::PositionDeletes)
            .with_partition_spec_id(0)
            .build();

        let pos_del_3 = FileScanTaskDeleteFile::builder()
            .with_file_path(format!(
                "{}/pos-del-3.parquet",
                table_location.to_str().unwrap()
            ))
            .with_file_size_in_bytes(
                std::fs::metadata(format!(
                    "{}/pos-del-3.parquet",
                    table_location.to_str().unwrap()
                ))
                .unwrap()
                .len(),
            )
            .with_file_type(DataContentType::PositionDeletes)
            .with_partition_spec_id(0)
            .build();

        let file_scan_tasks = vec![
            FileScanTask::builder()
                .with_file_size_in_bytes(0)
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{}/1.parquet", table_location.to_str().unwrap()))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(data_file_schema.clone())
                .with_project_field_ids(vec![])
                .with_deletes(vec![pos_del_1, pos_del_2.clone()])
                .with_case_sensitive(false)
                .build(),
            FileScanTask::builder()
                .with_file_size_in_bytes(0)
                .with_start(0)
                .with_length(0)
                .with_data_file_path(format!("{}/2.parquet", table_location.to_str().unwrap()))
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(data_file_schema.clone())
                .with_project_field_ids(vec![])
                .with_deletes(vec![pos_del_3])
                .with_case_sensitive(false)
                .build(),
        ];

        file_scan_tasks
    }

    pub(crate) fn create_pos_del_schema() -> ArrowSchemaRef {
        let fields = vec![
            arrow_schema::Field::new("file_path", arrow_schema::DataType::Utf8, false)
                .with_metadata(HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    FIELD_ID_POSITIONAL_DELETE_FILE_PATH.to_string(),
                )])),
            arrow_schema::Field::new("pos", arrow_schema::DataType::Int64, false).with_metadata(
                HashMap::from([(
                    PARQUET_FIELD_ID_META_KEY.to_string(),
                    FIELD_ID_POSITIONAL_DELETE_POS.to_string(),
                )]),
            ),
        ];
        Arc::new(arrow_schema::Schema::new(fields))
    }

    #[tokio::test]
    async fn test_build_equality_delete_predicate_case_sensitive() {
        let schema = Arc::new(
            Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "Id", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .unwrap(),
        );

        // ---------- fake FileScanTask ----------
        let task = FileScanTask::builder()
            .with_file_size_in_bytes(0)
            .with_start(0)
            .with_length(0)
            .with_data_file_path("data.parquet".to_string())
            .with_data_file_format(crate::spec::DataFileFormat::Parquet)
            .with_schema(schema.clone())
            .with_project_field_ids(vec![])
            .with_deletes(vec![
                FileScanTaskDeleteFile::builder()
                    .with_file_path("eq-del.parquet".to_string())
                    .with_file_size_in_bytes(1) // never read; this test fails before opening the file
                    .with_file_type(DataContentType::EqualityDeletes)
                    .with_partition_spec_id(0)
                    .build(),
            ])
            .with_case_sensitive(true)
            .build();

        let filter = DeleteFilter::new(Runtime::current());

        // ---------- insert equality delete predicate ----------
        let pred = Reference::new("id").equal_to(Datum::long(10));

        let (tx, rx) = tokio::sync::oneshot::channel();
        filter.insert_equality_delete("eq-del.parquet", rx);

        tx.send(pred).unwrap();

        // ---------- should FAIL ----------
        let result = filter.build_equality_delete_predicate(&task).await;

        assert!(
            result.is_err(),
            "case_sensitive=true should fail when column case mismatches"
        );
    }
}

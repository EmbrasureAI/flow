// Added by Embrasure Flow; see LOCAL_CHANGES.md. Not part of upstream
// Apache Iceberg Rust.
//
// Copyright 2026 The Embrasure Flow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Keep S3 operators alive across file operations and REST catalog table reloads.
//!
//! Each operator owns its HTTP pool and reqsign's expiration-aware, serialized
//! credential refresh. Rebuilding it for every file defeats both caches and can
//! throttle the ECS task-role endpoint before requests reach S3.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use iceberg::{Error, ErrorKind, Result};
use opendal::Operator;
use opendal::layers::{RetryLayer, TimeoutLayer};
use opendal::services::S3Config;
use url::Url;

use crate::s3::{CustomAwsCredentialLoader, s3_config_build};

const CAPACITY: usize = 128;
const IDLE_TTL: Duration = Duration::from_secs(15 * 60);
// Providers without an expiration (for example environment credentials) must
// also be reconsidered eventually, even while the operator stays busy.
const MAX_AGE: Duration = Duration::from_secs(60 * 60);

/// Internal bounded operator cache. It belongs to one storage factory, never to
/// the process, so catalogs with separate credential providers cannot share it.
#[doc(hidden)]
#[derive(Default)]
pub struct S3OperatorCache {
    entries: Mutex<VecDeque<Entry>>,
}

struct Entry {
    config: S3Config,
    bucket: String,
    operator: Operator,
    last_used: Instant,
    created: Instant,
}

impl std::fmt::Debug for S3OperatorCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Configurations can contain secrets; never include keys or entries.
        f.debug_struct("S3OperatorCache").finish_non_exhaustive()
    }
}

impl S3OperatorCache {
    pub(crate) fn get(
        &self,
        config: &S3Config,
        loader: &Option<CustomAwsCredentialLoader>,
        path: &str,
    ) -> Result<Operator> {
        let now = Instant::now();
        let url = Url::parse(path)?;
        let bucket = url
            .host_str()
            .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "S3 URL is missing its bucket"))?;
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| Error::new(ErrorKind::Unexpected, "S3 operator cache lock poisoned"))?;
        entries.retain(|entry| {
            now.saturating_duration_since(entry.last_used) < IDLE_TTL
                && now.saturating_duration_since(entry.created) < MAX_AGE
        });
        if let Some(index) = entries
            .iter()
            .position(|entry| entry.bucket == bucket && entry.config == *config)
        {
            let mut entry = entries.remove(index).unwrap();
            entry.last_used = now;
            let operator = entry.operator.clone();
            entries.push_back(entry);
            return Ok(operator);
        }

        // Operator construction performs no I/O. Build under the lock so cold
        // concurrent callers receive one signer/credential cache, not many.
        // TimeoutLayer updates the shared accessor's executor. Applying it to
        // cached clones would grow a recursive executor chain on every access.
        // Layer once before sharing, with timeout inside retry so each attempt
        // stays independently bounded.
        let operator = s3_config_build(config, loader, path)?
            .layer(TimeoutLayer::new())
            .layer(RetryLayer::new());
        if entries.len() == CAPACITY {
            entries.pop_front();
        }
        entries.push_back(Entry {
            config: config.clone(),
            bucket: bucket.to_owned(),
            operator: operator.clone(),
            last_used: now,
            created: now,
        });
        Ok(operator)
    }
}

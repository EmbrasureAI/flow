<!-- Modified by Embrasure Flow; see LOCAL_CHANGES.md. -->
<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Apache Iceberg Rest Catalog Official Native Rust Implementation

[![crates.io](https://img.shields.io/crates/v/iceberg.svg)](https://crates.io/crates/iceberg-catalog-rest)
[![docs.rs](https://img.shields.io/docsrs/iceberg.svg)](https://docs.rs/iceberg/latest/iceberg-catalog-rest/)

This crate contains the official Native Rust implementation of Apache Iceberg Rest Catalog.

See the [API documentation](https://docs.rs/iceberg-catalog-rest/latest) for examples and the full API.

## Local diagnostics extension

The opt-in `metrics` feature observes reqwest execute and response-body boundaries
with bounded endpoint/method/outcome labels. It never labels URLs, names, or
credentials, and does not claim to count SDK-internal retries or billed HTTP
requests. The daemon enables this feature. The separately recorded local patches
are `docs/patches/iceberg-rest-retryable-status.patch` and
`docs/patches/iceberg-rest-observability.patch` at the repository root.

## Local table creation correction

Table creation sends `TableCreation.format_version` in the REST request's
reserved `format-version` property, including the default v2. The typed field
takes precedence over a conflicting raw property; other properties are preserved.
This prevents explicit v3 creation from silently using the catalog's default.
The correction and HTTP request regression are recorded in
`docs/patches/iceberg-rest-format-version.patch`.

## Local HTTPS and OAuth token renewal

reqwest is built with its rustls backend and the platform trust store
(`SSL_CERT_FILE` and `SSL_CERT_DIR` replace that store when set), so `https://`
catalogs work with the default client. OAuth tokens obtained from client
credentials are exchanged again before the `expires_in` the token endpoint
reports: five minutes early, or after nine tenths of a shorter lifetime. A
failed early exchange keeps using the current token until it expires. When the
catalog answers 401 or 419 and credentials are configured, the client obtains a
new token and sends the request once more. Errors caused by rejected credentials
(catalog 401, 403 or 419; token endpoint 400, 401 or 403) carry the public
`AuthRejected` marker as their source. The HTTP client's `Debug` output shows
configured header names only. The change is recorded in
`docs/patches/iceberg-rest-oauth-refresh.patch`.

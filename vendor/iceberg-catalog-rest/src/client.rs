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
use std::fmt::{Debug, Display, Formatter};
use std::time::{Duration, Instant};

use http::StatusCode;
use iceberg::{Error, ErrorKind, Result};
use reqwest::header::HeaderMap;
use reqwest::{Client, IntoUrl, Method, Request, RequestBuilder, Response};
use serde::de::DeserializeOwned;
use tokio::sync::Mutex;

use crate::types::TokenResponse;
use crate::RestCatalogConfig;

/// Replace an OAuth token this long before the server-reported expiry, or after
/// nine tenths of a shorter lifetime, so requests do not race the expiry.
const TOKEN_REFRESH_MARGIN: Duration = Duration::from_secs(300);

/// Marks an error caused by the catalog rejecting the request's credentials.
///
/// It is attached as the source of catalog responses with HTTP 401, 403 or
/// 419, and of OAuth token responses with HTTP 400, 401 or 403. Callers can
/// distinguish credential failures from catalog outages without parsing
/// messages. It never carries response text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthRejected;

impl Display for AuthRejected {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("catalog rejected the request credentials")
    }
}

impl std::error::Error for AuthRejected {}

/// A bearer token. Tokens obtained from the OAuth endpoint with `expires_in`
/// are replaced before they expire; configured tokens are used until rejected.
#[derive(Clone)]
struct CachedToken {
    value: String,
    refresh_at: Option<Instant>,
    expires_at: Option<Instant>,
}

impl CachedToken {
    fn configured(value: String) -> Self {
        Self {
            value,
            refresh_at: None,
            expires_at: None,
        }
    }

    fn issued(value: String, expires_in: Option<u64>, now: Instant) -> Self {
        let lifetime = expires_in.map(Duration::from_secs);
        Self {
            value,
            refresh_at: lifetime.and_then(|lifetime| {
                now.checked_add(lifetime - TOKEN_REFRESH_MARGIN.min(lifetime / 10))
            }),
            expires_at: lifetime.and_then(|lifetime| now.checked_add(lifetime)),
        }
    }

    fn refresh_due(&self, now: Instant) -> bool {
        self.refresh_at.is_some_and(|at| now >= at)
    }

    fn expired(&self, now: Instant) -> bool {
        self.expires_at.is_some_and(|at| now >= at)
    }
}

/// A catalog response status for which a new OAuth token may succeed.
fn token_rejected(status: StatusCode) -> bool {
    status == StatusCode::UNAUTHORIZED || status.as_u16() == 419
}

/// A catalog response status caused by the request's credentials.
fn auth_rejected(status: StatusCode) -> bool {
    token_rejected(status) || status == StatusCode::FORBIDDEN
}

pub(crate) struct HttpClient {
    client: Client,

    /// The token to be used for authentication.
    ///
    /// It's possible to fetch the token from the server while needed.
    token: Mutex<Option<CachedToken>>,
    /// Serializes token exchanges so concurrent requests share one refresh.
    refresh: Mutex<()>,
    /// The token endpoint to be used for authentication.
    token_endpoint: String,
    /// The credential to be used for authentication.
    credential: Option<(Option<String>, String)>,
    /// Extra headers to be added to each request.
    extra_headers: HeaderMap,
    /// Extra oauth parameters to be added to each authentication request.
    extra_oauth_params: HashMap<String, String>,
    /// Whether to disable header redaction in error logs (defaults to false for security).
    disable_header_redaction: bool,
}

impl Debug for HttpClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // Configured headers can carry credentials (`header.Authorization`,
        // API keys); show only their names. Tokens and credentials are omitted.
        f.debug_struct("HttpClient")
            .field("client", &self.client)
            .field(
                "extra_headers",
                &self.extra_headers.keys().collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

impl HttpClient {
    /// Create a new http client.
    pub fn new(cfg: &RestCatalogConfig) -> Result<Self> {
        let extra_headers = cfg.extra_headers()?;
        Ok(HttpClient {
            client: cfg.client().unwrap_or_default(),
            token: Mutex::new(cfg.token().map(CachedToken::configured)),
            refresh: Mutex::new(()),
            token_endpoint: cfg.get_token_endpoint(),
            credential: cfg.credential(),
            extra_headers,
            extra_oauth_params: cfg.extra_oauth_params(),
            disable_header_redaction: cfg.disable_header_redaction(),
        })
    }

    /// Update the http client with new configuration.
    ///
    /// If cfg carries new value, we will use cfg instead.
    /// Otherwise, we will keep the old value.
    pub fn update_with(self, cfg: &RestCatalogConfig) -> Result<Self> {
        let extra_headers = (!cfg.extra_headers()?.is_empty())
            .then(|| cfg.extra_headers())
            .transpose()?
            .unwrap_or(self.extra_headers);
        Ok(HttpClient {
            client: cfg.client().unwrap_or(self.client),
            token: Mutex::new(
                cfg.token()
                    .map(CachedToken::configured)
                    .or_else(|| self.token.into_inner()),
            ),
            refresh: Mutex::new(()),
            token_endpoint: if !cfg.get_token_endpoint().is_empty() {
                cfg.get_token_endpoint()
            } else {
                self.token_endpoint
            },
            credential: cfg.credential().or(self.credential),
            extra_headers,
            extra_oauth_params: if !cfg.extra_oauth_params().is_empty() {
                cfg.extra_oauth_params()
            } else {
                self.extra_oauth_params
            },
            disable_header_redaction: cfg.disable_header_redaction(),
        })
    }

    /// This API is testing only to assert the token.
    #[cfg(test)]
    pub(crate) async fn token(&self) -> Option<String> {
        let mut req = self
            .request(Method::GET, &self.token_endpoint)
            .build()
            .unwrap();
        self.authenticate(&mut req).await.ok();
        self.token.lock().await.as_ref().map(|token| token.value.clone())
    }

    async fn exchange_credential_for_token(&self) -> Result<CachedToken> {
        // Credential must exist here.
        let (client_id, client_secret) = self.credential.as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Credential must be provided for authentication",
            )
        })?;

        let mut params = HashMap::with_capacity(4);
        params.insert("grant_type", "client_credentials");
        if let Some(client_id) = client_id {
            params.insert("client_id", client_id);
        }
        params.insert("client_secret", client_secret);
        params.extend(
            self.extra_oauth_params
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str())),
        );

        let mut auth_req = self
            .request(Method::POST, &self.token_endpoint)
            .form(&params)
            .build()?;
        // extra headers add content-type application/json header it's necessary to override it with proper type
        // note that form call doesn't add content-type header if already present
        auth_req.headers_mut().insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/x-www-form-urlencoded"),
        );
        let auth_url = auth_req.url().clone();
        let auth_resp = self.send(auth_req, true).await?;
        let issued_at = Instant::now();

        let auth_res: TokenResponse = if auth_resp.status() == StatusCode::OK {
            let text = response_bytes(auth_resp)
                .await
                .map_err(|err| err.with_url(auth_url.clone()))?;
            Ok(parse_response_json(&text).map_err(|error| {
                error
                    .with_context("operation", "auth")
                    .with_context("url", auth_url.to_string())
            })?)
        } else {
            let code = auth_resp.status();
            let retryable = retryable_status(code);
            // Consume the body for transport accounting, but never put OAuth
            // response text (including server error messages) into diagnostics.
            let _ = response_bytes(auth_resp)
                .await
                .map_err(|err| err.with_url(auth_url.clone()))?;
            let error = Error::new(ErrorKind::Unexpected, "OAuth token request failed")
                .with_retryable(retryable)
                .with_context("code", code.to_string())
                .with_context("operation", "auth");
            // RFC 6749 reports rejected client credentials and grants as 400 or 401.
            Err(
                if matches!(
                    code,
                    StatusCode::BAD_REQUEST | StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
                ) {
                    error.with_source(AuthRejected)
                } else {
                    error
                },
            )
        }?;
        Ok(CachedToken::issued(
            auth_res.access_token,
            auth_res.expires_in,
            issued_at,
        ))
    }

    /// Invalidate the current token without generating a new one. On the next request, the client
    /// will attempt to generate a new token.
    pub(crate) async fn invalidate_token(&self) -> Result<()> {
        *self.token.lock().await = None;
        Ok(())
    }

    /// Invalidate the current token and set a new one. Generates a new token before invalidating
    /// the current token, meaning the old token will be used until this function acquires the lock
    /// and overwrites the token.
    ///
    /// If credential is invalid, or the request fails, this method will return an error and leave
    /// the current token unchanged.
    pub(crate) async fn regenerate_token(&self) -> Result<()> {
        let _refresh = self.refresh.lock().await;
        let new_token = self.exchange_credential_for_token().await?;
        *self.token.lock().await = Some(new_token);
        Ok(())
    }

    /// Returns the bearer token for the next request, if any.
    ///
    /// This method supports three authentication modes:
    ///
    /// 1. **No authentication** - Skip authentication when both `credential` and `token` are missing.
    /// 2. **Token authentication** - Use the provided `token` directly for authentication.
    /// 3. **OAuth authentication** - Exchange `credential` for a token, cache it, then use it for authentication.
    ///
    /// When both `credential` and `token` are present, `token` takes precedence until the
    /// catalog rejects it. An OAuth token is exchanged again before its reported expiry; if
    /// that early exchange fails, the current token is used until it actually expires.
    async fn current_token(&self) -> Result<Option<String>> {
        // Clone the token from lock without holding the lock for entire function.
        let cached = self.token.lock().await.clone();
        match &cached {
            Some(token) if !token.refresh_due(Instant::now()) => {
                return Ok(Some(token.value.clone()));
            }
            None if self.credential.is_none() => return Ok(None),
            _ => {}
        }

        let _refresh = self.refresh.lock().await;
        // Another request may have replaced the token while this one waited.
        if let Some(token) = self.token.lock().await.as_ref()
            && !token.refresh_due(Instant::now())
        {
            return Ok(Some(token.value.clone()));
        }
        match self.exchange_credential_for_token().await {
            Ok(token) => {
                let value = token.value.clone();
                // Update token so that we use it for next request instead of
                // exchanging credential for token from the server again
                *self.token.lock().await = Some(token);
                Ok(Some(value))
            }
            Err(error) => match cached {
                Some(token) if !token.expired(Instant::now()) => Ok(Some(token.value)),
                _ => Err(error),
            },
        }
    }

    /// Replaces a token that the catalog rejected, unless a concurrent request already did.
    async fn replace_rejected_token(&self, rejected: &str) -> Result<String> {
        let _refresh = self.refresh.lock().await;
        if let Some(token) = self.token.lock().await.as_ref()
            && token.value != rejected
            && !token.refresh_due(Instant::now())
        {
            return Ok(token.value.clone());
        }
        let token = self.exchange_credential_for_token().await?;
        let value = token.value.clone();
        *self.token.lock().await = Some(token);
        Ok(value)
    }

    /// Authenticates the request by adding a bearer token to the authorization header.
    ///
    /// Returns the token used, if any.
    async fn authenticate(&self, req: &mut Request) -> Result<Option<String>> {
        let token = self.current_token().await?;
        if let Some(token) = &token {
            set_bearer(req, token)?;
        }
        Ok(token)
    }

    #[inline]
    pub fn request<U: IntoUrl>(&self, method: Method, url: U) -> RequestBuilder {
        self.client
            .request(method, url)
            .headers(self.extra_headers.clone())
    }

    /// Executes the given `Request` and returns a `Response`.
    pub async fn execute(&self, mut request: Request) -> Result<Response> {
        request.headers_mut().extend(self.extra_headers.clone());
        self.send(request, false).await
    }

    async fn send(&self, request: Request, _oauth: bool) -> Result<Response> {
        #[cfg(feature = "metrics")]
        {
            Ok(crate::observation::execute(&self.client, request, _oauth).await?)
        }
        #[cfg(not(feature = "metrics"))]
        {
            Ok(self.client.execute(request).await?)
        }
    }

    // Queries the Iceberg REST catalog after authentication with the given `Request` and
    // returns a `Response`.
    //
    // When OAuth credentials are configured and the catalog rejects the token, the
    // credential is exchanged for a new token and the request is sent once more. A
    // rejected request was not processed, so repeating it is safe. Configured tokens
    // without credentials cannot be replaced; their rejection is returned.
    pub async fn query_catalog(&self, mut request: Request) -> Result<Response> {
        let retry = self
            .credential
            .as_ref()
            .and_then(|_| request.try_clone());
        let token = self.authenticate(&mut request).await?;
        let response = self.execute(request).await?;
        let (Some(mut retry), Some(token)) = (retry, token) else {
            return Ok(response);
        };
        if !token_rejected(response.status()) {
            return Ok(response);
        }
        // Consume the body for transport accounting; it is not diagnostic.
        let _ = response_bytes(response).await;
        let token = self.replace_rejected_token(&token).await?;
        set_bearer(&mut retry, &token)?;
        self.execute(retry).await
    }

    /// Returns whether header redaction is disabled for this client.
    pub(crate) fn disable_header_redaction(&self) -> bool {
        self.disable_header_redaction
    }
}

/// Inserts the bearer token, replacing any existing authorization header.
fn set_bearer(req: &mut Request, token: &str) -> Result<()> {
    req.headers_mut().insert(
        http::header::AUTHORIZATION,
        format!("Bearer {token}").parse().map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                "Invalid token received from catalog server!",
            )
            .with_source(e)
        })?,
    );
    Ok(())
}

#[cfg(feature = "metrics")]
use crate::observation::response_bytes;

#[cfg(not(feature = "metrics"))]
async fn response_bytes(response: Response) -> reqwest::Result<bytes::Bytes> {
    response.bytes().await
}

/// Deserializes a catalog response into the given [`DeserializedOwned`] type.
///
/// Returns an error if unable to parse the response bytes.
pub(crate) async fn deserialize_catalog_response<R: DeserializeOwned>(
    response: Response,
) -> Result<R> {
    let bytes = response_bytes(response).await?;
    parse_response_json(&bytes)
}

fn parse_response_json<R: DeserializeOwned>(bytes: &[u8]) -> Result<R> {
    serde_json::from_slice(bytes).map_err(|error| {
        // serde errors can themselves quote input values. Keep only structural
        // diagnostics, without either the response body or the original source.
        Error::new(
            ErrorKind::Unexpected,
            "Failed to parse response from rest catalog server",
        )
        .with_context("category", format!("{:?}", error.classify()))
        .with_context("line", error.line().to_string())
        .with_context("column", error.column().to_string())
    })
}

/// Headers that contain sensitive information and should be excluded from logs.
const SENSITIVE_HEADERS: &[&str] = &[
    "authorization",
    "proxy-authorization",
    "set-cookie",
    "cookie",
    "x-api-key",
    "x-auth-token",
];

/// Returns true if the header name is considered sensitive.
fn is_sensitive_header(name: &str) -> bool {
    let name_lower = name.to_lowercase();
    SENSITIVE_HEADERS.iter().any(|h| name_lower == *h)
}

/// Redacts sensitive headers and returns a debug-formatted string.
///
/// If `disable_redaction` is true, returns all headers without redaction.
/// Otherwise, replaces sensitive header values with "[REDACTED]".
fn format_headers_redacted(headers: &HeaderMap, disable_redaction: bool) -> String {
    if disable_redaction {
        // Return all headers as-is without redaction
        let all: HashMap<&str, &str> = headers
            .iter()
            .filter_map(|(name, value)| value.to_str().ok().map(|v| (name.as_str(), v)))
            .collect();
        return format!("{all:?}");
    }

    // Redact sensitive headers by replacing their values with "[REDACTED]"
    let redacted: HashMap<&str, &str> = headers
        .iter()
        .filter_map(|(name, value)| {
            if is_sensitive_header(name.as_str()) {
                Some((name.as_str(), "[REDACTED]"))
            } else {
                value.to_str().ok().map(|v| (name.as_str(), v))
            }
        })
        .collect();
    format!("{redacted:?}")
}

fn retryable_status(status: StatusCode) -> bool {
    status.is_server_error()
        || status == StatusCode::TOO_MANY_REQUESTS
        || status == StatusCode::REQUEST_TIMEOUT
}

/// Deserializes a unexpected catalog response into an error.
pub(crate) async fn deserialize_unexpected_catalog_error(
    response: Response,
    disable_header_redaction: bool,
) -> Error {
    let status = response.status();
    let err = Error::new(
        ErrorKind::Unexpected,
        "Received response with unexpected status code",
    )
    .with_retryable(retryable_status(status))
    .with_context("status", status.to_string())
    .with_context(
        "headers",
        format_headers_redacted(response.headers(), disable_header_redaction),
    );
    let err = if auth_rejected(status) {
        err.with_source(AuthRejected)
    } else {
        err
    };

    let bytes = match response_bytes(response).await {
        Ok(bytes) => bytes,
        Err(err) => return err.into(),
    };

    err.with_context("response_bytes", bytes.len().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oauth_tokens_refresh_before_reported_expiry() {
        let now = Instant::now();
        let at = |seconds| now + Duration::from_secs(seconds);

        let hour = CachedToken::issued("token".into(), Some(3600), now);
        assert!(!hour.refresh_due(at(3299)));
        assert!(hour.refresh_due(at(3300)));
        assert!(!hour.expired(at(3599)));
        assert!(hour.expired(at(3600)));

        // Short lifetimes keep nine tenths of the lifetime before refreshing.
        let minute = CachedToken::issued("token".into(), Some(60), now);
        assert!(!minute.refresh_due(at(53)));
        assert!(minute.refresh_due(at(54)));

        for token in [
            CachedToken::issued("token".into(), None, now),
            CachedToken::issued("token".into(), Some(u64::MAX), now),
            CachedToken::configured("token".into()),
        ] {
            assert!(!token.refresh_due(at(10 * 365 * 86400)));
            assert!(!token.expired(at(10 * 365 * 86400)));
        }
    }

    #[tokio::test]
    async fn debug_output_omits_header_values_and_credentials() {
        let client = HttpClient::new(
            &RestCatalogConfig::builder()
                .uri("http://localhost:8181".to_string())
                .props(HashMap::from([
                    (
                        "header.Authorization".to_string(),
                        "Bearer FAKE_HEADER_SECRET".to_string(),
                    ),
                    (
                        "header.x-api-key".to_string(),
                        "FAKE_API_KEY_SECRET".to_string(),
                    ),
                    ("token".to_string(), "FAKE_TOKEN_SECRET".to_string()),
                    (
                        "credential".to_string(),
                        "client:FAKE_CREDENTIAL_SECRET".to_string(),
                    ),
                ]))
                .build(),
        )
        .unwrap();
        let debug = format!("{client:?} {client:#?}");
        assert!(debug.contains("authorization") && debug.contains("x-api-key"));
        assert!(!debug.contains("FAKE_"), "{debug}");
    }

    #[tokio::test]
    async fn failed_early_refresh_uses_the_unexpired_token() {
        let mut server = mockito::Server::new_async().await;
        let oauth = server
            .mock("POST", "/v1/oauth/tokens")
            .with_status(503)
            .expect(2)
            .create_async()
            .await;
        let namespaces = server
            .mock("GET", "/v1/namespaces")
            .match_header("authorization", "Bearer current")
            .with_status(200)
            .expect(1)
            .create_async()
            .await;
        let client = HttpClient::new(
            &RestCatalogConfig::builder()
                .uri(server.url())
                .props(HashMap::from([(
                    "credential".to_string(),
                    "client:secret".to_string(),
                )]))
                .build(),
        )
        .unwrap();
        let now = Instant::now();
        *client.token.lock().await = Some(CachedToken {
            value: "current".into(),
            refresh_at: Some(now),
            expires_at: Some(now + Duration::from_secs(60)),
        });
        let request = client
            .request(Method::GET, format!("{}/v1/namespaces", server.url()))
            .build()
            .unwrap();
        assert_eq!(
            client.query_catalog(request).await.unwrap().status(),
            StatusCode::OK
        );

        // Once the token has expired, the exchange failure is returned.
        client.token.lock().await.as_mut().unwrap().expires_at = Some(now);
        let request = client
            .request(Method::GET, format!("{}/v1/namespaces", server.url()))
            .build()
            .unwrap();
        let error = client.query_catalog(request).await.unwrap_err();
        assert!(error.retryable(), "{error:?}");
        oauth.assert_async().await;
        namespaces.assert_async().await;
    }

    #[test]
    fn test_format_headers_redacted_empty() {
        let headers = HeaderMap::new();
        let result = format_headers_redacted(&headers, false);
        assert_eq!(result, "{}");
    }

    #[test]
    fn test_format_headers_redacted_non_sensitive() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());
        headers.insert("x-request-id", "abc123".parse().unwrap());

        let result = format_headers_redacted(&headers, false);

        assert!(result.contains("content-type"));
        assert!(result.contains("application/json"));
        assert!(result.contains("x-request-id"));
        assert!(result.contains("abc123"));
    }

    #[test]
    fn test_format_headers_redacted_filters_sensitive() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer secret-token".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());

        let result = format_headers_redacted(&headers, false);

        // Sensitive header should be present but with redacted value
        assert!(result.contains("authorization"));
        assert!(result.contains("[REDACTED]"));
        // Sensitive value should NOT be present
        assert!(!result.contains("secret-token"));
        // Non-sensitive header should be present with actual value
        assert!(result.contains("content-type"));
        assert!(result.contains("application/json"));
    }

    #[test]
    fn test_format_headers_redacted_filters_set_cookie() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "set-cookie",
            "CF_Authorization=sensitive-session-token; Path=/; Secure;"
                .parse()
                .unwrap(),
        );
        headers.insert("server", "cloudflare".parse().unwrap());

        let result = format_headers_redacted(&headers, false);

        // Sensitive header should be present but with redacted value
        assert!(result.contains("set-cookie"));
        assert!(result.contains("[REDACTED]"));
        // Sensitive value should NOT be present
        assert!(!result.contains("sensitive-session-token"));
        // Non-sensitive header should be present with actual value
        assert!(result.contains("server"));
        assert!(result.contains("cloudflare"));
    }

    #[test]
    fn test_format_headers_redacted_filters_all_sensitive() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer token".parse().unwrap());
        headers.insert("proxy-authorization", "Basic creds".parse().unwrap());
        headers.insert("set-cookie", "session=abc".parse().unwrap());
        headers.insert("cookie", "session=abc".parse().unwrap());
        headers.insert("x-api-key", "api-key-123".parse().unwrap());
        headers.insert("x-auth-token", "auth-token-456".parse().unwrap());
        headers.insert("x-request-id", "req-123".parse().unwrap());

        let result = format_headers_redacted(&headers, false);

        // All sensitive headers should be present but with redacted values
        assert!(result.contains("authorization"));
        assert!(result.contains("proxy-authorization"));
        assert!(result.contains("set-cookie"));
        assert!(result.contains("cookie"));
        assert!(result.contains("x-api-key"));
        assert!(result.contains("x-auth-token"));
        assert!(result.contains("[REDACTED]"));

        // Ensure no sensitive values leaked
        assert!(!result.contains("Bearer token"));
        assert!(!result.contains("Basic creds"));
        assert!(!result.contains("session=abc"));
        assert!(!result.contains("api-key-123"));
        assert!(!result.contains("auth-token-456"));

        // Non-sensitive header should be present with actual value
        assert!(result.contains("x-request-id"));
        assert!(result.contains("req-123"));
    }

    #[test]
    fn test_format_headers_with_redaction_disabled() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer secret-token".parse().unwrap());
        headers.insert("x-api-key", "api-key-123".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());

        let result = format_headers_redacted(&headers, true);

        // When redaction is disabled, all headers and values should be present
        assert!(result.contains("authorization"));
        assert!(result.contains("Bearer secret-token"));
        assert!(result.contains("x-api-key"));
        assert!(result.contains("api-key-123"));
        assert!(result.contains("content-type"));
        assert!(result.contains("application/json"));
        // [REDACTED] should NOT be present when redaction is disabled
        assert!(!result.contains("[REDACTED]"));
    }
}

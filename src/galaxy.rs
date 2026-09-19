//! Asynchronous client for the subset of the Galaxy NG v3 API used by the
//! mirror.  Galaxy installations differ slightly in their discovery documents,
//! so links are deliberately decoded more liberally than collection data.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use futures::StreamExt;
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use reqwest::{Client, Response};
use serde::Deserialize;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use url::Url;

use crate::metrics::{HttpOperation, HttpResult, Metrics};

pub const DEFAULT_USER_AGENT: &str = "ansible-galaxy/2.19.0";

#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Number of retries after the first request.
    pub attempts: usize,
    pub initial_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            attempts: 4,
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GalaxyError {
    #[error("invalid Galaxy URL: {0}")]
    InvalidUrl(#[from] url::ParseError),
    #[error("invalid HTTP header {0}")]
    InvalidHeader(String),
    #[error("Galaxy request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Galaxy returned HTTP {status} for {url}")]
    Status { status: StatusCode, url: Url },
    #[error("Galaxy response at {url} does not contain a v3 API link")]
    NoV3Api { url: Url },
    #[error("Galaxy response at {url} has an invalid schema: {source}")]
    Schema {
        url: Url,
        #[source]
        source: serde_json::Error,
    },
    #[error("too many redirects while requesting {0}")]
    TooManyRedirects(Url),
    #[error("concurrency limiter was closed")]
    Closed,
}

pub type Result<T> = std::result::Result<T, GalaxyError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceSelection {
    pub name: String,
    /// `None` means every collection in this namespace.
    pub collections: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct Collection {
    pub name: String,
    #[serde(deserialize_with = "namespace_name")]
    pub namespace: String,
    #[serde(default)]
    pub href: Option<String>,
    #[serde(default)]
    pub versions_url: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct CollectionVersion {
    pub version: String,
    #[serde(default)]
    pub href: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct Artifact {
    pub sha256: String,
    #[serde(default)]
    pub filename: Option<String>,
    #[serde(default, alias = "size")]
    pub size: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct VersionDetail {
    pub version: String,
    pub created_at: String,
    pub download_url: String,
    pub artifact: Artifact,
    #[serde(default, deserialize_with = "optional_namespace_name")]
    pub namespace: Option<String>,
    #[serde(default, deserialize_with = "optional_collection_name")]
    pub collection: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Page<T> {
    #[serde(alias = "results")]
    data: Vec<T>,
    #[serde(default)]
    links: PageLinks,
}

#[derive(Debug, Default, Deserialize)]
struct PageLinks {
    #[serde(default)]
    next: Option<Link>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum Link {
    Text(String),
    Object { href: String },
}

impl Link {
    fn as_str(&self) -> &str {
        match self {
            Self::Text(value) => value,
            Self::Object { href } => href,
        }
    }
}

#[derive(Clone)]
pub struct GalaxyClient {
    base_url: Url,
    client: Client,
    headers: HeaderMap,
    limiter: Arc<Semaphore>,
    retry: RetryConfig,
    download_timeout: Duration,
    telemetry: Option<(String, Metrics)>,
}

struct RequestMetricGuard {
    metrics: Metrics,
    server: String,
    operation: HttpOperation,
    result: HttpResult,
    started: Instant,
}

impl Drop for RequestMetricGuard {
    fn drop(&mut self) {
        self.metrics.request_finished(
            &self.server,
            self.operation,
            self.result,
            self.started.elapsed(),
        );
    }
}

impl GalaxyClient {
    pub fn new(
        base_url: Url,
        mut headers: HeaderMap,
        concurrency: usize,
        connect_timeout: Duration,
        request_timeout: Duration,
        download_timeout: Duration,
        retry: RetryConfig,
    ) -> Result<Self> {
        if !matches!(base_url.scheme(), "http" | "https") || base_url.host_str().is_none() {
            return Err(GalaxyError::InvalidHeader(
                "base URL must be absolute HTTP(S)".into(),
            ));
        }
        if concurrency == 0 {
            return Err(GalaxyError::InvalidHeader(
                "concurrency must be greater than zero".into(),
            ));
        }
        if !headers.contains_key(http::header::USER_AGENT) {
            headers.insert(
                http::header::USER_AGENT,
                HeaderValue::from_static(DEFAULT_USER_AGENT),
            );
        }
        let client = Client::builder()
            .connect_timeout(connect_timeout)
            .timeout(request_timeout)
            // Redirects are followed here, where credential forwarding is explicit.
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self {
            base_url: normalize_directory_url(base_url),
            client,
            headers,
            limiter: Arc::new(Semaphore::new(concurrency)),
            retry,
            download_timeout,
            telemetry: None,
        })
    }

    pub fn with_metrics(mut self, server: impl Into<String>, metrics: Metrics) -> Self {
        self.telemetry = Some((server.into(), metrics));
        self
    }

    pub fn headers_from_strings(headers: &HashMap<String, String>) -> Result<HeaderMap> {
        let mut result = HeaderMap::new();
        for (name, value) in headers {
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| GalaxyError::InvalidHeader(name.clone()))?;
            let value = HeaderValue::from_str(value)
                .map_err(|_| GalaxyError::InvalidHeader(name.to_string()))?;
            result.insert(name, value);
        }
        Ok(result)
    }

    /// Resolve the v3 root using the server discovery document. Direct v3 URLs
    /// are also accepted, which is useful for private Automation Hub instances.
    pub async fn discover_v3(&self) -> Result<Url> {
        let value: serde_json::Value = self
            .get_json(self.base_url.clone(), HttpOperation::Discovery)
            .await?;
        if let Some(link) = find_v3_link(&value) {
            return Ok(resolve(&self.base_url, link)?);
        }
        if self.base_url.path().split('/').any(|part| part == "v3") {
            return Ok(self.base_url.clone());
        }
        Err(GalaxyError::NoV3Api {
            url: self.base_url.clone(),
        })
    }

    /// Enumerate collections according to the configured selector. An empty
    /// selector means the complete collection index.
    pub async fn collections(
        &self,
        v3_root: &Url,
        selectors: Option<&[NamespaceSelection]>,
    ) -> Result<Vec<Collection>> {
        let index = collections_index(v3_root)?;
        match selectors {
            None => self.paginated(index, HttpOperation::ListCollections).await,
            Some(selectors) => {
                let mut urls = Vec::new();
                for selector in selectors {
                    match &selector.collections {
                        None => {
                            let mut url = index.clone();
                            url.query_pairs_mut()
                                .append_pair("namespace", &selector.name);
                            urls.push(url);
                        }
                        Some(names) => {
                            for name in names {
                                let mut url = index.clone();
                                url.query_pairs_mut()
                                    .append_pair("namespace", &selector.name)
                                    .append_pair("name", name);
                                urls.push(url);
                            }
                        }
                    }
                }
                let width = urls.len().max(1);
                let pages = futures::stream::iter(
                    urls.into_iter()
                        .map(|url| self.paginated(url, HttpOperation::ListCollections)),
                )
                .buffer_unordered(width)
                .collect::<Vec<_>>()
                .await;
                let mut output: Vec<Collection> = Vec::new();
                for page in pages {
                    output.extend(page?);
                }
                output.sort_by(|a, b| (&a.namespace, &a.name).cmp(&(&b.namespace, &b.name)));
                output.dedup_by(|a, b| a.namespace == b.namespace && a.name == b.name);
                Ok(output)
            }
        }
    }

    pub async fn versions(
        &self,
        v3_root: &Url,
        namespace: &str,
        collection: &str,
    ) -> Result<Vec<CollectionVersion>> {
        let url = collection_path(v3_root, namespace, collection, "versions/")?;
        self.paginated(url, HttpOperation::ListVersions).await
    }

    /// Prefer the collection index link because Galaxy deployments may use a
    /// content prefix that differs from the discovery root.
    pub async fn collection_versions(
        &self,
        v3_root: &Url,
        collection: &Collection,
    ) -> Result<Vec<CollectionVersion>> {
        if let Some(url) = &collection.versions_url {
            return self
                .paginated(resolve(v3_root, url)?, HttpOperation::ListVersions)
                .await;
        }
        self.versions(v3_root, &collection.namespace, &collection.name)
            .await
    }

    pub async fn version_detail(
        &self,
        v3_root: &Url,
        namespace: &str,
        collection: &str,
        version: &str,
    ) -> Result<VersionDetail> {
        let suffix = format!("versions/{version}/");
        let url = collection_path(v3_root, namespace, collection, &suffix)?;
        self.get_json(url, HttpOperation::VersionDetail).await
    }

    pub async fn collection_version_detail(
        &self,
        v3_root: &Url,
        namespace: &str,
        collection: &str,
        version: &CollectionVersion,
    ) -> Result<VersionDetail> {
        if let Some(href) = &version.href {
            return self
                .get_json(resolve(v3_root, href)?, HttpOperation::VersionDetail)
                .await;
        }
        self.version_detail(v3_root, namespace, collection, &version.version)
            .await
    }

    /// Open an artifact stream. Sensitive custom headers are included only when
    /// the artifact has the same origin as the configured API server.
    pub async fn download(&self, url: &str) -> Result<Response> {
        let url = resolve(&self.base_url, url)?;
        self.request_with_retries_timeout(url, Some(self.download_timeout), HttpOperation::Download)
            .await
    }

    async fn paginated<T>(&self, first: Url, operation: HttpOperation) -> Result<Vec<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        let mut result = Vec::new();
        let mut next = Some(first);
        while let Some(url) = next.take() {
            let page: Page<T> = self.get_json(url.clone(), operation).await?;
            result.extend(page.data);
            next = page
                .links
                .next
                .as_ref()
                .map(|link| resolve(&url, link.as_str()))
                .transpose()?;
        }
        Ok(result)
    }

    async fn get_json<T: for<'de> Deserialize<'de>>(
        &self,
        url: Url,
        operation: HttpOperation,
    ) -> Result<T> {
        let response = self.request_with_retries(url.clone(), operation).await?;
        let bytes = response.bytes().await?;
        serde_json::from_slice(&bytes).map_err(|source| GalaxyError::Schema { url, source })
    }

    async fn request_with_retries(&self, url: Url, operation: HttpOperation) -> Result<Response> {
        self.request_with_retries_timeout(url, None, operation)
            .await
    }

    async fn request_with_retries_timeout(
        &self,
        url: Url,
        timeout: Option<Duration>,
        operation: HttpOperation,
    ) -> Result<Response> {
        let mut last_error = None;
        for attempt in 0..=self.retry.attempts {
            match self
                .request_following_redirects(url.clone(), timeout, operation)
                .await
            {
                Ok(response) if response.status().is_success() => return Ok(response),
                Ok(response) => {
                    let status = response.status();
                    let retry_after = retry_after(response.headers());
                    drop(response);
                    let error = GalaxyError::Status {
                        status,
                        url: url.clone(),
                    };
                    if attempt == self.retry.attempts || !retryable_status(status) {
                        return Err(error);
                    }
                    last_error = Some(error);
                    self.record_retry(operation);
                    self.backoff(attempt, retry_after).await;
                }
                Err(error) => {
                    if attempt == self.retry.attempts || !retryable_error(&error) {
                        return Err(error);
                    }
                    last_error = Some(error);
                    self.record_retry(operation);
                    self.backoff(attempt, None).await;
                }
            }
        }
        Err(last_error.expect("retry loop executes at least once"))
    }

    async fn request_following_redirects(
        &self,
        mut url: Url,
        timeout: Option<Duration>,
        operation: HttpOperation,
    ) -> Result<Response> {
        let original = url.clone();
        for _ in 0..10 {
            let permit = self.permit().await?;
            let headers = headers_for_target(&self.headers, &self.base_url, &url);
            let started = Instant::now();
            if let Some((server, metrics)) = &self.telemetry {
                metrics.request_started(server);
            }
            let mut request = self.client.get(url.clone()).headers(headers);
            if let Some(timeout) = timeout {
                request = request.timeout(timeout);
            }
            let response = request.send().await;
            let mut response = match response {
                Ok(response) => response,
                Err(error) => {
                    if let Some((server, metrics)) = &self.telemetry {
                        metrics.request_finished(
                            server,
                            operation,
                            HttpResult::Error,
                            started.elapsed(),
                        );
                    }
                    return Err(error.into());
                }
            };
            // Keep the permit with the response so streamed downloads count
            // against the limit until their body (or response) is dropped.
            response.extensions_mut().insert(Arc::new(permit));
            if let Some((server, metrics)) = &self.telemetry {
                let result = if response.status().is_success() {
                    HttpResult::Success
                } else {
                    HttpResult::Error
                };
                response
                    .extensions_mut()
                    .insert(Arc::new(RequestMetricGuard {
                        metrics: metrics.clone(),
                        server: server.clone(),
                        operation,
                        result,
                        started,
                    }));
            }
            if !response.status().is_redirection() {
                return Ok(response);
            }
            let location = response
                .headers()
                .get(http::header::LOCATION)
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| GalaxyError::Status {
                    status: response.status(),
                    url: url.clone(),
                })?;
            url = resolve(&url, location)?;
        }
        Err(GalaxyError::TooManyRedirects(original))
    }

    async fn permit(&self) -> Result<OwnedSemaphorePermit> {
        self.limiter
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| GalaxyError::Closed)
    }

    async fn backoff(&self, attempt: usize, retry_after: Option<Duration>) {
        let delay = retry_after.unwrap_or_else(|| {
            let factor = 1u32.checked_shl(attempt.min(30) as u32).unwrap_or(u32::MAX);
            self.retry
                .initial_delay
                .saturating_mul(factor)
                .min(self.retry.max_delay)
        });
        tokio::time::sleep(delay).await;
    }

    fn record_retry(&self, operation: HttpOperation) {
        if let Some((server, metrics)) = &self.telemetry {
            metrics.request_retried(server, operation);
        }
    }
}

fn normalize_directory_url(mut url: Url) -> Url {
    if !url.path().ends_with('/') {
        let path = format!("{}/", url.path());
        url.set_path(&path);
    }
    url
}

fn find_v3_link(value: &serde_json::Value) -> Option<&str> {
    value
        .pointer("/available_versions/v3")
        .or_else(|| value.pointer("/versions/v3"))
        .or_else(|| value.pointer("/links/v3"))
        .and_then(|value| {
            value
                .as_str()
                .or_else(|| value.get("href").and_then(serde_json::Value::as_str))
        })
}

fn collections_index(v3_root: &Url) -> Result<Url> {
    if v3_root.path().contains("/plugin/ansible/content/") {
        return Ok(resolve(v3_root, "collections/index/")?);
    }
    Ok(resolve(
        v3_root,
        "plugin/ansible/content/published/collections/index/",
    )?)
}

fn collection_path(v3_root: &Url, namespace: &str, collection: &str, suffix: &str) -> Result<Url> {
    let mut url = collections_index(v3_root)?;
    {
        let mut segments = url
            .path_segments_mut()
            .map_err(|_| url::ParseError::RelativeUrlWithCannotBeABaseBase)?;
        // Version list and detail endpoints both live below `collections/index/`.
        segments.pop_if_empty();
        segments.push(namespace).push(collection);
        for part in suffix.split('/').filter(|part| !part.is_empty()) {
            segments.push(part);
        }
        segments.push("");
    }
    Ok(url)
}

fn resolve(base: &Url, link: &str) -> std::result::Result<Url, url::ParseError> {
    Url::parse(link).or_else(|_| base.join(link))
}

fn same_origin(left: &Url, right: &Url) -> bool {
    left.scheme() == right.scheme()
        && left.host_str() == right.host_str()
        && left.port_or_known_default() == right.port_or_known_default()
}

fn headers_for_target(headers: &HeaderMap, api: &Url, target: &Url) -> HeaderMap {
    if same_origin(api, target) {
        return headers.clone();
    }
    headers
        .iter()
        .filter(|(name, _)| !sensitive_header(name))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

fn sensitive_header(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "authorization"
            | "proxy-authorization"
            | "cookie"
            | "cookie2"
            | "x-api-key"
            | "x-auth-token"
            | "x-authorization"
    )
}

fn retryable_status(status: StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 429 | 500 | 502 | 503 | 504)
}

fn retryable_error(error: &GalaxyError) -> bool {
    matches!(error, GalaxyError::Http(error) if error.is_timeout() || error.is_connect() || error.is_request())
}

fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    let value = headers.get(http::header::RETRY_AFTER)?.to_str().ok()?;
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    httpdate::parse_http_date(value)
        .ok()?
        .duration_since(std::time::SystemTime::now())
        .ok()
}

fn namespace_name<'de, D>(deserializer: D) -> std::result::Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Name {
        Text(String),
        Object { name: String },
    }
    Ok(match Name::deserialize(deserializer)? {
        Name::Text(v) => v,
        Name::Object { name } => name,
    })
}

fn optional_namespace_name<'de, D>(deserializer: D) -> std::result::Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<serde_json::Value>::deserialize(deserializer)?
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .or_else(|| {
                    value
                        .get("name")
                        .and_then(|v| v.as_str())
                        .map(str::to_owned)
                })
                .ok_or_else(|| {
                    serde::de::Error::custom("namespace must be a string or an object with name")
                })
        })
        .transpose()
}

fn optional_collection_name<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    optional_namespace_name(deserializer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supplies_default_user_agent_case_insensitively() {
        let mut input = HashMap::new();
        input.insert("uSeR-aGeNt".into(), "custom/1".into());
        let headers = GalaxyClient::headers_from_strings(&input).unwrap();
        let client = GalaxyClient::new(
            Url::parse("https://example.test/api").unwrap(),
            headers,
            1,
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
            RetryConfig::default(),
        )
        .unwrap();
        assert_eq!(client.headers[http::header::USER_AGENT], "custom/1");
    }

    #[test]
    fn removes_credentials_for_another_origin() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_static("secret"),
        );
        headers.insert("x-api-key", HeaderValue::from_static("secret2"));
        headers.insert(http::header::USER_AGENT, HeaderValue::from_static("client"));
        let filtered = headers_for_target(
            &headers,
            &Url::parse("https://api.example/a").unwrap(),
            &Url::parse("https://cdn.example/a").unwrap(),
        );
        assert!(!filtered.contains_key(http::header::AUTHORIZATION));
        assert!(!filtered.contains_key("x-api-key"));
        assert_eq!(filtered[http::header::USER_AGENT], "client");
    }

    #[test]
    fn parses_both_collection_namespace_shapes() {
        let object: Collection =
            serde_json::from_str(r#"{"name":"general","namespace":{"name":"community"}}"#).unwrap();
        let text: Collection =
            serde_json::from_str(r#"{"name":"general","namespace":"community"}"#).unwrap();
        assert_eq!(object, text);
    }

    #[test]
    fn collection_version_url_uses_path_segments() {
        let root = Url::parse("https://example.test/api/galaxy/v3/").unwrap();
        let url = collection_path(&root, "community", "general", "versions/1.2.3/").unwrap();
        assert_eq!(
            url.as_str(),
            "https://example.test/api/galaxy/v3/plugin/ansible/content/published/collections/index/community/general/versions/1.2.3/"
        );
    }

    #[test]
    fn discovers_common_shapes() {
        for json in [
            r#"{"available_versions":{"v3":"v3/"}}"#,
            r#"{"available_versions":{"v3":{"href":"/api/v3/"}}}"#,
            r#"{"links":{"v3":"v3/"}}"#,
        ] {
            let value: serde_json::Value = serde_json::from_str(json).unwrap();
            assert!(find_v3_link(&value).is_some());
        }
    }

    #[test]
    fn parses_retry_after_seconds_and_date() {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::RETRY_AFTER, HeaderValue::from_static("42"));
        assert_eq!(retry_after(&headers), Some(Duration::from_secs(42)));

        let future = std::time::SystemTime::now() + Duration::from_secs(120);
        headers.insert(
            http::header::RETRY_AFTER,
            HeaderValue::from_str(&httpdate::fmt_http_date(future)).unwrap(),
        );
        let parsed = retry_after(&headers).unwrap();
        assert!(parsed >= Duration::from_secs(118) && parsed <= Duration::from_secs(120));
    }
}

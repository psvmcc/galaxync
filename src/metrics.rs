//! Prometheus metrics and the daemon's health endpoints.
//!
//! Labels whose values are not server names are represented by enums.  This is
//! deliberate: accepting arbitrary strings for result/reason/operation labels
//! would make it very easy to accidentally create unbounded time series.

use std::{
    collections::HashSet,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    body::Body,
    http::{Response, StatusCode, header},
    response::IntoResponse,
    routing::get,
};
use prometheus_client::{
    encoding::{EncodeLabelSet, text::encode},
    metrics::{
        counter::Counter,
        family::Family,
        gauge::Gauge,
        histogram::{Histogram, exponential_buckets},
    },
    registry::Registry,
};
use tokio::{net::TcpListener, sync::watch};

fn sync_histogram() -> Histogram {
    Histogram::new(exponential_buckets(1.0, 2.0, 12))
}

fn http_histogram() -> Histogram {
    Histogram::new(exponential_buckets(0.05, 2.0, 12))
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum ServerStatus {
    NeverSynced,
    Syncing,
    Synced,
    Partial,
    Failed,
    Disabled,
    Interrupted,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum SyncResult {
    Success,
    Partial,
    Failed,
    Interrupted,
    Disabled,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum FailureReason {
    Api,
    Download,
    Checksum,
    Filesystem,
    InvalidMetadata,
    Other,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum HttpOperation {
    Discovery,
    ListCollections,
    ListVersions,
    VersionDetail,
    Download,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum HttpResult {
    Success,
    Error,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum ReloadResult {
    Success,
    Failed,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum DisableSource {
    Config,
    Marker,
}

macro_rules! label_values {
    ($type:ty, $($variant:ident => $value:literal),+ $(,)?) => {
        impl $type {
            fn label(self) -> &'static str {
                match self { $(Self::$variant => $value),+ }
            }
        }
    };
}

label_values!(ServerStatus, NeverSynced => "never_synced", Syncing => "syncing",
    Synced => "synced", Partial => "partial", Failed => "failed",
    Disabled => "disabled", Interrupted => "interrupted");
label_values!(SyncResult, Success => "success", Partial => "partial", Failed => "failed",
    Interrupted => "interrupted", Disabled => "disabled");
label_values!(FailureReason, Api => "api", Download => "download", Checksum => "checksum",
    Filesystem => "filesystem", InvalidMetadata => "invalid_metadata", Other => "other");
label_values!(HttpOperation, Discovery => "discovery", ListCollections => "list_collections",
    ListVersions => "list_versions", VersionDetail => "version_detail", Download => "download");
label_values!(HttpResult, Success => "success", Error => "error");
label_values!(ReloadResult, Success => "success", Failed => "failed");
label_values!(DisableSource, Config => "config", Marker => "marker");

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ServerLabels {
    server: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct StatusLabels {
    server: String,
    status: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct SyncResultLabels {
    server: String,
    result: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct FailureLabels {
    server: String,
    reason: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct HttpLabels {
    server: String,
    operation: &'static str,
    result: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct HttpDurationLabels {
    server: String,
    operation: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct RetryLabels {
    server: String,
    operation: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ReloadLabels {
    result: &'static str,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct DisabledLabels {
    server: String,
    source: &'static str,
}

/// Cloneable metrics handle shared by synchronization tasks and the HTTP server.
#[derive(Clone)]
pub struct Metrics {
    registry: Arc<RwLock<Registry>>,
    ready: Arc<AtomicBool>,
    configured_servers: Arc<RwLock<HashSet<String>>>,
    server_status: Family<StatusLabels, Gauge>,
    server_enabled: Family<ServerLabels, Gauge>,
    server_disabled: Family<DisabledLabels, Gauge>,
    sync_in_progress: Family<ServerLabels, Gauge>,
    last_sync_success: Family<ServerLabels, Gauge>,
    last_sync_started: Family<ServerLabels, Gauge>,
    last_sync_completed: Family<ServerLabels, Gauge>,
    last_successful_sync: Family<ServerLabels, Gauge>,
    sync_interval: Family<ServerLabels, Gauge>,
    config_generation: Gauge,
    config_reloads: Family<ReloadLabels, Counter>,
    sync_runs: Family<SyncResultLabels, Counter>,
    sync_duration: Family<ServerLabels, Histogram, fn() -> Histogram>,
    collections_discovered: Family<ServerLabels, Gauge>,
    versions_discovered: Family<ServerLabels, Gauge>,
    artifacts_checked: Family<ServerLabels, Counter>,
    artifacts_downloaded: Family<ServerLabels, Counter>,
    artifacts_repaired: Family<ServerLabels, Counter>,
    artifacts_failed: Family<FailureLabels, Counter>,
    download_bytes: Family<ServerLabels, Counter>,
    active_requests: Family<ServerLabels, Gauge>,
    concurrency_limit: Family<ServerLabels, Gauge>,
    http_requests: Family<HttpLabels, Counter>,
    http_duration: Family<HttpDurationLabels, Histogram, fn() -> Histogram>,
    http_retries: Family<RetryLabels, Counter>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        let mut registry = Registry::default();
        let server_status = Family::default();
        let server_enabled = Family::default();
        let server_disabled = Family::default();
        let sync_in_progress = Family::default();
        let last_sync_success = Family::default();
        let last_sync_started = Family::default();
        let last_sync_completed = Family::default();
        let last_successful_sync = Family::default();
        let sync_interval = Family::default();
        let config_generation = Gauge::default();
        let config_reloads = Family::default();
        let sync_runs = Family::default();
        let sync_duration = Family::new_with_constructor(sync_histogram as fn() -> Histogram);
        let collections_discovered = Family::default();
        let versions_discovered = Family::default();
        let artifacts_checked = Family::default();
        let artifacts_downloaded = Family::default();
        let artifacts_repaired = Family::default();
        let artifacts_failed = Family::default();
        let download_bytes = Family::default();
        let active_requests = Family::default();
        let concurrency_limit = Family::default();
        let http_requests = Family::default();
        let http_duration = Family::new_with_constructor(http_histogram as fn() -> Histogram);
        let http_retries = Family::default();

        macro_rules! register {
            ($name:literal, $help:literal, $metric:ident) => {
                registry.register($name, $help, $metric.clone())
            };
        }
        register!(
            "galaxync_server_status",
            "Current server synchronization status.",
            server_status
        );
        register!(
            "galaxync_server_enabled",
            "Whether synchronization is enabled for a server.",
            server_enabled
        );
        register!(
            "galaxync_server_disabled",
            "Whether a server is disabled, by source.",
            server_disabled
        );
        register!(
            "galaxync_sync_in_progress",
            "Whether a server synchronization is running.",
            sync_in_progress
        );
        register!(
            "galaxync_last_sync_success",
            "Whether the last completed synchronization succeeded.",
            last_sync_success
        );
        register!(
            "galaxync_last_sync_started_timestamp_seconds",
            "Unix timestamp of the last synchronization start.",
            last_sync_started
        );
        register!(
            "galaxync_last_sync_completed_timestamp_seconds",
            "Unix timestamp of the last synchronization completion.",
            last_sync_completed
        );
        register!(
            "galaxync_last_successful_sync_timestamp_seconds",
            "Unix timestamp of the last successful synchronization.",
            last_successful_sync
        );
        register!(
            "galaxync_sync_interval_seconds",
            "Configured synchronization interval.",
            sync_interval
        );
        registry.register(
            "galaxync_config_generation",
            "Active configuration generation.",
            config_generation.clone(),
        );
        register!(
            "galaxync_config_reloads",
            "Configuration reload attempts.",
            config_reloads
        );
        register!(
            "galaxync_sync_runs",
            "Completed synchronization runs.",
            sync_runs
        );
        register!(
            "galaxync_sync_duration_seconds",
            "Synchronization run duration.",
            sync_duration
        );
        register!(
            "galaxync_collections_discovered",
            "Collections discovered in the latest run.",
            collections_discovered
        );
        register!(
            "galaxync_versions_discovered",
            "Versions discovered in the latest run.",
            versions_discovered
        );
        register!(
            "galaxync_artifacts_checked",
            "Artifacts whose local checksum was checked.",
            artifacts_checked
        );
        register!(
            "galaxync_artifacts_downloaded",
            "Artifacts downloaded.",
            artifacts_downloaded
        );
        register!(
            "galaxync_artifacts_repaired",
            "Corrupt artifacts replaced.",
            artifacts_repaired
        );
        register!(
            "galaxync_artifacts_failed",
            "Artifact operations that failed.",
            artifacts_failed
        );
        register!(
            "galaxync_download_bytes",
            "Bytes successfully downloaded.",
            download_bytes
        );
        register!(
            "galaxync_active_requests",
            "Currently active upstream requests.",
            active_requests
        );
        register!(
            "galaxync_request_concurrency_limit",
            "Configured per-server request limit.",
            concurrency_limit
        );
        register!(
            "galaxync_http_requests",
            "Upstream HTTP requests.",
            http_requests
        );
        register!(
            "galaxync_http_request_duration_seconds",
            "Upstream HTTP request duration.",
            http_duration
        );
        register!(
            "galaxync_http_retries",
            "Upstream HTTP retry attempts.",
            http_retries
        );

        Self {
            registry: Arc::new(RwLock::new(registry)),
            ready: Arc::new(AtomicBool::new(false)),
            configured_servers: Arc::new(RwLock::new(HashSet::new())),
            server_status,
            server_enabled,
            server_disabled,
            sync_in_progress,
            last_sync_success,
            last_sync_started,
            last_sync_completed,
            last_successful_sync,
            sync_interval,
            config_generation,
            config_reloads,
            sync_runs,
            sync_duration,
            collections_discovered,
            versions_discovered,
            artifacts_checked,
            artifacts_downloaded,
            artifacts_repaired,
            artifacts_failed,
            download_bytes,
            active_requests,
            concurrency_limit,
            http_requests,
            http_duration,
            http_retries,
        }
    }

    fn server(server: &str) -> ServerLabels {
        ServerLabels {
            server: server.to_owned(),
        }
    }

    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Release);
    }
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }
    pub fn set_config_generation(&self, generation: u64) {
        self.config_generation.set(generation as i64);
    }
    pub fn config_reload(&self, result: ReloadResult) {
        self.config_reloads
            .get_or_create(&ReloadLabels {
                result: result.label(),
            })
            .inc();
    }

    pub fn configure_server(
        &self,
        server: &str,
        enabled: bool,
        interval: Duration,
        concurrency: usize,
    ) {
        let labels = Self::server(server);
        self.server_enabled
            .get_or_create(&labels)
            .set(i64::from(enabled));
        self.sync_interval
            .get_or_create(&labels)
            .set(interval.as_secs() as i64);
        self.concurrency_limit
            .get_or_create(&labels)
            .set(concurrency as i64);
        if self
            .configured_servers
            .write()
            .expect("configured server lock poisoned")
            .insert(server.to_owned())
        {
            self.set_status(server, ServerStatus::NeverSynced);
        }
    }

    pub fn reconcile_servers<'a>(&self, active: impl IntoIterator<Item = &'a str>) {
        let active = active.into_iter().collect::<HashSet<_>>();
        let removed = {
            let mut configured = self
                .configured_servers
                .write()
                .expect("configured server lock poisoned");
            let removed = configured
                .iter()
                .filter(|server| !active.contains(server.as_str()))
                .cloned()
                .collect::<Vec<_>>();
            configured.retain(|server| active.contains(server.as_str()));
            removed
        };
        for server in removed {
            self.server_enabled
                .get_or_create(&Self::server(&server))
                .set(0);
            self.set_status(&server, ServerStatus::Disabled);
        }
    }

    pub fn set_disabled(&self, server: &str, source: DisableSource, disabled: bool) {
        self.server_disabled
            .get_or_create(&DisabledLabels {
                server: server.to_owned(),
                source: source.label(),
            })
            .set(i64::from(disabled));
        self.server_enabled
            .get_or_create(&Self::server(server))
            .set(i64::from(!disabled));
        if disabled {
            self.set_status(server, ServerStatus::Disabled);
        }
    }

    pub fn set_status(&self, server: &str, status: ServerStatus) {
        const ALL: [ServerStatus; 7] = [
            ServerStatus::NeverSynced,
            ServerStatus::Syncing,
            ServerStatus::Synced,
            ServerStatus::Partial,
            ServerStatus::Failed,
            ServerStatus::Disabled,
            ServerStatus::Interrupted,
        ];
        for candidate in ALL {
            self.server_status
                .get_or_create(&StatusLabels {
                    server: server.to_owned(),
                    status: candidate.label(),
                })
                .set(i64::from(candidate == status));
        }
    }

    pub fn sync_started(&self, server: &str, unix_timestamp: i64) {
        let labels = Self::server(server);
        self.set_status(server, ServerStatus::Syncing);
        self.sync_in_progress.get_or_create(&labels).set(1);
        self.last_sync_started
            .get_or_create(&labels)
            .set(unix_timestamp);
    }

    pub fn sync_completed(
        &self,
        server: &str,
        result: SyncResult,
        duration: Duration,
        unix_timestamp: i64,
    ) {
        let labels = Self::server(server);
        self.sync_in_progress.get_or_create(&labels).set(0);
        self.last_sync_completed
            .get_or_create(&labels)
            .set(unix_timestamp);
        self.sync_runs
            .get_or_create(&SyncResultLabels {
                server: server.to_owned(),
                result: result.label(),
            })
            .inc();
        self.sync_duration
            .get_or_create(&labels)
            .observe(duration.as_secs_f64());
        let success = result == SyncResult::Success;
        self.last_sync_success
            .get_or_create(&labels)
            .set(i64::from(success));
        if success {
            self.last_successful_sync
                .get_or_create(&labels)
                .set(unix_timestamp);
        }
        self.set_status(
            server,
            match result {
                SyncResult::Success => ServerStatus::Synced,
                SyncResult::Partial => ServerStatus::Partial,
                SyncResult::Failed => ServerStatus::Failed,
                SyncResult::Interrupted => ServerStatus::Interrupted,
                SyncResult::Disabled => ServerStatus::Disabled,
            },
        );
    }

    pub fn set_discovered(&self, server: &str, collections: u64, versions: u64) {
        let labels = Self::server(server);
        self.collections_discovered
            .get_or_create(&labels)
            .set(collections as i64);
        self.versions_discovered
            .get_or_create(&labels)
            .set(versions as i64);
    }
    pub fn artifact_checked(&self, server: &str) {
        self.artifacts_checked
            .get_or_create(&Self::server(server))
            .inc();
    }
    pub fn artifact_downloaded(&self, server: &str, bytes: u64) {
        let labels = Self::server(server);
        self.artifacts_downloaded.get_or_create(&labels).inc();
        self.download_bytes.get_or_create(&labels).inc_by(bytes);
    }
    pub fn artifact_repaired(&self, server: &str) {
        self.artifacts_repaired
            .get_or_create(&Self::server(server))
            .inc();
    }
    pub fn artifact_failed(&self, server: &str, reason: FailureReason) {
        self.artifacts_failed
            .get_or_create(&FailureLabels {
                server: server.to_owned(),
                reason: reason.label(),
            })
            .inc();
    }
    pub fn request_started(&self, server: &str) {
        self.active_requests
            .get_or_create(&Self::server(server))
            .inc();
    }
    pub fn request_finished(
        &self,
        server: &str,
        operation: HttpOperation,
        result: HttpResult,
        duration: Duration,
    ) {
        self.active_requests
            .get_or_create(&Self::server(server))
            .dec();
        self.http_requests
            .get_or_create(&HttpLabels {
                server: server.to_owned(),
                operation: operation.label(),
                result: result.label(),
            })
            .inc();
        self.http_duration
            .get_or_create(&HttpDurationLabels {
                server: server.to_owned(),
                operation: operation.label(),
            })
            .observe(duration.as_secs_f64());
    }

    pub fn request_retried(&self, server: &str, operation: HttpOperation) {
        self.http_retries
            .get_or_create(&RetryLabels {
                server: server.to_owned(),
                operation: operation.label(),
            })
            .inc();
    }

    pub fn encode(&self) -> Result<String, std::fmt::Error> {
        let mut output = String::new();
        encode(
            &mut output,
            &self
                .registry
                .read()
                .expect("metrics registry lock poisoned"),
        )?;
        Ok(output)
    }
}

/// Build the HTTP router. `metrics_path` must start with `/` and must not
/// collide with the fixed health endpoints.
pub fn router(metrics: Metrics, metrics_path: &str) -> Result<Router, String> {
    if !metrics_path.starts_with('/') || matches!(metrics_path, "/healthz" | "/readyz") {
        return Err(format!("invalid metrics path: {metrics_path}"));
    }
    let metrics_for_scrape = metrics.clone();
    let readiness = metrics.clone();
    Ok(Router::new()
        .route(
            metrics_path,
            get(move || scrape(metrics_for_scrape.clone())),
        )
        .route("/healthz", get(|| async { (StatusCode::OK, "ok\n") }))
        .route("/readyz", get(move || ready(readiness.clone()))))
}

async fn scrape(metrics: Metrics) -> Response<Body> {
    match metrics.encode() {
        Ok(body) => Response::builder()
            .status(StatusCode::OK)
            .header(
                header::CONTENT_TYPE,
                "application/openmetrics-text; version=1.0.0; charset=utf-8",
            )
            .body(Body::from(body))
            .expect("valid metrics response"),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "metrics encoding failed\n",
        )
            .into_response(),
    }
}

async fn ready(metrics: Metrics) -> impl IntoResponse {
    if metrics.is_ready() {
        (StatusCode::OK, "ready\n")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready\n")
    }
}

/// Serve metrics until `shutdown` becomes true or its sender is dropped.
pub async fn serve(
    listener: TcpListener,
    metrics_path: &str,
    metrics: Metrics,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app = router(metrics, metrics_path).map_err(std::io::Error::other)?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            if !*shutdown.borrow() {
                let _ = shutdown.changed().await;
            }
        })
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_bounded_sync_metrics() {
        let metrics = Metrics::new();
        metrics.configure_server("community", true, Duration::from_secs(60), 8);
        metrics.sync_started("community", 100);
        metrics.artifact_downloaded("community", 42);
        metrics.sync_completed(
            "community",
            SyncResult::Success,
            Duration::from_secs(2),
            102,
        );
        metrics.configure_server("community", true, Duration::from_secs(120), 4);
        let text = metrics.encode().unwrap();
        assert!(text.contains("galaxync_download_bytes_total{server=\"community\"} 42"));
        assert!(text.contains("galaxync_server_status{server=\"community\",status=\"synced\"} 1"));
        assert!(text.contains("galaxync_last_sync_success{server=\"community\"} 1"));
        assert!(!text.contains("namespace="));
    }

    #[test]
    fn rejects_health_route_collision() {
        assert!(router(Metrics::new(), "/healthz").is_err());
        assert!(router(Metrics::new(), "metrics").is_err());
    }

    #[test]
    fn removed_servers_are_disabled_and_readded_servers_start_fresh() {
        let metrics = Metrics::new();
        metrics.reconcile_servers(["one"]);
        metrics.configure_server("one", true, Duration::from_secs(60), 1);
        metrics.sync_completed("one", SyncResult::Success, Duration::ZERO, 1);
        metrics.reconcile_servers(std::iter::empty());
        let removed = metrics.encode().unwrap();
        assert!(removed.contains("galaxync_server_enabled{server=\"one\"} 0"));
        assert!(removed.contains("galaxync_server_status{server=\"one\",status=\"disabled\"} 1"));

        metrics.reconcile_servers(["one"]);
        metrics.configure_server("one", true, Duration::from_secs(60), 1);
        assert!(
            metrics
                .encode()
                .unwrap()
                .contains("galaxync_server_status{server=\"one\",status=\"never_synced\"} 1")
        );
    }
}

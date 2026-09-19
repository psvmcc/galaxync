# galaxync Implementation Plan

## 1. Goal

`galaxync` is a tested Rust daemon that periodically mirrors Ansible Galaxy collections from multiple Galaxy API servers into a local filesystem.

It must support YAML and TOML configuration, custom HTTP headers, bounded concurrency, periodic reconciliation, checksum validation, remote timestamps, runtime server disabling, Prometheus metrics, transactional SIGHUP reload, and safe restart recovery.

The first release targets Galaxy NG API v3, behind an API abstraction that can support other versions later.

## 2. Configuration

The default path is a compile-time constant:

```rust
const DEFAULT_CONFIG_PATH: &str = "/etc/galaxync/config.yaml";
```

Override it with `galaxync --config /path/config.toml`. The effective startup path is retained for SIGHUP reload. Supported extensions are `.yaml`, `.yml`, and `.toml`; both formats deserialize into one strict model with unknown fields rejected.

`${NAME}` values are expanded from environment variables. Missing variables are errors. Sensitive headers must never appear in logs, state/status files, or metrics.

Validate without starting the daemon:

```shell
galaxync config validate --config /etc/galaxync/config.yaml
```

### YAML example

```yaml
version: 1
storage_dir: /var/lib/galaxync
sync_interval: 6h
run_on_start: true
overlap_policy: skip
delete_orphans: false

http:
  connect_timeout: 10s
  request_timeout: 60s
  download_timeout: 30m
  retry_attempts: 5
  retry_initial_delay: 1s
  retry_max_delay: 30s

metrics:
  enabled: true
  listen: 127.0.0.1:9898
  path: /metrics

servers:
  # Mirror everything.
  - label: community
    url: https://galaxy.ansible.com/api/
    enabled: true
    concurrency: 8
    include_prerelease: false

  # Mirror selected collections from acme and all of community.
  - label: internal
    url: https://galaxy.example.org/api/
    enabled: true
    concurrency: 4
    headers:
      Authorization: "Token ${GALAXY_TOKEN}"
      User-Agent: "company-galaxy-client/1.0"
    namespaces:
      - name: acme
        collections: [system, network]
      - name: community

  - label: old-hub
    url: https://old-hub.example.org/api/
    enabled: false
    concurrency: 2
```

### TOML example

```toml
version = 1
storage_dir = "/var/lib/galaxync"
sync_interval = "6h"
run_on_start = true
overlap_policy = "skip"
delete_orphans = false

[http]
connect_timeout = "10s"
request_timeout = "60s"
download_timeout = "30m"
retry_attempts = 5
retry_initial_delay = "1s"
retry_max_delay = "30s"

[metrics]
enabled = true
listen = "127.0.0.1:9898"
path = "/metrics"

[[servers]]
label = "community"
url = "https://galaxy.ansible.com/api/"
enabled = true
concurrency = 8

[[servers]]
label = "internal"
url = "https://galaxy.example.org/api/"
enabled = true
concurrency = 4

[servers.headers]
Authorization = "Token ${GALAXY_TOKEN}"

[[servers.namespaces]]
name = "acme"
collections = ["system", "network"]

[[servers.namespaces]]
name = "community"
```

Selection semantics:

| Setting | Behavior |
| --- | --- |
| `namespaces` omitted | Mirror every namespace, collection, and version |
| Namespace without `collections` | Mirror all collections in it |
| Namespace with `collections` | Mirror only those collections |
| Empty namespace or collection list | Configuration error |

Labels must be unique and path-safe, URLs absolute HTTP(S), concurrency greater than zero, and duplicate selectors rejected. `enabled` defaults to true; `delete_orphans` defaults to false.

`include_prerelease` is a per-server boolean and defaults to `false`. When false, valid SemVer versions with a prerelease component such as `1.2.0-alpha.1`, `2.0.0-beta`, or `3.0.0-rc.1` are skipped before metadata requests and downloads. Set it to `true` to mirror them.

## 3. Galaxy API and HTTP

For each enabled server:

1. Normalize the base URL and perform API discovery.
2. Select Galaxy v3 and follow API-provided links and `links.next` pagination.
3. Enumerate the full collection index or apply namespace/name filters.
4. Enumerate all versions and read version details.
5. Extract namespace, collection, version, `created_at`, `download_url`, `artifact.sha256`, and optional size.

Missing explicitly selected content is an object-level failure and does not stop other servers.

If no case-insensitive User-Agent override exists, send an Ansible-compatible value such as `ansible-galaxy/2.19.0`. Custom values win. Strip Authorization, Cookie, and other credentials from cross-origin downloads and redirects.

`concurrency` limits simultaneous API/download operations per server with a Tokio semaphore. Servers may run in parallel.

Retry network failures, timeouts, HTTP 408/429/500/502/503/504, and checksum mismatch up to the configured limit. Honor `Retry-After`; otherwise use exponential backoff with jitter. Do not blindly retry authentication, not-found, schema, path, or configuration errors.

## 4. Storage and downloads

```text
<storage_dir>/<server.label>/<namespace>/<collection>/
  <namespace>-<collection>-<version>.tar.gz
```

Reject NUL, separators, `..`, and all path traversal input.

Downloads use no temporary directory. Write beside the destination as `<filename>.tar.gz.tmp`, calculate SHA-256 while streaming, verify optional size and checksum, flush and fsync, atomically rename, set `mtime` from remote `created_at`, and fsync the parent where supported.

Never expose an invalid final file. Keep an old corrupt file until a verified replacement is ready. Leftover `.tmp` files are overwritten rather than resumed in the first release.

## 5. Synchronization

For each server cycle:

1. Check `enabled` and `.disable`.
2. Write `syncing` status and generate a run ID.
3. Discover the selected tree and build the expected artifact set.
4. Calculate actual SHA-256 for every existing local artifact.
5. Skip matching files; download missing files; atomically replace mismatches.
6. Process orphans only when explicitly enabled and discovery was complete.
7. Update status, global state, and metrics.

States are `syncing`, `synced`, `partial`, `failed`, `disabled`, and `interrupted`. Failures are isolated per artifact/server. Cycles never overlap; skipped cycles are logged and counted.

## 6. SIGHUP reload

SIGHUP transactionally reloads the effective startup path:

1. Read the entire file, parse by its original extension, expand environment values, and fully validate it.
2. Prepare replacement clients and runtime settings without touching live state.
3. Atomically swap the immutable configuration snapshot only after success.
4. Rebuild scheduling and the server set.

On failure, keep the old configuration and active cycles, log the error, increment a failed-reload metric, and keep the process ready.

A running cycle retains its original snapshot and `config_generation`; the next cycle uses the new generation. New servers are scheduled, removed servers stop being scheduled without deleting data, and interval changes rebuild deadlines. URL, headers, selectors, retries, timeouts, concurrency, and enablement apply next cycle.

Changing `metrics.listen`, `metrics.path`, or `metrics.enabled` requires restart in the first release and rejects the whole reload. Concurrent SIGHUP events are coalesced; none start after shutdown begins.

Successful reload updates `config_generation` and `config_loaded_at` in global state and status files.

## 7. Restart and crash recovery

SIGTERM/SIGINT stops new work, lets active atomic operations finish within a timeout, marks unfinished servers `interrupted`, writes state/status, releases the process lock, and exits.

After restart, a full reconciliation verifies completed files and redownloads only missing or corrupt content. A crash or SIGKILL may leave `syncing` and partial `.tmp` files; final archives remain safe because publication is atomic. Startup converts stale `syncing` to `interrupted` and overwrites partial temporary files when encountered.

## 8. State, status, and disabling

Global state is stored inside the configured storage directory:

```text
<storage_dir>/.state.json
```

It contains format version, config generation/load time, and per-server last-started, completed, successful, status, and statistics fields. It is written via `.state.json.tmp`, fsync, and atomic rename. It is an operational summary, never an integrity cache.

Only one process runs per storage directory, enforced by `<storage_dir>/.lock`.

Each server has:

```text
<storage_dir>/<server.label>/.status.json
```

It records server, state, run ID, config generation, timestamps, last success, a safe message, and cycle statistics. Updates use `.status.json.tmp` and are throttled (for example, every five seconds) to act as a heartbeat.

Servers can be disabled permanently with `enabled: false` or dynamically with:

```text
<storage_dir>/<server.label>/.disable
```

The marker may contain a UTF-8 reason up to 4 KiB. Only a regular file is accepted; symlinks and special files are errors. If it appears mid-cycle, stop scheduling new work, finish active atomic operations, set `disabled`, and preserve last-success time. Removing it enables the next scheduled cycle but never overrides `enabled: false`.

## 9. Prometheus and health

Expose `GET /metrics`, `GET /healthz`, and `GET /readyz`. Default binding is loopback. Readiness requires valid configuration and usable state/storage.

Core metrics include:

```text
galaxync_server_status{server="community",status="synced"} 1
galaxync_server_enabled{server="community"} 1
galaxync_sync_in_progress{server="community"} 0
galaxync_last_sync_success{server="community"} 1
galaxync_last_successful_sync_timestamp_seconds{server="community"} 1789808534
galaxync_sync_interval_seconds{server="community"} 21600
galaxync_config_generation 3
galaxync_config_reloads_total{result="success"} 2
galaxync_sync_runs_total{server="community",result="success"} 12
galaxync_sync_duration_seconds_bucket{server="community",le="30"} 4
galaxync_collections_discovered{server="community"} 120
galaxync_versions_discovered{server="community"} 1840
galaxync_artifacts_checked_total{server="community"} 18400
galaxync_artifacts_downloaded_total{server="community"} 82
galaxync_artifacts_repaired_total{server="community"} 3
galaxync_artifacts_failed_total{server="community",reason="checksum"} 1
galaxync_download_bytes_total{server="community"} 987654321
galaxync_active_requests{server="community"} 3
galaxync_request_concurrency_limit{server="community"} 8
```

Use bounded labels only: server, status, result, reason, operation, and disable source. Never label by namespace, collection, version, URL, checksum, run ID, filename, or error text. Status and metrics consume the same internal events.

## 10. Rust structure

```text
src/
  main.rs cli.rs config.rs daemon.rs scheduler.rs state.rs error.rs
  galaxy/{client,discovery,pagination,models}.rs
  sync/{planner,executor,downloader,verifier,events}.rs
  storage/{layout,atomic_file,status,disable,lock}.rs
  observability/{logging,metrics,metrics_http}.rs
```

Use Tokio, reqwest/rustls, serde, yaml_serde, toml, serde_json, url, sha2, time, filetime, uuid, fs2, prometheus-client, axum, tracing, clap, humantime, and thiserror.

## 11. Test plan

Unit tests cover YAML/TOML equivalence, default/overridden path, strict validation, environment expansion, headers, selectors, URL/link handling, safe paths, checksums, dates, retry classification, and secret redaction.

Reload tests cover valid and invalid SIGHUP, immutable cycle snapshots, config generations, server addition/removal, deadline changes, coalescing, shutdown races, and rejected metrics-listener changes.

HTTP tests cover discovery, pagination, selection modes, headers, Retry-After, transient failures, timeouts, interrupted bodies, checksum mismatch, and cross-origin credential stripping.

Filesystem tests cover first and repeated sync, new versions, corrupt-file repair, mtime, atomic replacement, leftover `.tmp`, state/status atomicity, stale-status recovery, safe orphan behavior, and process locking.

Concurrency/disable tests verify limits, server isolation, no overlap, both disable mechanisms, mid-cycle disabling, and graceful termination.

Metrics tests verify endpoint content, counters/gauges/histograms, readiness, consistency with status, absence of secrets/high-cardinality labels, and stable series counts.

End-to-end tests use a mock Galaxy API to exercise initial sync, new versions, corruption repair, disabling, valid/invalid SIGHUP, graceful restart during sync, abrupt termination, and recovery.

CI:

```shell
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo audit
cargo deny check advisories licenses sources
cargo llvm-cov
```

Target at least 80% line coverage for project logic, with mandatory critical branch coverage.

## 12. Delivery stages

1. Crate, CLI, default path, and strict YAML/TOML model.
2. Validation, environment expansion, and secret redaction.
3. Safe layout, lock, and atomic JSON files.
4. Galaxy discovery, v3 models, and pagination.
5. Content selection and reconciliation planning.
6. Checksum verification and atomic downloader.
7. Remote mtime and temporary-file recovery.
8. Concurrency, retries, and server isolation.
9. Scheduler, overlap policy, and graceful shutdown.
10. Transactional SIGHUP reload and config generations.
11. State, status, and event model.
12. Configuration and marker-based disabling.
13. Prometheus, health, and readiness endpoints.
14. Unit, integration, concurrency, and end-to-end tests.
15. Container image, native systemd unit, Podman Quadlet, and operations documentation.

Release artifacts and multi-architecture container images include checksums,
SBOMs, provenance attestations, the project license, and automated dependency
license/advisory policy checks.

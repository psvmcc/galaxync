# galaxync

`galaxync` is a Rust daemon that mirrors Ansible Galaxy collections to a local filesystem. It supports multiple Galaxy API v3 servers, namespace/collection selectors, checksum reconciliation, atomic downloads, bounded concurrency, Prometheus metrics, and SIGHUP configuration reloads.

## Features

- YAML and TOML configuration with strict validation;
- full-tree, namespace, and collection selection;
- custom headers and environment-variable expansion;
- Ansible Galaxy-compatible default `User-Agent`;
- per-server concurrency and retry/backoff;
- optional prerelease filtering;
- streaming SHA-256 and artifact-size verification;
- atomic `.tmp` downloads with remote `created_at` as `mtime`;
- global state and per-server status files;
- runtime `.disable` marker;
- Prometheus, liveness, and readiness endpoints;
- minimal non-root `scratch` container image.

## Requirements

Local development requires Rust 1.98.1+, Cargo, and [just](https://github.com/casey/just). Podman or Docker is required for image builds.

## Quick start

```shell
just config-validate config.example.yaml
just run ./config.yaml
just check
```

The default configuration path is `/etc/galaxync/config.yaml`. Override it with `--config` or pass a path to `just run`.

## Configuration

YAML and TOML use the same schema. Unknown fields are rejected. `${NAME}` values are expanded from the environment; missing variables are errors.

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
  # Omit namespaces to mirror the complete tree.
  - label: community
    url: https://galaxy.ansible.com/api/
    enabled: true
    concurrency: 8
    include_prerelease: false

  # A namespace without collections means all collections in it.
  - label: internal
    url: https://galaxy.example.org/api/
    enabled: true
    concurrency: 4
    headers:
      Authorization: "Token ${GALAXY_TOKEN}"
      User-Agent: "internal-galaxy-mirror/1.0"
    namespaces:
      - name: platform
      - name: operations

  # A namespace with collections selects only those collections.
  - label: curated
    url: https://hub.example.org/api/galaxy/
    enabled: true
    concurrency: 6
    headers:
      Authorization: "Bearer ${HUB_TOKEN}"
    namespaces:
      - name: acme
        collections: [system, network]
      - name: community
```

Selection rules:

- omitted `namespaces`: all namespaces, collections, and versions;
- namespace without `collections`: all collections in that namespace;
- namespace with `collections`: only listed names;
- empty selector lists are rejected;
- `enabled` defaults to `true`.

Set `include_prerelease: true` to include SemVer versions such as `1.0.0-alpha.1`, `2.0.0-beta`, and `3.0.0-rc.1`. The default is `false`.

Validate a file:

```shell
galaxync config validate --config ./config.yaml
```

## Storage layout

```text
<storage_dir>/<server.label>/<namespace>/<collection>/
  <namespace>-<collection>-<version>.tar.gz
```

For example:

```text
/var/lib/galaxync/community/community/general/community-general-10.7.1.tar.gz
```

The daemon also writes these files inside `storage_dir`:

```text
.state.json                 global state and config generation
.lock                       single-process lock
<server.label>/.status.json  machine-readable server status
<server.label>/.disable     optional runtime disable marker
```

Downloads use `<filename>.tmp` beside the destination. The final name is published only after size and SHA-256 validation. Existing matching files are skipped. The remote version `created_at` becomes the local modification time.

## Disabling a server

Permanent configuration disablement:

```yaml
enabled: false
```

Temporary runtime disablement without editing configuration:

```shell
touch /var/lib/galaxync/community/.disable
rm /var/lib/galaxync/community/.disable
```

An optional text reason can be stored in the marker. Configuration disablement takes precedence. A marker appearing during a cycle stops new work while active atomic operations finish.

## Scheduling and signals

`run_on_start: true` starts an immediate cycle; otherwise the first cycle waits for `sync_interval`. Cycles for one server never overlap.

Reload the selected configuration path without restarting:

```shell
kill -HUP $(pgrep galaxync)
```

The candidate file is fully parsed and validated before activation. An invalid reload keeps the previous configuration. Running cycles retain their original snapshot; the next cycle uses the new generation. Metrics listener changes require restart.

SIGTERM and SIGINT stop new work, wait for active tasks within the shutdown timeout, write state/status, and release the lock. Startup cleans abandoned `.tmp` files and recovers stale `syncing` state.

## Prometheus and health

When enabled, the daemon exposes:

- `GET /metrics` — Prometheus metrics;
- `GET /healthz` — liveness;
- `GET /readyz` — configuration, state, and storage readiness.

The default listener is `127.0.0.1:9898`. Metrics include server state, sync timestamps and duration, discovered collections/versions, checked/downloaded/repaired artifacts, failures, retries, active requests, concurrency, and reload results. High-cardinality values such as collection names, versions, URLs, checksums, and error text are never labels.

## Container image

Build with Podman:

```shell
just container-build
```

The multi-stage build produces a static musl binary in a minimal `scratch`
runtime image. It runs as UID/GID `65532`, contains only the binary, CA
certificates, and the project license, and exposes port `9898`.

```shell
just container-run config_path=./config.yaml
```

Equivalent direct command:

```shell
podman run --rm \
  --mount type=bind,src="$PWD/config.yaml",dst=/etc/galaxync/config.yaml,ro=true \
  --mount type=volume,src=galaxync-data,dst=/var/lib/galaxync \
  --publish 127.0.0.1:9898:9898 \
  localhost/galaxync:dev
```

## systemd

A hardened unit is provided at
[`packaging/systemd/galaxync.service`](packaging/systemd/galaxync.service). For
a source-built binary installed in `/usr/local/bin`:

```shell
sudo install -m 0755 target/release/galaxync /usr/local/bin/galaxync
sudo install -m 0644 packaging/systemd/galaxync.service /etc/systemd/system/
sudo install -d -m 0750 /etc/galaxync
sudo install -m 0644 config.yaml /etc/galaxync/config.yaml
sudo systemctl daemon-reload
sudo systemctl enable --now galaxync
```

The unit uses `DynamicUser` and systemd's `StateDirectory`, which makes
`/var/lib/galaxync` writable without a persistent service account. Review the
configuration file permissions if it contains credentials.

### Podman Quadlet

[`packaging/quadlet/galaxync.container`](packaging/quadlet/galaxync.container)
provides a hardened Quadlet example using the published image and a persistent
host directory:

```shell
sudo install -d -m 0750 /etc/galaxync /etc/containers/systemd
sudo install -d -o 65532 -g 65532 -m 0750 /var/lib/galaxync
sudo install -m 0644 config.yaml /etc/galaxync/config.yaml
sudo install -m 0644 packaging/quadlet/galaxync.container /etc/containers/systemd/
sudo systemctl daemon-reload
sudo systemctl enable --now galaxync.service
```

The example binds metrics to `127.0.0.1:9898`; configure the container's
`metrics.listen` as `0.0.0.0:9898` so the published port is reachable. To pass
tokens referenced as `${NAME}` in the YAML/TOML configuration, add an
`EnvironmentFile=/etc/galaxync/galaxync.env` line to the Quadlet's `[Container]`
section and restrict that file to root (`0600`). After editing the Quadlet, run
`systemctl daemon-reload` and restart `galaxync.service`.

The complete `/etc/galaxync` directory is bind-mounted, so replacing
`config.yaml` atomically on the host is visible inside the container. Reload
the daemon configuration without restarting the container:

```shell
sudo systemctl reload galaxync.service
```

Quadlet's `ReloadSignal=HUP` generates the reload action and sends `SIGHUP` to
the container. The equivalent direct command is
`sudo podman kill --signal HUP galaxync`.

## Development commands

```shell
just fmt
just test
just clippy
just check
just deny
just audit
just licenses
just build
just run ./config.yaml
just config-validate ./config.yaml
just container-build
```

The tests cover configuration, selectors, URL construction, credentials,
checksums, atomic storage, state recovery, HTTP/retry metrics, concurrent
collection and version requests, initial/repeated/corrupt-file synchronization,
mid-cycle disabling, transactional SIGHUP reload, and graceful SIGTERM.

## GitHub Actions and releases

`.github/workflows/ci.yml` runs formatting, Clippy, unit/integration tests,
dependency policy and security checks, systemd/Quadlet validation, release
compilation, and a container smoke test. Third-party actions and container base
images are pinned to immutable commit or manifest digests; Dependabot proposes
updates.
`container-main.yml` publishes multi-architecture images with SBOM and
provenance attestations to `ghcr.io/psvmcc/galaxync:latest` on `main`.
`release.yml` handles `X.Y.Z` tags, publishes Linux archives and checksums, and
pushes versioned, `stable`, and `latest` GHCR tags. Release archives contain the
MIT license, README, and a generated third-party license report; an SPDX JSON
SBOM is published alongside them.

Release tags must match the package version in `Cargo.toml` and point to a commit reachable from `main`.

## Security notes

- Keep API tokens in environment variables or an external secret manager.
- Run as a non-root user and use a dedicated writable storage directory.
- Bind metrics to loopback unless external scraping is required.
- Review download and redirect hosts for private Galaxy installations.
- `delete_orphans` is reserved for a future safe-pruning implementation and
  currently must remain `false`; validation rejects `true` instead of silently
  ignoring it.

## License

galaxync is licensed under the [MIT License](LICENSE).

Third-party dependencies and container components remain subject to their own
licenses.

See [CONTRIBUTING.md](CONTRIBUTING.md) for development and submission guidance,
and [SECURITY.md](SECURITY.md) for private vulnerability reporting.

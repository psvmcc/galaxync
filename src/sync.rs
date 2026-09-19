use std::{
    collections::{HashMap, HashSet},
    fs::OpenOptions,
    future::Future,
    panic::AssertUnwindSafe,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use fs2::FileExt;
use futures::{FutureExt, StreamExt};
use sha2::{Digest, Sha256};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::{
    io::AsyncWriteExt,
    sync::{Mutex, watch},
    task::JoinSet,
};

use crate::{
    config::{Config, DEFAULT_CONFIG_PATH},
    galaxy::{
        Collection, CollectionVersion, GalaxyClient, GalaxyError, NamespaceSelection, RetryConfig,
    },
    metrics::{self, DisableSource, FailureReason, Metrics, ReloadResult, SyncResult},
    state::{
        CycleStats, GlobalState, ServerStatus, StatusFile, load_status, recover_status_file,
        write_json,
    },
    storage::{Storage, sha256_file},
};

#[derive(Debug, thiserror::Error)]
#[error("synchronization interrupted")]
struct SynchronizationInterrupted;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[arg(long, default_value=DEFAULT_CONFIG_PATH)]
    config: PathBuf,
    #[command(subcommand)]
    command: Option<Command>,
}
#[derive(Subcommand)]
enum Command {
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
}
#[derive(Subcommand)]
enum ConfigCommand {
    Validate,
}

pub async fn run() -> Result<()> {
    let cli = Cli::parse();
    if matches!(
        cli.command,
        Some(Command::Config {
            command: ConfigCommand::Validate
        })
    ) {
        Config::load(&cli.config)?;
        println!("configuration is valid");
        return Ok(());
    }
    let mut cfg = Arc::new(Config::load(&cli.config)?);
    tracing::info!(config=%cli.config.display(), storage=%cfg.storage_dir.display(), servers=cfg.servers.len(), "configuration loaded");
    let _lock = lock(&cfg.storage_dir)?;
    crate::storage::cleanup_tmp_files(&cfg.storage_dir).await?;
    let state_path = Arc::new(cfg.storage_dir.join(".status.json"));
    let mut loaded_state = GlobalState::load(&state_path).await?;
    let started_at = OffsetDateTime::now_utc();
    loaded_state.recover_stale_syncing(started_at);
    for server in &cfg.servers {
        let status_path = cfg.storage_dir.join(&server.label).join(".status.json");
        recover_status_file(&status_path, started_at).await?;
    }
    loaded_state.config_loaded(1, started_at);
    loaded_state.save(&state_path).await?;
    let global = Arc::new(Mutex::new(loaded_state));
    let metrics = Metrics::new();
    configure(&metrics, &cfg, 1);
    let (stop_tx, stop_rx) = watch::channel(false);
    let cancel_rx = stop_rx.clone();
    if cfg.metrics.enabled {
        let address = cfg.metrics.socket_addr()?;
        let listener = tokio::net::TcpListener::bind(address)
            .await
            .with_context(|| format!("bind metrics listener {address}"))?;
        let (p, m) = (cfg.metrics.path.clone(), metrics.clone());
        tokio::spawn(async move {
            let readiness = m.clone();
            if let Err(e) = metrics::serve(listener, &p, m, stop_rx).await {
                readiness.set_ready(false);
                tracing::error!(%e, "metrics server stopped")
            }
        });
    }
    metrics.set_ready(true);
    let mut generation = 1;
    let now = tokio::time::Instant::now();
    let mut deadlines: HashMap<String, tokio::time::Instant> = cfg
        .servers
        .iter()
        .map(|server| {
            let deadline = if cfg.run_on_start {
                now
            } else {
                now + server.sync_interval
            };
            (server.label.clone(), deadline)
        })
        .collect();
    let mut tasks = JoinSet::new();
    let mut running = HashSet::new();
    let mut hup = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    loop {
        tokio::select! {
          _=tokio::time::sleep_until(deadlines.values().copied().min().expect("at least one server"))=>{
              let now=tokio::time::Instant::now();
              for server in cfg.servers.clone(){
                  let due=deadlines.get(&server.label).is_some_and(|deadline| *deadline<=now);
                  if !due { continue; }
                  deadlines.insert(server.label.clone(), now+server.sync_interval);
                  if running.insert(server.label.clone()){
                      tracing::info!(server=%server.label,generation,"scheduling synchronization");
                      let label=server.label.clone();let c=cfg.clone();let m=metrics.clone();let cancel=cancel_rx.clone();let state=global.clone();let state_path=state_path.clone();
                      let panic_cfg=c.clone();let panic_server=server.clone();let panic_metrics=m.clone();let panic_state=state.clone();let panic_state_path=state_path.clone();
                      tasks.spawn(async move {
                          let result = AssertUnwindSafe(sync_server(c,server,generation,m,cancel,state,state_path))
                              .catch_unwind()
                              .await;
                          if result.is_err() {
                              mark_panicked(panic_cfg, panic_server, generation, panic_metrics, panic_state, panic_state_path).await;
                          }
                          (label, result)
                      });
                  }else{tracing::warn!(server=%server.label,"sync skipped because previous cycle is running");}
              }
          }
          Some(done)=tasks.join_next(),if !tasks.is_empty()=>{match done{
              Ok((label,Ok(result)))=>{running.remove(&label);if let Err(e)=result{tracing::error!(server=%label,%e,"sync failed")}}
              Ok((label,Err(_)))=>{running.remove(&label);tracing::error!(server=%label,"sync task panicked")}
              Err(e)=>tracing::error!(%e,"sync task join failed")
          }}
          _=hup.recv()=>match Config::load(&cli.config){
              Ok(n) if n.metrics != cfg.metrics => {metrics.config_reload(ReloadResult::Failed);tracing::error!("metrics settings require restart")},
              Ok(n) if n.storage_dir != cfg.storage_dir => {metrics.config_reload(ReloadResult::Failed);tracing::error!("storage_dir changes require restart")},
              Ok(n)=>{
                  let next_generation=generation+1;
                  let loaded_at=OffsetDateTime::now_utc();
                  {
                      let mut state=global.lock().await;
                      let mut next_state=state.clone();
                      next_state.config_loaded(next_generation,loaded_at);
                      if let Err(e)=next_state.save(&state_path).await {
                          metrics.config_reload(ReloadResult::Failed);
                          tracing::error!(%e,"cannot persist reloaded configuration state");
                          continue;
                      }
                      *state=next_state;
                  }
                  let old_cfg=cfg.clone();
                  cfg=Arc::new(n);
                  let now=tokio::time::Instant::now();
                  deadlines=cfg.servers.iter().map(|server| {
                      let deadline=old_cfg.servers.iter()
                          .find(|old| old.label==server.label && old.sync_interval==server.sync_interval)
                          .and_then(|_| deadlines.get(&server.label).copied())
                          .unwrap_or(now+server.sync_interval);
                      (server.label.clone(),deadline)
                  }).collect();
                  generation=next_generation;
                  configure(&metrics,&cfg,generation);
                  metrics.config_reload(ReloadResult::Success);
                  tracing::info!(generation,"configuration reloaded");
              },
              Err(e)=>{metrics.config_reload(ReloadResult::Failed);tracing::error!(%e,"configuration reload rejected")}
          },
          _=tokio::signal::ctrl_c()=>{tracing::info!(signal="SIGINT","shutdown signal received; waiting for active synchronization tasks");break},
          _=terminate.recv()=>{tracing::info!(signal="SIGTERM","shutdown signal received; waiting for active synchronization tasks");break},
        }
    }
    metrics.set_ready(false);
    let _ = stop_tx.send(true);
    if tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while let Some(result) = tasks.join_next().await {
            if let Ok((label, _)) = result {
                running.remove(&label);
            }
        }
    })
    .await
    .is_err()
    {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        let recovered_at = OffsetDateTime::now_utc();
        for label in &running {
            let status_path = cfg.storage_dir.join(label).join(".status.json");
            if let Some(status) = recover_status_file(&status_path, recovered_at).await?
                && status.status == ServerStatus::Interrupted
            {
                let mut state = global.lock().await;
                state.server_completed(
                    label.clone(),
                    ServerStatus::Interrupted,
                    recovered_at,
                    &status.stats,
                );
                state.save(&state_path).await?;
            }
        }
    }
    tracing::info!("galaxync stopped");
    Ok(())
}

fn lock(runtime_dir: &Path) -> Result<std::fs::File> {
    std::fs::create_dir_all(runtime_dir)
        .with_context(|| format!("create runtime directory {}", runtime_dir.display()))?;
    let p = runtime_dir.join(".lock");
    let f = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&p)
        .with_context(|| format!("open process lock {}", p.display()))?;
    f.try_lock_exclusive()
        .context("another galaxync instance is running")?;
    Ok(f)
}
fn configure(m: &Metrics, c: &Config, g: u64) {
    m.set_config_generation(g);
    m.reconcile_servers(c.servers.iter().map(|server| server.label.as_str()));
    for s in &c.servers {
        m.configure_server(&s.label, s.enabled, s.sync_interval, s.concurrency)
    }
}

async fn mark_panicked(
    cfg: Arc<Config>,
    server: crate::config::ServerConfig,
    generation: u64,
    metrics: Metrics,
    global: Arc<Mutex<GlobalState>>,
    state_path: Arc<PathBuf>,
) {
    let end = OffsetDateTime::now_utc();
    let status_path = cfg.storage_dir.join(&server.label).join(".status.json");
    let (stats, duration, status_saved) = match load_status(&status_path).await {
        Ok(Some(mut status)) => {
            let duration = status
                .started_at
                .and_then(|started| (end - started).try_into().ok())
                .unwrap_or_default();
            status.status = ServerStatus::Failed;
            status.config_generation = generation;
            status.updated_at = end;
            status.completed_at = Some(end);
            status.message = Some("synchronization task panicked".into());
            let stats = status.stats.clone();
            let saved = write_json(&status_path, &status).await;
            if let Err(error) = &saved {
                metrics.set_persistence_health(format!("status:{}", server.label), false);
                tracing::error!(server=%server.label, %error, "cannot persist panic status");
            }
            (stats, duration, saved.is_ok())
        }
        Ok(None) => {
            tracing::error!(server=%server.label, "synchronization task panicked without a status file");
            (CycleStats::default(), std::time::Duration::ZERO, false)
        }
        Err(error) => {
            tracing::error!(server=%server.label, %error, "cannot read synchronization status after panic");
            (CycleStats::default(), std::time::Duration::ZERO, false)
        }
    };
    metrics.sync_completed(
        &server.label,
        SyncResult::Failed,
        duration,
        end.unix_timestamp(),
    );
    let mut state = global.lock().await;
    state.server_completed(server.label.clone(), ServerStatus::Failed, end, &stats);
    let global_saved = state.save(&state_path).await;
    if let Err(error) = &global_saved {
        metrics.set_persistence_health("global-status", false);
        tracing::error!(%error, "cannot persist global state after synchronization panic");
    } else if status_saved {
        metrics.set_persistence_health("global-status", true);
        metrics.set_persistence_health(format!("status:{}", server.label), true);
    }
}

async fn sync_server(
    cfg: Arc<Config>,
    server: crate::config::ServerConfig,
    g: u64,
    m: Metrics,
    cancel: watch::Receiver<bool>,
    global: Arc<Mutex<GlobalState>>,
    state_path: Arc<PathBuf>,
) -> Result<()> {
    let cycle_clock = Instant::now();
    let store = Storage::new(&cfg.storage_dir)?;
    let dir = store.server_dir(&server.label)?;
    tokio::fs::create_dir_all(&dir).await?;
    let previous_last_successful = match crate::state::load_status(&dir.join(".status.json")).await
    {
        Ok(status) => status.and_then(|status| status.last_successful_sync_at),
        Err(error) => {
            tracing::warn!(server=%server.label, %error, "cannot read previous status; replacing it with the new cycle status");
            None
        }
    };
    let now = OffsetDateTime::now_utc();
    let initial = StatusFile {
        format_version: 1,
        server: server.label.clone(),
        status: ServerStatus::Syncing,
        run_id: Some(uuid::Uuid::now_v7().to_string()),
        config_generation: g,
        started_at: Some(now),
        updated_at: now,
        completed_at: None,
        last_successful_sync_at: previous_last_successful,
        message: None,
        stats: CycleStats::default(),
    };
    tracing::info!(server=%server.label, run_id=?initial.run_id, generation=g, "synchronization started");
    let progress = Arc::new(Mutex::new(CycleStats::default()));
    let initial_status_result = write_json(&dir.join(".status.json"), &initial).await;
    if let Err(error) = &initial_status_result {
        m.set_persistence_health(format!("status:{}", server.label), false);
        tracing::error!(server=%server.label, %error, "cannot persist initial synchronization status");
    } else {
        m.set_persistence_health(format!("status:{}", server.label), true);
    }
    let initial_global_result = {
        let mut state = global.lock().await;
        state.server_started(server.label.clone(), now);
        state.save(&state_path).await
    };
    if let Err(error) = &initial_global_result {
        m.set_persistence_health("global-status", false);
        tracing::error!(server=%server.label, %error, "cannot persist initial synchronization in global state");
    } else {
        m.set_persistence_health("global-status", true);
    }
    let result = if let Err(error) = initial_status_result {
        Err(error)
    } else {
        sync_server_work_inner(
            cfg,
            server.clone(),
            g,
            m.clone(),
            cancel,
            previous_last_successful,
            initial.run_id.clone(),
            progress.clone(),
        )
        .await
    };
    if let Err(error) = &result {
        let end = OffsetDateTime::now_utc();
        let interrupted = error.downcast_ref::<SynchronizationInterrupted>().is_some();
        let failed_status = if interrupted {
            ServerStatus::Interrupted
        } else {
            ServerStatus::Failed
        };
        m.sync_completed(
            &server.label,
            if interrupted {
                SyncResult::Interrupted
            } else {
                SyncResult::Failed
            },
            cycle_clock.elapsed(),
            end.unix_timestamp(),
        );
        let failed = StatusFile {
            status: failed_status,
            updated_at: end,
            completed_at: Some(end),
            message: Some(error.to_string()),
            stats: progress.lock().await.clone(),
            ..initial
        };
        let status_saved = write_json(&dir.join(".status.json"), &failed).await;
        if let Err(persist_error) = &status_saved {
            m.set_persistence_health(format!("status:{}", server.label), false);
            tracing::error!(server=%server.label, %persist_error, "cannot persist failed synchronization status");
        }
        tracing::error!(server=%server.label, status=?failed.status, %error, "synchronization finished with an error");
        {
            let mut state = global.lock().await;
            state.server_completed(server.label.clone(), failed_status, end, &failed.stats);
            let global_saved = state.save(&state_path).await;
            if let Err(persist_error) = &global_saved {
                m.set_persistence_health("global-status", false);
                tracing::error!(server=%server.label, %persist_error, "cannot persist failed synchronization in global state");
            }
            if status_saved.is_ok() && global_saved.is_ok() {
                m.set_persistence_health("global-status", true);
                m.set_persistence_health(format!("status:{}", server.label), true);
            }
        }
    } else if let Ok(completed) = &result {
        m.set_persistence_health(format!("status:{}", server.label), true);
        let mut state = global.lock().await;
        state.server_completed(
            server.label.clone(),
            completed.status,
            completed.completed_at,
            &completed.stats,
        );
        if let Err(error) = state.save(&state_path).await {
            m.set_persistence_health("global-status", false);
            tracing::error!(server=%server.label, %error, "cannot persist completed synchronization in global state");
        } else {
            m.set_persistence_health("global-status", true);
        }
        tracing::info!(server=%server.label, status=?completed.status, checked=completed.stats.artifacts_checked, downloaded=completed.stats.artifacts_downloaded, repaired=completed.stats.artifacts_repaired, failed=completed.stats.artifacts_failed, "synchronization finished");
    }
    result.map(|_| ())
}

struct CycleCompletion {
    status: ServerStatus,
    completed_at: OffsetDateTime,
    stats: CycleStats,
}

struct HeartbeatContext {
    path: PathBuf,
    server: String,
    run_id: Option<String>,
    generation: u64,
    started_at: OffsetDateTime,
    last_successful_sync_at: Option<OffsetDateTime>,
    progress: Arc<Mutex<CycleStats>>,
    metrics: Metrics,
}

impl HeartbeatContext {
    async fn write(&self) {
        let status = StatusFile {
            format_version: 1,
            server: self.server.clone(),
            status: ServerStatus::Syncing,
            run_id: self.run_id.clone(),
            config_generation: self.generation,
            started_at: Some(self.started_at),
            updated_at: OffsetDateTime::now_utc(),
            completed_at: None,
            last_successful_sync_at: self.last_successful_sync_at,
            message: None,
            stats: self.progress.lock().await.clone(),
        };
        match write_json(&self.path, &status).await {
            Ok(()) => self
                .metrics
                .set_persistence_health(format!("status:{}", self.server), true),
            Err(error) => {
                self.metrics
                    .set_persistence_health(format!("status:{}", self.server), false);
                tracing::warn!(server=%self.server, %error, "cannot write synchronization heartbeat");
            }
        }
    }
}

async fn await_with_heartbeat<T, F>(
    future: F,
    heartbeat: &HeartbeatContext,
    interval: &mut tokio::time::Interval,
    mut cancel: watch::Receiver<bool>,
) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    if *cancel.borrow() {
        return Err(SynchronizationInterrupted.into());
    }
    tokio::pin!(future);
    loop {
        tokio::select! {
            result = &mut future => return result,
            _ = interval.tick() => heartbeat.write().await,
            changed = cancel.changed() => {
                if changed.is_err() || *cancel.borrow() {
                    return Err(SynchronizationInterrupted.into());
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn sync_server_work_inner(
    cfg: Arc<Config>,
    server: crate::config::ServerConfig,
    g: u64,
    m: Metrics,
    cancel: watch::Receiver<bool>,
    previous_last_successful: Option<OffsetDateTime>,
    run_id: Option<String>,
    progress: Arc<Mutex<CycleStats>>,
) -> Result<CycleCompletion> {
    let store = Storage::new(&cfg.storage_dir)?;
    let dir = store.server_dir(&server.label)?;
    tokio::fs::create_dir_all(&dir).await?;
    let marker_disabled = server.enabled && store.is_disabled(&server.label).await?.is_some();
    m.set_disabled(&server.label, DisableSource::Config, !server.enabled);
    m.set_disabled(&server.label, DisableSource::Marker, marker_disabled);
    m.set_enabled(&server.label, server.enabled && !marker_disabled);
    if !server.enabled || marker_disabled {
        m.set_disabled(
            &server.label,
            if server.enabled {
                DisableSource::Marker
            } else {
                DisableSource::Config
            },
            true,
        );
        let now = OffsetDateTime::now_utc();
        write_json(
            &dir.join(".status.json"),
            &StatusFile {
                format_version: 1,
                server: server.label.clone(),
                status: ServerStatus::Disabled,
                run_id: None,
                config_generation: g,
                started_at: None,
                updated_at: now,
                completed_at: Some(now),
                last_successful_sync_at: previous_last_successful,
                message: Some("synchronization disabled".into()),
                stats: CycleStats::default(),
            },
        )
        .await?;
        tracing::info!(server=%server.label, source=if server.enabled {"marker"} else {"config"}, "synchronization disabled");
        return Ok(CycleCompletion {
            status: ServerStatus::Disabled,
            completed_at: now,
            stats: CycleStats::default(),
        });
    }
    let start = OffsetDateTime::now_utc();
    let clock = Instant::now();
    m.sync_started(&server.label, start.unix_timestamp());
    let mut stats = CycleStats::default();
    let headers =
        GalaxyClient::headers_from_strings(&server.headers.clone().into_iter().collect())?;
    let retry = RetryConfig {
        attempts: cfg.http.retry_attempts as usize,
        initial_delay: cfg.http.retry_initial_delay,
        max_delay: cfg.http.retry_max_delay,
    };
    let client = GalaxyClient::new(
        server.parsed_url()?,
        headers,
        server.concurrency,
        cfg.http.connect_timeout,
        cfg.http.request_timeout,
        cfg.http.download_timeout,
        retry.clone(),
    )?
    .with_metrics(server.label.clone(), m.clone());
    let heartbeat = HeartbeatContext {
        path: dir.join(".status.json"),
        server: server.label.clone(),
        run_id: run_id.clone(),
        generation: g,
        started_at: start,
        last_successful_sync_at: previous_last_successful,
        progress: progress.clone(),
        metrics: m.clone(),
    };
    let heartbeat_period = std::time::Duration::from_secs(5);
    let mut heartbeat_interval = tokio::time::interval_at(
        tokio::time::Instant::now() + heartbeat_period,
        heartbeat_period,
    );
    heartbeat_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tracing::info!(server=%server.label, url=%server.url, "discovering Galaxy API");
    let root = await_with_heartbeat(
        async { Ok(client.discover_v3().await?) },
        &heartbeat,
        &mut heartbeat_interval,
        cancel.clone(),
    )
    .await?;
    tracing::info!(server=%server.label, api=%root, "Galaxy API discovered; listing collections");
    let sel = server.namespaces.as_ref().map(|v| {
        v.iter()
            .map(|n| NamespaceSelection {
                name: n.name.clone(),
                collections: n.collections.clone(),
            })
            .collect::<Vec<_>>()
    });
    let collections = await_with_heartbeat(
        async { Ok(client.collections(&root, sel.as_deref()).await?) },
        &heartbeat,
        &mut heartbeat_interval,
        cancel.clone(),
    )
    .await?;
    stats.collections_discovered = collections.len() as u64;
    progress.lock().await.collections_discovered = stats.collections_discovered;
    tracing::info!(server=%server.label, collections=collections.len(), "collection discovery completed");
    let interrupted = Arc::new(AtomicBool::new(*cancel.borrow()));
    let disabled = Arc::new(AtomicBool::new(false));
    let include_prerelease = server.include_prerelease;
    let concurrency = server.concurrency;
    let results = futures::stream::iter(collections.into_iter().map(|collection| {
        let client = client.clone();
        let root = root.clone();
        let store = store.clone();
        let label = server.label.clone();
        let metrics = m.clone();
        let cancel = cancel.clone();
        let interrupted = interrupted.clone();
        let disabled = disabled.clone();
        let progress = progress.clone();
        let retry = retry.clone();
        async move {
            let mut local = CycleStats::default();
            if *cancel.borrow() {
                interrupted.store(true, Ordering::Release);
                return Ok(local);
            }
            if store.is_disabled(&label).await?.is_some() {
                disabled.store(true, Ordering::Release);
                return Ok(local);
            }
            tracing::info!(server=%label, namespace=%collection.namespace, collection=%collection.name, "listing collection versions");
            let versions = match client.collection_versions(&root, &collection).await {
                Ok(versions) => versions,
                Err(error) => {
                    local.artifacts_failed += 1;
                    progress.lock().await.artifacts_failed += 1;
                    metrics.artifact_failed(&label, FailureReason::Api);
                    tracing::warn!(server=%label, namespace=%collection.namespace, collection=%collection.name, %error, "collection versions unavailable; skipping collection");
                    return Ok(local);
                }
            };
            tracing::info!(server=%label, namespace=%collection.namespace, collection=%collection.name, versions=versions.len(), "collection versions discovered");
            let mut version_results = futures::stream::iter(versions.into_iter().map(|version| {
                sync_version(
                    client.clone(),
                    root.clone(),
                    store.clone(),
                    label.clone(),
                    collection.clone(),
                    version,
                    metrics.clone(),
                    include_prerelease,
                    cancel.clone(),
                    interrupted.clone(),
                    disabled.clone(),
                    retry.clone(),
                )
            }))
            .buffer_unordered(concurrency);
            while let Some(result) = version_results.next().await {
                match result {
                    Ok(value) => {
                        merge_stats(&mut local, &value);
                        let mut total = progress.lock().await;
                        merge_stats(&mut total, &value);
                    }
                    Err(error) => {
                        local.artifacts_failed += 1;
                        progress.lock().await.artifacts_failed += 1;
                        metrics.artifact_failed(&label, FailureReason::Filesystem);
                        tracing::error!(server=%label, %error, "version synchronization failed");
                    }
                }
            }
            Ok::<_, anyhow::Error>(local)
        }
    }))
    .buffer_unordered(server.concurrency)
    .collect::<Vec<_>>();
    let results = await_with_heartbeat(
        async { Ok(results.await) },
        &heartbeat,
        &mut heartbeat_interval,
        cancel.clone(),
    )
    .await?;
    for result in results {
        match result {
            Ok(local) => merge_stats(&mut stats, &local),
            Err(error) => {
                stats.artifacts_failed += 1;
                progress.lock().await.artifacts_failed += 1;
                m.artifact_failed(&server.label, FailureReason::Filesystem);
                tracing::error!(server=%server.label, %error, "collection synchronization failed");
            }
        }
    }
    if *cancel.borrow() {
        interrupted.store(true, Ordering::Release);
    }
    if store.is_disabled(&server.label).await?.is_some() {
        disabled.store(true, Ordering::Release);
    }
    let end = OffsetDateTime::now_utc();
    m.set_discovered(
        &server.label,
        stats.collections_discovered,
        stats.versions_discovered,
    );
    let was_interrupted = interrupted.load(Ordering::Acquire);
    let was_disabled = disabled.load(Ordering::Acquire);
    let outcome = if was_interrupted {
        SyncResult::Interrupted
    } else if was_disabled {
        SyncResult::Disabled
    } else if stats.artifacts_failed == 0 {
        SyncResult::Success
    } else {
        SyncResult::Partial
    };
    let status_outcome = if was_interrupted {
        ServerStatus::Interrupted
    } else if was_disabled {
        ServerStatus::Disabled
    } else if stats.artifacts_failed == 0 {
        ServerStatus::Synced
    } else {
        ServerStatus::Partial
    };
    if was_disabled {
        m.set_disabled(&server.label, DisableSource::Marker, true);
        m.set_enabled(&server.label, false);
    }
    write_json(
        &dir.join(".status.json"),
        &StatusFile {
            format_version: 1,
            server: server.label.clone(),
            status: status_outcome,
            run_id,
            config_generation: g,
            started_at: Some(start),
            updated_at: end,
            completed_at: Some(end),
            last_successful_sync_at: if status_outcome == ServerStatus::Synced {
                Some(end)
            } else {
                previous_last_successful
            },
            message: match status_outcome {
                ServerStatus::Interrupted => Some("synchronization interrupted".into()),
                ServerStatus::Disabled => Some("synchronization disabled during cycle".into()),
                _ => None,
            },
            stats: stats.clone(),
        },
    )
    .await?;
    m.sync_completed(
        &server.label,
        outcome,
        clock.elapsed(),
        end.unix_timestamp(),
    );
    Ok(CycleCompletion {
        status: status_outcome,
        completed_at: end,
        stats,
    })
}

#[cfg(test)]
async fn sync_server_work(
    cfg: Arc<Config>,
    server: crate::config::ServerConfig,
    generation: u64,
    metrics: Metrics,
    cancel: watch::Receiver<bool>,
    previous_last_successful: Option<OffsetDateTime>,
    run_id: Option<String>,
) -> Result<CycleCompletion> {
    sync_server_work_inner(
        cfg,
        server,
        generation,
        metrics,
        cancel,
        previous_last_successful,
        run_id,
        Arc::new(Mutex::new(CycleStats::default())),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn sync_version(
    client: GalaxyClient,
    root: url::Url,
    store: Storage,
    label: String,
    collection: Collection,
    version: CollectionVersion,
    metrics: Metrics,
    include_prerelease: bool,
    cancel: watch::Receiver<bool>,
    interrupted: Arc<AtomicBool>,
    disabled: Arc<AtomicBool>,
    retry: RetryConfig,
) -> Result<CycleStats> {
    let mut stats = CycleStats::default();
    if *cancel.borrow() {
        interrupted.store(true, Ordering::Release);
        return Ok(stats);
    }
    if store.is_disabled(&label).await?.is_some() {
        disabled.store(true, Ordering::Release);
        return Ok(stats);
    }
    if !include_prerelease
        && semver::Version::parse(&version.version)
            .map(|parsed| !parsed.pre.is_empty())
            .unwrap_or(false)
    {
        tracing::info!(server=%label, namespace=%collection.namespace, collection=%collection.name, version=%version.version, "skipping prerelease collection version");
        return Ok(stats);
    }
    stats.versions_discovered = 1;
    stats.artifacts_checked = 1;
    metrics.artifact_checked(&label);
    let detail = match client
        .collection_version_detail(&root, &collection.namespace, &collection.name, &version)
        .await
    {
        Ok(detail) => detail,
        Err(error) => {
            stats.artifacts_failed = 1;
            metrics.artifact_failed(&label, FailureReason::Api);
            tracing::warn!(server=%label, namespace=%collection.namespace, collection=%collection.name, version=%version.version, %error, "collection version metadata unavailable; skipping version");
            return Ok(stats);
        }
    };
    let path = store.artifact_path(
        &label,
        &collection.namespace,
        &collection.name,
        &version.version,
    )?;
    let existed = tokio::fs::try_exists(&path).await?;
    if existed
        && sha256_file(&path)
            .await?
            .eq_ignore_ascii_case(&detail.artifact.sha256)
    {
        return Ok(stats);
    }
    tracing::info!(server=%label, namespace=%collection.namespace, collection=%collection.name, version=%version.version, repair=existed, "downloading collection artifact");
    if let Err(error) = download(
        &client,
        &detail.download_url,
        &detail.artifact.sha256,
        detail.artifact.size,
        &detail.created_at,
        &path,
        retry,
    )
    .await
    {
        stats.artifacts_failed = 1;
        metrics.artifact_failed(&label, FailureReason::Download);
        tracing::error!(server=%label, namespace=%collection.namespace, collection=%collection.name, version=%version.version, %error, "artifact download failed");
        return Ok(stats);
    }
    stats.artifacts_downloaded = 1;
    metrics.artifact_downloaded(&label, detail.artifact.size.unwrap_or(0));
    if existed {
        stats.artifacts_repaired = 1;
        metrics.artifact_repaired(&label);
    }
    Ok(stats)
}

fn merge_stats(total: &mut CycleStats, value: &CycleStats) {
    total.versions_discovered += value.versions_discovered;
    total.artifacts_checked += value.artifacts_checked;
    total.artifacts_downloaded += value.artifacts_downloaded;
    total.artifacts_repaired += value.artifacts_repaired;
    total.artifacts_failed += value.artifacts_failed;
}

async fn download(
    c: &GalaxyClient,
    url: &str,
    sha: &str,
    size: Option<u64>,
    created: &str,
    path: &Path,
    retry: RetryConfig,
) -> Result<()> {
    for attempt in 0..=retry.attempts {
        match download_once(c, url, sha, size, created, path).await {
            Ok(()) => return Ok(()),
            Err(error) if attempt < retry.attempts && retryable_download_error(&error) => {
                let factor = 1u32.checked_shl(attempt.min(30) as u32).unwrap_or(u32::MAX);
                let delay = error
                    .downcast_ref::<GalaxyError>()
                    .and_then(GalaxyError::retry_after)
                    .unwrap_or_else(|| {
                        retry
                            .initial_delay
                            .saturating_mul(factor)
                            .min(retry.max_delay)
                    });
                c.record_download_retry();
                tokio::time::sleep(delay).await;
                tracing::warn!(attempt=attempt+1, max_attempts=retry.attempts+1, %error, "artifact download failed; retrying");
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("download retry loop executes at least once")
}

fn retryable_download_error(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<GalaxyError>()
        .is_some_and(GalaxyError::is_retryable)
        || error.downcast_ref::<reqwest::Error>().is_some_and(|error| {
            error.is_timeout() || error.is_connect() || error.is_request() || error.is_body()
        })
}

async fn download_once(
    c: &GalaxyClient,
    url: &str,
    sha: &str,
    size: Option<u64>,
    created: &str,
    path: &Path,
) -> Result<()> {
    tokio::fs::create_dir_all(path.parent().unwrap()).await?;
    let tmp = crate::storage::temporary_path(path);
    let result = async {
        let mut stream = c.download(url).await?.bytes_stream();
        let mut f = tokio::fs::File::create(&tmp).await?;
        let mut h = Sha256::new();
        let mut n = 0;
        while let Some(b) = stream.next().await {
            let b = b?;
            n += b.len() as u64;
            h.update(&b);
            f.write_all(&b).await?
        }
        f.flush().await?;
        f.sync_all().await?;
        drop(f);
        if size.is_some_and(|s| s != n) {
            bail!("size mismatch")
        }
        if !hex::encode(h.finalize()).eq_ignore_ascii_case(sha) {
            bail!("checksum mismatch")
        }
        let t = OffsetDateTime::parse(created, &Rfc3339)?.unix_timestamp();
        crate::storage::publish_download(&tmp, path, t).await
    }
    .await;
    if result.is_err() {
        let _ = crate::storage::remove_file_if_exists(&tmp).await;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderMap;
    use std::time::Duration;
    use url::Url;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    fn config(
        storage: &Path,
        url: &str,
        concurrency: usize,
    ) -> (Arc<Config>, crate::config::ServerConfig) {
        let text = format!(
            r#"version: 1
storage_dir: {}
http:
  retry_attempts: 1
  retry_initial_delay: 1ms
  retry_max_delay: 2ms
servers:
  - label: test
    url: {url}
    sync_interval: 1h
    concurrency: {concurrency}
"#,
            storage.display()
        );
        let config = Arc::new(Config::parse("config.yaml", &text).unwrap());
        let server = config.servers[0].clone();
        (config, server)
    }

    async fn mount_discovery(server: &MockServer, collections: serde_json::Value) {
        Mock::given(method("GET"))
            .and(path("/api/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"available_versions":{"v3":"/api/v3/"}})),
            )
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/api/v3/plugin/ansible/content/published/collections/index/",
            ))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"data":collections,"links":{}})),
            )
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn end_to_end_download_skip_and_repair() {
        let remote = MockServer::start().await;
        let artifact = b"collection artifact";
        let checksum = hex::encode(Sha256::digest(artifact));
        mount_discovery(
            &remote,
            serde_json::json!([{
                "name":"general",
                "namespace":"community",
                "versions_url":"/versions/general/"
            }]),
        )
        .await;
        Mock::given(method("GET"))
            .and(path("/versions/general/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data":[{"version":"1.0.0","href":"/details/general/1.0.0/"}],
                "links":{}
            })))
            .mount(&remote)
            .await;
        Mock::given(method("GET"))
            .and(path("/details/general/1.0.0/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "version":"1.0.0",
                "created_at":"2024-01-02T03:04:05Z",
                "download_url":format!("{}/download/general-1.0.0.tar.gz", remote.uri()),
                "artifact":{"sha256":checksum,"size":artifact.len()}
            })))
            .mount(&remote)
            .await;
        Mock::given(method("GET"))
            .and(path("/download/general-1.0.0.tar.gz"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(artifact))
            .expect(2)
            .mount(&remote)
            .await;

        let temp = tempfile::tempdir().unwrap();
        let (cfg, server) = config(temp.path(), &format!("{}/api/", remote.uri()), 2);
        let metrics = Metrics::new();
        metrics.configure_server("test", true, Duration::from_secs(3600), 2);
        let (_stop_tx, stop_rx) = watch::channel(false);
        let path = Storage::new(temp.path())
            .unwrap()
            .artifact_path("test", "community", "general", "1.0.0")
            .unwrap();

        sync_server_work(
            cfg.clone(),
            server.clone(),
            1,
            metrics.clone(),
            stop_rx.clone(),
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(tokio::fs::read(&path).await.unwrap(), artifact);

        sync_server_work(
            cfg.clone(),
            server.clone(),
            1,
            metrics.clone(),
            stop_rx.clone(),
            None,
            None,
        )
        .await
        .unwrap();
        tokio::fs::write(&path, b"corrupt").await.unwrap();
        sync_server_work(cfg, server, 1, metrics.clone(), stop_rx, None, None)
            .await
            .unwrap();
        assert_eq!(tokio::fs::read(&path).await.unwrap(), artifact);

        let status = crate::state::load_status(&temp.path().join("test/.status.json"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.status, ServerStatus::Synced);
        assert_eq!(status.stats.artifacts_repaired, 1);
        let encoded = metrics.encode().unwrap();
        assert!(encoded.contains("galaxync_http_requests_total{server=\"test\""));
        assert!(encoded.contains("galaxync_active_requests{server=\"test\"} 0"));
    }

    #[tokio::test]
    async fn collection_requests_honor_concurrency() {
        let remote = MockServer::start().await;
        mount_discovery(
            &remote,
            serde_json::json!([
                {"name":"one","namespace":"acme","versions_url":"/versions/one/"},
                {"name":"two","namespace":"acme","versions_url":"/versions/two/"}
            ]),
        )
        .await;
        for name in ["one", "two"] {
            Mock::given(method("GET"))
                .and(path(format!("/versions/{name}/")))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_delay(Duration::from_millis(500))
                        .set_body_json(serde_json::json!({"data":[],"links":{}})),
                )
                .mount(&remote)
                .await;
        }
        let temp = tempfile::tempdir().unwrap();
        let (cfg, server) = config(temp.path(), &format!("{}/api/", remote.uri()), 2);
        let metrics = Metrics::new();
        let (_stop_tx, stop_rx) = watch::channel(false);
        let started = Instant::now();
        sync_server_work(cfg, server, 1, metrics, stop_rx, None, None)
            .await
            .unwrap();
        assert!(
            started.elapsed() < Duration::from_millis(900),
            "version requests were not concurrent: {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn version_requests_honor_concurrency_for_one_collection() {
        let remote = MockServer::start().await;
        mount_discovery(
            &remote,
            serde_json::json!([{
                "name":"one",
                "namespace":"acme",
                "versions_url":"/versions/one/"
            }]),
        )
        .await;
        Mock::given(method("GET"))
            .and(path("/versions/one/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data":[
                    {"version":"1.0.0","href":"/details/one/1.0.0/"},
                    {"version":"2.0.0","href":"/details/one/2.0.0/"}
                ],
                "links":{}
            })))
            .mount(&remote)
            .await;
        for version in ["1.0.0", "2.0.0"] {
            Mock::given(method("GET"))
                .and(path(format!("/details/one/{version}/")))
                .respond_with(ResponseTemplate::new(404).set_delay(Duration::from_millis(500)))
                .mount(&remote)
                .await;
        }
        let temp = tempfile::tempdir().unwrap();
        let (cfg, server) = config(temp.path(), &format!("{}/api/", remote.uri()), 2);
        let metrics = Metrics::new();
        let (_stop_tx, stop_rx) = watch::channel(false);
        let started = Instant::now();
        sync_server_work(cfg, server, 1, metrics, stop_rx, None, None)
            .await
            .unwrap();
        assert!(
            started.elapsed() < Duration::from_millis(900),
            "version detail requests were not concurrent: {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn cancellation_is_not_reported_as_success() {
        let remote = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_secs(5))
                    .set_body_json(serde_json::json!({
                        "available_versions":{"v3":"/api/v3/"}
                    })),
            )
            .mount(&remote)
            .await;
        let temp = tempfile::tempdir().unwrap();
        let (cfg, server) = config(temp.path(), &format!("{}/api/", remote.uri()), 2);
        let metrics = Metrics::new();
        let (stop_tx, stop_rx) = watch::channel(false);
        let global = Arc::new(Mutex::new(GlobalState::default()));
        let state_path = Arc::new(temp.path().join(".status.json"));
        let task = tokio::spawn(sync_server(
            cfg, server, 1, metrics, stop_rx, global, state_path,
        ));
        tokio::time::sleep(Duration::from_millis(50)).await;
        stop_tx.send(true).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .expect("cancellation did not stop discovery")
                .unwrap()
                .is_err()
        );
        let status = crate::state::load_status(&temp.path().join("test/.status.json"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.status, ServerStatus::Interrupted);
    }

    #[tokio::test]
    async fn retries_transient_api_failures_and_records_metric() {
        let remote = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/"))
            .respond_with(ResponseTemplate::new(503))
            .with_priority(1)
            .up_to_n_times(1)
            .expect(1)
            .mount(&remote)
            .await;
        mount_discovery(&remote, serde_json::json!([])).await;
        let temp = tempfile::tempdir().unwrap();
        let (cfg, server) = config(temp.path(), &format!("{}/api/", remote.uri()), 2);
        let metrics = Metrics::new();
        let (_stop_tx, stop_rx) = watch::channel(false);
        sync_server_work(cfg, server, 1, metrics.clone(), stop_rx, None, None)
            .await
            .unwrap();
        assert!(
            metrics
                .encode()
                .unwrap()
                .contains("galaxync_http_retries_total{server=\"test\",operation=\"discovery\"} 1")
        );
    }

    #[tokio::test]
    async fn download_uses_one_retry_budget() {
        let remote = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/artifact.tar.gz"))
            .respond_with(ResponseTemplate::new(503))
            .expect(2)
            .mount(&remote)
            .await;
        let metrics = Metrics::new();
        let client = GalaxyClient::new(
            Url::parse(&format!("{}/", remote.uri())).unwrap(),
            HeaderMap::new(),
            1,
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
            RetryConfig {
                attempts: 4,
                initial_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
            },
        )
        .unwrap()
        .with_metrics("test", metrics.clone());
        let temp = tempfile::tempdir().unwrap();
        let result = download(
            &client,
            &format!("{}/artifact.tar.gz", remote.uri()),
            &hex::encode(Sha256::digest(b"unused")),
            None,
            "2024-01-02T03:04:05Z",
            &temp.path().join("artifact.tar.gz"),
            RetryConfig {
                attempts: 1,
                initial_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
            },
        )
        .await;
        assert!(result.is_err());
        assert!(
            metrics
                .encode()
                .unwrap()
                .contains("galaxync_http_retries_total{server=\"test\",operation=\"download\"} 1")
        );
    }

    #[tokio::test]
    async fn download_stream_holds_concurrency_permit() {
        let remote = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/artifact.tar.gz"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"artifact"))
            .expect(2)
            .mount(&remote)
            .await;
        let client = GalaxyClient::new(
            Url::parse(&format!("{}/", remote.uri())).unwrap(),
            HeaderMap::new(),
            1,
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
            RetryConfig::default(),
        )
        .unwrap();
        let url = format!("{}/artifact.tar.gz", remote.uri());
        let stream = client.download(&url).await.unwrap().bytes_stream();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), client.download(&url))
                .await
                .is_err(),
            "a second download started while the first body was still held"
        );
        drop(stream);
        client.download(&url).await.unwrap();
    }

    #[tokio::test]
    async fn download_does_not_retry_permanent_status() {
        let remote = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/missing.tar.gz"))
            .respond_with(ResponseTemplate::new(404))
            .expect(1)
            .mount(&remote)
            .await;
        let client = GalaxyClient::new(
            Url::parse(&format!("{}/", remote.uri())).unwrap(),
            HeaderMap::new(),
            1,
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
            RetryConfig::default(),
        )
        .unwrap();
        let result = download(
            &client,
            &format!("{}/missing.tar.gz", remote.uri()),
            "unused",
            None,
            "2024-01-02T03:04:05Z",
            &tempfile::tempdir().unwrap().path().join("artifact.tar.gz"),
            RetryConfig::default(),
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn download_honors_retry_after() {
        let remote = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/artifact.tar.gz"))
            .respond_with(ResponseTemplate::new(503).insert_header("Retry-After", "0"))
            .with_priority(1)
            .up_to_n_times(1)
            .expect(1)
            .mount(&remote)
            .await;
        let artifact = b"artifact";
        Mock::given(method("GET"))
            .and(path("/artifact.tar.gz"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(artifact))
            .expect(1)
            .mount(&remote)
            .await;
        let client = GalaxyClient::new(
            Url::parse(&format!("{}/", remote.uri())).unwrap(),
            HeaderMap::new(),
            1,
            Duration::from_secs(1),
            Duration::from_secs(1),
            Duration::from_secs(1),
            RetryConfig::default(),
        )
        .unwrap();
        let temp = tempfile::tempdir().unwrap();
        let started = Instant::now();
        download(
            &client,
            &format!("{}/artifact.tar.gz", remote.uri()),
            &hex::encode(Sha256::digest(artifact)),
            Some(artifact.len() as u64),
            "2024-01-02T03:04:05Z",
            &temp.path().join("artifact.tar.gz"),
            RetryConfig {
                attempts: 1,
                initial_delay: Duration::from_millis(500),
                max_delay: Duration::from_millis(500),
            },
        )
        .await
        .unwrap();
        assert!(started.elapsed() < Duration::from_millis(300));
    }

    #[tokio::test]
    async fn disable_marker_during_cycle_sets_disabled_status() {
        let remote = MockServer::start().await;
        mount_discovery(
            &remote,
            serde_json::json!([{
                "name":"one",
                "namespace":"acme",
                "versions_url":"/versions/one/"
            }]),
        )
        .await;
        Mock::given(method("GET"))
            .and(path("/versions/one/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(300))
                    .set_body_json(serde_json::json!({
                        "data":[{"version":"1.0.0"}],
                        "links":{}
                    })),
            )
            .mount(&remote)
            .await;
        let temp = tempfile::tempdir().unwrap();
        let (cfg, server) = config(temp.path(), &format!("{}/api/", remote.uri()), 1);
        let metrics = Metrics::new();
        let (_stop_tx, stop_rx) = watch::channel(false);
        let task = tokio::spawn(sync_server_work(
            cfg, server, 1, metrics, stop_rx, None, None,
        ));
        tokio::time::sleep(Duration::from_millis(100)).await;
        tokio::fs::create_dir_all(temp.path().join("test"))
            .await
            .unwrap();
        tokio::fs::write(temp.path().join("test/.disable"), b"maintenance")
            .await
            .unwrap();
        task.await.unwrap().unwrap();
        let status = crate::state::load_status(&temp.path().join("test/.status.json"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.status, ServerStatus::Disabled);
    }
}

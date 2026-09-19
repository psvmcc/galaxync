use std::{collections::BTreeMap, io::ErrorKind, path::Path};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokio::fs;

use crate::storage::atomic_write;

pub const STATE_FORMAT_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GlobalState {
    pub format_version: u32,
    pub config_generation: u64,
    #[serde(with = "time::serde::rfc3339::option")]
    pub config_loaded_at: Option<OffsetDateTime>,
    pub servers: BTreeMap<String, ServerSummary>,
}

impl Default for GlobalState {
    fn default() -> Self {
        Self {
            format_version: STATE_FORMAT_VERSION,
            config_generation: 0,
            config_loaded_at: None,
            servers: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerSummary {
    #[serde(with = "time::serde::rfc3339::option")]
    pub last_started_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub last_completed_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub last_successful_sync_at: Option<OffsetDateTime>,
    pub last_status: Option<ServerStatus>,
    pub collections_seen: u64,
    pub artifacts_verified: u64,
    pub artifacts_downloaded: u64,
    pub artifacts_failed: u64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ServerStatus {
    Syncing,
    Synced,
    Partial,
    Failed,
    Interrupted,
    Disabled,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StatusFile {
    pub format_version: u32,
    pub server: String,
    pub status: ServerStatus,
    pub run_id: Option<String>,
    pub config_generation: u64,
    #[serde(with = "time::serde::rfc3339::option")]
    pub started_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    pub completed_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub last_successful_sync_at: Option<OffsetDateTime>,
    pub message: Option<String>,
    pub stats: CycleStats,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CycleStats {
    pub collections_discovered: u64,
    pub versions_discovered: u64,
    pub artifacts_checked: u64,
    pub artifacts_downloaded: u64,
    pub artifacts_repaired: u64,
    pub artifacts_failed: u64,
}

impl GlobalState {
    /// Loads state from disk. A missing file is a normal first-start condition.
    pub async fn load(path: &Path) -> Result<Self> {
        let bytes = match fs::read(path).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Self::default()),
            Err(error) => {
                return Err(error).with_context(|| format!("read state file {}", path.display()));
            }
        };
        let state: Self = serde_json::from_slice(&bytes)
            .with_context(|| format!("parse state file {}", path.display()))?;
        if state.format_version != STATE_FORMAT_VERSION {
            anyhow::bail!(
                "unsupported state format version {} in {} (expected {})",
                state.format_version,
                path.display(),
                STATE_FORMAT_VERSION
            );
        }
        Ok(state)
    }

    /// Persists a complete snapshot using the same-directory `.tmp` + rename protocol.
    pub async fn save(&self, path: &Path) -> Result<()> {
        write_json(path, self)
            .await
            .with_context(|| format!("write state file {}", path.display()))
    }

    pub fn config_loaded(&mut self, generation: u64, loaded_at: OffsetDateTime) {
        self.config_generation = generation;
        self.config_loaded_at = Some(loaded_at);
    }

    pub fn server_started(&mut self, server: impl Into<String>, started_at: OffsetDateTime) {
        let summary = self.servers.entry(server.into()).or_default();
        summary.last_started_at = Some(started_at);
        summary.last_status = Some(ServerStatus::Syncing);
    }

    pub fn server_completed(
        &mut self,
        server: impl Into<String>,
        status: ServerStatus,
        completed_at: OffsetDateTime,
        stats: &CycleStats,
    ) {
        let summary = self.servers.entry(server.into()).or_default();
        summary.last_completed_at = Some(completed_at);
        summary.last_status = Some(status);
        summary.collections_seen = stats.collections_discovered;
        summary.artifacts_verified = stats.artifacts_checked;
        summary.artifacts_downloaded = stats.artifacts_downloaded;
        summary.artifacts_failed = stats.artifacts_failed;
        if status == ServerStatus::Synced {
            summary.last_successful_sync_at = Some(completed_at);
        }
    }

    pub fn server_disabled(&mut self, server: impl Into<String>) {
        self.servers.entry(server.into()).or_default().last_status = Some(ServerStatus::Disabled);
    }

    /// Marks runs that were still active when the previous process stopped as interrupted.
    /// Returns server labels whose state changed.
    pub fn recover_stale_syncing(&mut self, recovered_at: OffsetDateTime) -> Vec<String> {
        let mut recovered = Vec::new();
        for (server, summary) in &mut self.servers {
            if summary.last_status == Some(ServerStatus::Syncing) {
                summary.last_status = Some(ServerStatus::Interrupted);
                summary.last_completed_at = Some(recovered_at);
                recovered.push(server.clone());
            }
        }
        recovered
    }
}

impl StatusFile {
    pub fn recover_if_stale(&mut self, recovered_at: OffsetDateTime) -> bool {
        if self.status != ServerStatus::Syncing {
            return false;
        }
        self.status = ServerStatus::Interrupted;
        self.updated_at = recovered_at;
        self.completed_at = Some(recovered_at);
        self.message
            .get_or_insert_with(|| "Previous synchronization did not complete".to_owned());
        true
    }
}

/// Loads a per-server status file when present.
pub async fn load_status(path: &Path) -> Result<Option<StatusFile>> {
    let bytes = match fs::read(path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| format!("read status file {}", path.display()));
        }
    };
    serde_json::from_slice(&bytes)
        .with_context(|| format!("parse status file {}", path.display()))
        .map(Some)
}

/// Recovers a stale `syncing` status in place and persists the transition atomically.
pub async fn recover_status_file(
    path: &Path,
    recovered_at: OffsetDateTime,
) -> Result<Option<StatusFile>> {
    let Some(mut status) = load_status(path).await? else {
        return Ok(None);
    };
    if status.recover_if_stale(recovered_at) {
        write_json(path, &status).await?;
    }
    Ok(Some(status))
}

pub async fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    atomic_write(path, &serde_json::to_vec_pretty(value)?).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()
    }

    #[tokio::test]
    async fn missing_state_loads_current_default() {
        let dir = tempfile::tempdir().unwrap();
        let state = GlobalState::load(&dir.path().join("missing.json"))
            .await
            .unwrap();
        assert_eq!(state.format_version, STATE_FORMAT_VERSION);
        assert!(state.servers.is_empty());
    }

    #[tokio::test]
    async fn state_round_trip_and_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".status.json");
        let mut state = GlobalState::default();
        state.config_loaded(3, now());
        state.server_started("community", now());
        state.server_completed(
            "community",
            ServerStatus::Synced,
            now() + time::Duration::seconds(10),
            &CycleStats {
                collections_discovered: 2,
                artifacts_checked: 7,
                artifacts_downloaded: 1,
                ..CycleStats::default()
            },
        );
        state.save(&path).await.unwrap();
        let json = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(json.contains("\"last_completed_at\": \"2023-11-14T22:13:30Z\""));

        let loaded = GlobalState::load(&path).await.unwrap();
        assert_eq!(loaded.config_generation, 3);
        let server = &loaded.servers["community"];
        assert_eq!(server.last_status, Some(ServerStatus::Synced));
        assert_eq!(server.artifacts_verified, 7);
        assert_eq!(server.last_successful_sync_at, server.last_completed_at);
        assert!(!path.with_extension("json.tmp").exists());
    }

    #[test]
    fn stale_global_runs_are_interrupted() {
        let mut state = GlobalState::default();
        state.server_started("stale", now());
        state.server_completed("done", ServerStatus::Synced, now(), &CycleStats::default());
        assert_eq!(state.recover_stale_syncing(now()), vec!["stale"]);
        assert_eq!(
            state.servers["stale"].last_status,
            Some(ServerStatus::Interrupted)
        );
        assert_eq!(
            state.servers["done"].last_status,
            Some(ServerStatus::Synced)
        );
    }

    #[tokio::test]
    async fn stale_status_file_is_recovered_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".status.json");
        let status = StatusFile {
            format_version: STATE_FORMAT_VERSION,
            server: "community".into(),
            status: ServerStatus::Syncing,
            run_id: Some("run-1".into()),
            config_generation: 1,
            started_at: Some(now()),
            updated_at: now(),
            completed_at: None,
            last_successful_sync_at: None,
            message: None,
            stats: CycleStats::default(),
        };
        write_json(&path, &status).await.unwrap();
        let recovered_at = now() + time::Duration::minutes(1);
        let recovered = recover_status_file(&path, recovered_at)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(recovered.status, ServerStatus::Interrupted);
        assert_eq!(recovered.completed_at, Some(recovered_at));
        assert!(recovered.message.unwrap().contains("did not complete"));
        assert_eq!(
            load_status(&path).await.unwrap().unwrap().status,
            ServerStatus::Interrupted
        );
    }

    #[tokio::test]
    async fn malformed_state_is_not_silently_discarded() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".status.json");
        tokio::fs::write(&path, b"not json").await.unwrap();
        assert!(GlobalState::load(&path).await.is_err());
    }
}

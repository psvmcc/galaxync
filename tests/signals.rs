#![cfg(unix)]

use std::{
    fs,
    path::Path,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use serde_json::Value;
use tempfile::TempDir;

struct Daemon(Child);

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn write_config(path: &Path, storage: &Path, interval: &str) {
    fs::write(
        path,
        format!(
            r#"version: 1
storage_dir: {}
run_on_start: false
servers:
  - label: test
    url: https://example.invalid/api/
    sync_interval: {interval}
"#,
            storage.display()
        ),
    )
    .unwrap();
}

fn wait_for_generation(state: &Path, expected: u64) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(bytes) = fs::read(state)
            && let Ok(value) = serde_json::from_slice::<Value>(&bytes)
            && value["config_generation"] == expected
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "generation {expected} was not persisted"
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn signal(pid: u32, name: &str) {
    let status = Command::new("kill")
        .args([name, &pid.to_string()])
        .status()
        .unwrap();
    assert!(status.success());
}

#[test]
fn sighup_reload_is_transactional_and_sigterm_is_graceful() {
    let temp = TempDir::new().unwrap();
    let storage = temp.path().join("storage");
    let config = temp.path().join("config.yaml");
    write_config(&config, &storage, "1h");

    let child = Command::new(env!("CARGO_BIN_EXE_galaxync"))
        .args(["--config", config.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut daemon = Daemon(child);
    let state = storage.join(".status.json");
    wait_for_generation(&state, 1);

    write_config(&config, &storage, "2h");
    signal(daemon.0.id(), "-HUP");
    wait_for_generation(&state, 2);

    fs::write(&config, "not: valid: yaml").unwrap();
    signal(daemon.0.id(), "-HUP");
    thread::sleep(Duration::from_millis(200));
    let state_value: Value = serde_json::from_slice(&fs::read(&state).unwrap()).unwrap();
    assert_eq!(state_value["config_generation"], 2);

    signal(daemon.0.id(), "-TERM");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = daemon.0.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        assert!(
            Instant::now() < deadline,
            "daemon did not stop after SIGTERM"
        );
        thread::sleep(Duration::from_millis(25));
    }
}

#[test]
fn occupied_metrics_address_fails_startup() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let temp = TempDir::new().unwrap();
    let storage = temp.path().join("storage");
    let config = temp.path().join("config.yaml");
    fs::write(
        &config,
        format!(
            r#"version: 1
storage_dir: {}
run_on_start: false
metrics:
  enabled: true
  listen: {address}
servers:
  - label: test
    url: https://example.invalid/api/
    sync_interval: 1h
"#,
            storage.display()
        ),
    )
    .unwrap();
    let status = Command::new(env!("CARGO_BIN_EXE_galaxync"))
        .args(["--config", config.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();
    assert!(!status.success());
}

#[test]
fn process_lock_rejects_overlap_and_is_reusable_after_exit() {
    let temp = TempDir::new().unwrap();
    let storage = temp.path().join("storage");
    let config = temp.path().join("config.yaml");
    write_config(&config, &storage, "1h");

    let first = Command::new(env!("CARGO_BIN_EXE_galaxync"))
        .args(["--config", config.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut first = Daemon(first);
    wait_for_generation(&storage.join(".status.json"), 1);

    let overlapping = Command::new(env!("CARGO_BIN_EXE_galaxync"))
        .args(["--config", config.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();
    assert!(!overlapping.success());

    signal(first.0.id(), "-TERM");
    assert!(first.0.wait().unwrap().success());

    let restarted = Command::new(env!("CARGO_BIN_EXE_galaxync"))
        .args(["--config", config.to_str().unwrap()])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut restarted = Daemon(restarted);
    thread::sleep(Duration::from_millis(100));
    assert!(restarted.0.try_wait().unwrap().is_none());
    signal(restarted.0.id(), "-TERM");
    assert!(restarted.0.wait().unwrap().success());
}

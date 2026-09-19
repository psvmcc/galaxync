use std::{fs, process::Command};

use tempfile::TempDir;

fn write_config(temp: &TempDir, name: &str, contents: &str) -> std::path::PathBuf {
    let path = temp.path().join(name);
    fs::write(&path, contents).expect("write test configuration");
    path
}

fn validate_command(path: &std::path::Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_galaxync"));
    command
        .arg("--config")
        .arg(path)
        .args(["config", "validate"]);
    command
}

#[test]
fn validates_yaml_configuration_via_cli() {
    let temp = TempDir::new().expect("create temporary directory");
    let path = write_config(
        &temp,
        "galaxync.yaml",
        r#"
version: 1
storage_dir: /var/lib/galaxync-test
sync_interval: 15m
metrics:
  enabled: false
servers:
  - label: community
    url: https://galaxy.ansible.com/api/
    concurrency: 8
    namespaces:
      - name: community
        collections:
          - general
          - docker
"#,
    );

    let output = validate_command(&path)
        .output()
        .expect("run configuration validation");

    assert!(
        output.status.success(),
        "validation failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        "configuration is valid"
    );
}

#[test]
fn validates_toml_configuration_and_expands_environment() {
    let temp = TempDir::new().expect("create temporary directory");
    let path = write_config(
        &temp,
        "galaxync.toml",
        r#"
version = 1
storage_dir = "${GALAXYNC_TEST_STORAGE}"
sync_interval = "1h"

[[servers]]
label = "internal"
url = "https://galaxy.example.test/api/"
enabled = false

[servers.headers]
Authorization = "Token ${GALAXYNC_TEST_TOKEN}"

[[servers.namespaces]]
name = "acme"
collections = ["system", "network"]
"#,
    );

    let output = validate_command(&path)
        .env("GALAXYNC_TEST_STORAGE", temp.path())
        .env("GALAXYNC_TEST_TOKEN", "not-a-real-secret")
        .output()
        .expect("run configuration validation");

    assert!(
        output.status.success(),
        "validation failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        "configuration is valid"
    );
}

#[test]
fn rejects_unknown_fields_via_cli() {
    let temp = TempDir::new().expect("create temporary directory");
    let path = write_config(
        &temp,
        "invalid.yaml",
        r#"
version: 1
storage_dir: /var/lib/galaxync-test
sync_interval: 5m
servers:
  - label: community
    url: https://galaxy.ansible.com/api/
    concurency: 4
"#,
    );

    let output = validate_command(&path)
        .output()
        .expect("run configuration validation");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("unknown field `concurency`"), "{stderr}");
}

#[test]
fn rejects_missing_environment_variable_without_leaking_placeholder_context() {
    let temp = TempDir::new().expect("create temporary directory");
    let path = write_config(
        &temp,
        "missing-env.yaml",
        r#"
version: 1
storage_dir: /var/lib/galaxync-test
sync_interval: 5m
servers:
  - label: community
    url: https://galaxy.ansible.com/api/
    headers:
      Authorization: "Token ${GALAXYNC_TEST_DEFINITELY_MISSING}"
"#,
    );

    let output = validate_command(&path)
        .env_remove("GALAXYNC_TEST_DEFINITELY_MISSING")
        .output()
        .expect("run configuration validation");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(
            "environment variable GALAXYNC_TEST_DEFINITELY_MISSING referenced by configuration is not set"
        ),
        "{stderr}"
    );
    assert!(!stderr.contains("Token ${"), "{stderr}");
}

#[test]
fn rejects_configuration_with_unsupported_extension() {
    let temp = TempDir::new().expect("create temporary directory");
    let path = write_config(
        &temp,
        "galaxync.json",
        r#"{"version":1,"storage_dir":"/tmp","sync_interval":"1h","servers":[]}"#,
    );

    let output = validate_command(&path)
        .output()
        .expect("run configuration validation");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("unsupported configuration extension"),
        "{stderr}"
    );
}

//! Loading and validation of the daemon configuration.

use std::{
    collections::{BTreeMap, HashSet},
    env,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use http::{HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

pub const DEFAULT_CONFIG_PATH: &str = "/etc/galaxync/config.yaml";
pub const DEFAULT_USER_AGENT: &str = "ansible-galaxy/2.19.0";

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("cannot read configuration {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("unsupported configuration extension for {0}; expected .yaml, .yml, or .toml")]
    UnsupportedFormat(PathBuf),
    #[error("invalid YAML configuration: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("invalid TOML configuration: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("environment variable {name} referenced by configuration is not set")]
    MissingEnvironment { name: String },
    #[error("invalid configuration: {0}")]
    Validation(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub version: u32,
    pub storage_dir: PathBuf,
    #[serde(with = "duration_serde")]
    pub sync_interval: Duration,
    #[serde(default = "default_true")]
    pub run_on_start: bool,
    #[serde(default)]
    pub overlap_policy: OverlapPolicy,
    #[serde(default)]
    pub delete_orphans: bool,
    #[serde(default)]
    pub http: HttpConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
    pub servers: Vec<ServerConfig>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OverlapPolicy {
    #[default]
    Skip,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HttpConfig {
    #[serde(default = "default_connect_timeout", with = "duration_serde")]
    pub connect_timeout: Duration,
    #[serde(default = "default_request_timeout", with = "duration_serde")]
    pub request_timeout: Duration,
    #[serde(default = "default_download_timeout", with = "duration_serde")]
    pub download_timeout: Duration,
    #[serde(default = "default_retry_attempts")]
    pub retry_attempts: u32,
    #[serde(default = "default_retry_initial_delay", with = "duration_serde")]
    pub retry_initial_delay: Duration,
    #[serde(default = "default_retry_max_delay", with = "duration_serde")]
    pub retry_max_delay: Duration,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            connect_timeout: default_connect_timeout(),
            request_timeout: default_request_timeout(),
            download_timeout: default_download_timeout(),
            retry_attempts: default_retry_attempts(),
            retry_initial_delay: default_retry_initial_delay(),
            retry_max_delay: default_retry_max_delay(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_metrics_listen")]
    pub listen: String,
    #[serde(default = "default_metrics_path")]
    pub path: String,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            listen: default_metrics_listen(),
            path: default_metrics_path(),
        }
    }
}

impl MetricsConfig {
    pub fn socket_addr(&self) -> Result<SocketAddr, ConfigError> {
        self.listen.parse().map_err(|_| {
            ConfigError::Validation(format!(
                "metrics.listen must be an IP socket address, got {:?}",
                self.listen
            ))
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub label: String,
    pub url: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    /// Download prerelease versions such as 1.0.0-beta.1 when true.
    #[serde(default)]
    pub include_prerelease: bool,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    pub namespaces: Option<Vec<NamespaceConfig>>,
}

impl ServerConfig {
    pub fn parsed_url(&self) -> Result<Url, ConfigError> {
        Url::parse(&self.url).map_err(|e| {
            ConfigError::Validation(format!("server {:?} has invalid url: {e}", self.label))
        })
    }

    /// Returns the configured user agent, or the Ansible-compatible default.
    pub fn user_agent(&self) -> &str {
        self.headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("user-agent"))
            .map(|(_, value)| value.as_str())
            .unwrap_or(DEFAULT_USER_AGENT)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceConfig {
    pub name: String,
    pub collections: Option<Vec<String>>,
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        Self::parse(path, &contents)
    }

    pub fn parse(path: impl AsRef<Path>, contents: &str) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let extension = path
            .extension()
            .and_then(|value| value.to_str())
            .map(str::to_ascii_lowercase);
        let mut config: Self = match extension.as_deref() {
            Some("yaml" | "yml") => serde_yaml::from_str(contents)?,
            Some("toml") => toml::from_str(contents)?,
            _ => return Err(ConfigError::UnsupportedFormat(path.to_path_buf())),
        };
        config.expand_environment()?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.version != 1 {
            return validation("version must be 1");
        }
        if !self.storage_dir.is_absolute() {
            return validation("storage_dir must be absolute");
        }
        if self.sync_interval.is_zero() {
            return validation("sync_interval must be greater than zero");
        }
        if self.delete_orphans {
            return validation(
                "delete_orphans is not supported yet; keep it false to prevent unsafe deletion",
            );
        }
        if self.servers.is_empty() {
            return validation("at least one server is required");
        }
        if self.http.connect_timeout.is_zero()
            || self.http.request_timeout.is_zero()
            || self.http.download_timeout.is_zero()
        {
            return validation("HTTP timeouts must be greater than zero");
        }
        if self.http.retry_initial_delay.is_zero() || self.http.retry_max_delay.is_zero() {
            return validation("retry delays must be greater than zero");
        }
        if self.http.retry_initial_delay > self.http.retry_max_delay {
            return validation("http.retry_initial_delay must not exceed retry_max_delay");
        }
        self.metrics.socket_addr()?;
        if !self.metrics.path.starts_with('/')
            || self.metrics.path.contains('?')
            || self.metrics.path.contains('#')
        {
            return validation(
                "metrics.path must be an absolute HTTP path without query or fragment",
            );
        }

        let mut labels = HashSet::new();
        for server in &self.servers {
            validate_component("server label", &server.label)?;
            if !labels.insert(server.label.as_str()) {
                return validation(format!("duplicate server label {:?}", server.label));
            }
            if server.concurrency == 0 {
                return validation(format!(
                    "server {:?} concurrency must be greater than zero",
                    server.label
                ));
            }
            let url = server.parsed_url()?;
            if !matches!(url.scheme(), "http" | "https") || url.host().is_none() {
                return validation(format!(
                    "server {:?} url must be an absolute HTTP(S) URL",
                    server.label
                ));
            }
            validate_headers(server)?;
            if let Some(namespaces) = &server.namespaces {
                if namespaces.is_empty() {
                    return validation(format!(
                        "server {:?} namespaces must not be empty",
                        server.label
                    ));
                }
                let mut seen_namespaces = HashSet::new();
                for namespace in namespaces {
                    validate_component("namespace", &namespace.name)?;
                    if !seen_namespaces.insert(namespace.name.as_str()) {
                        return validation(format!(
                            "duplicate namespace {:?} for server {:?}",
                            namespace.name, server.label
                        ));
                    }
                    if let Some(collections) = &namespace.collections {
                        if collections.is_empty() {
                            return validation(format!(
                                "collections for namespace {:?} must not be empty",
                                namespace.name
                            ));
                        }
                        let mut seen = HashSet::new();
                        for collection in collections {
                            validate_component("collection", collection)?;
                            if !seen.insert(collection.as_str()) {
                                return validation(format!(
                                    "duplicate collection {:?} in namespace {:?}",
                                    collection, namespace.name
                                ));
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn expand_environment(&mut self) -> Result<(), ConfigError> {
        self.storage_dir = PathBuf::from(expand(self.storage_dir.to_string_lossy().as_ref())?);
        self.metrics.listen = expand(&self.metrics.listen)?;
        self.metrics.path = expand(&self.metrics.path)?;
        for server in &mut self.servers {
            server.label = expand(&server.label)?;
            server.url = expand(&server.url)?;
            let mut headers = BTreeMap::new();
            for (name, value) in std::mem::take(&mut server.headers) {
                headers.insert(expand(&name)?, expand(&value)?);
            }
            server.headers = headers;
            if let Some(namespaces) = &mut server.namespaces {
                for namespace in namespaces {
                    namespace.name = expand(&namespace.name)?;
                    if let Some(collections) = &mut namespace.collections {
                        for collection in collections {
                            *collection = expand(collection)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

fn validate_headers(server: &ServerConfig) -> Result<(), ConfigError> {
    let mut names = HashSet::new();
    for (name, value) in &server.headers {
        let normalized = name.to_ascii_lowercase();
        if !names.insert(normalized) {
            return validation(format!(
                "server {:?} contains duplicate header {:?} (case-insensitive)",
                server.label, name
            ));
        }
        HeaderName::from_bytes(name.as_bytes()).map_err(|_| {
            ConfigError::Validation(format!(
                "server {:?} has invalid HTTP header name {:?}",
                server.label, name
            ))
        })?;
        HeaderValue::from_str(value).map_err(|_| {
            ConfigError::Validation(format!(
                "server {:?} has invalid value for HTTP header {:?}",
                server.label, name
            ))
        })?;
    }
    Ok(())
}

fn validate_component(kind: &str, value: &str) -> Result<(), ConfigError> {
    if value.is_empty()
        || value == "."
        || value.contains("..")
        || value.contains(['/', '\\', '\0'])
        || value.chars().any(char::is_control)
    {
        return validation(format!("{kind} {value:?} is not a safe path component"));
    }
    Ok(())
}

fn validation<T>(message: impl Into<String>) -> Result<T, ConfigError> {
    Err(ConfigError::Validation(message.into()))
}

fn expand(input: &str) -> Result<String, ConfigError> {
    expand_with(input, |name| env::var(name).ok())
}

fn expand_with(
    input: &str,
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<String, ConfigError> {
    let mut output = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(start) = rest.find("${") {
        output.push_str(&rest[..start]);
        let variable = &rest[start + 2..];
        let Some(end) = variable.find('}') else {
            return validation("unterminated environment variable expression");
        };
        let name = &variable[..end];
        if name.is_empty()
            || !name.bytes().enumerate().all(|(i, c)| {
                c == b'_' || c.is_ascii_alphanumeric() && (i > 0 || !c.is_ascii_digit())
            })
        {
            return validation(format!("invalid environment variable name {name:?}"));
        }
        output.push_str(
            &lookup(name).ok_or_else(|| ConfigError::MissingEnvironment { name: name.into() })?,
        );
        rest = &variable[end + 1..];
    }
    output.push_str(rest);
    Ok(output)
}

fn default_true() -> bool {
    true
}
fn default_concurrency() -> usize {
    4
}
fn default_retry_attempts() -> u32 {
    5
}
fn default_connect_timeout() -> Duration {
    Duration::from_secs(10)
}
fn default_request_timeout() -> Duration {
    Duration::from_secs(60)
}
fn default_download_timeout() -> Duration {
    Duration::from_secs(30 * 60)
}
fn default_retry_initial_delay() -> Duration {
    Duration::from_secs(1)
}
fn default_retry_max_delay() -> Duration {
    Duration::from_secs(30)
}
fn default_metrics_listen() -> String {
    "127.0.0.1:9898".into()
}
fn default_metrics_path() -> String {
    "/metrics".into()
}

mod duration_serde {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};
    use std::time::Duration;

    pub fn serialize<S: Serializer>(value: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&humantime::format_duration(*value).to_string())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
        let value = String::deserialize(deserializer)?;
        humantime::parse_duration(&value).map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const YAML: &str = r#"
version: 1
storage_dir: /var/lib/galaxync
sync_interval: 6h
servers:
  - label: community
    url: https://galaxy.ansible.com/api/
"#;

    #[test]
    fn parses_yaml_with_defaults() {
        let c = Config::parse("config.yaml", YAML).unwrap();
        assert_eq!(c.sync_interval, Duration::from_secs(21600));
        assert!(c.run_on_start && !c.delete_orphans && c.servers[0].enabled);
        assert_eq!(c.servers[0].concurrency, 4);
        assert_eq!(c.servers[0].user_agent(), DEFAULT_USER_AGENT);
    }

    #[test]
    fn parses_toml_and_selectors() {
        let c = Config::parse(
            "config.toml",
            r#"
version = 1
storage_dir = "/srv/galaxy"
sync_interval = "10m"
[[servers]]
label = "internal"
url = "https://example.test/api/"
concurrency = 9
[servers.headers]
User-Agent = "custom/1"
[[servers.namespaces]]
name = "acme"
collections = ["system", "network"]
"#,
        )
        .unwrap();
        assert_eq!(c.servers[0].user_agent(), "custom/1");
        assert_eq!(
            c.servers[0].namespaces.as_ref().unwrap()[0]
                .collections
                .as_ref()
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn rejects_unknown_fields_in_nested_structures() {
        let bad = YAML.replace(
            "url: https://galaxy.ansible.com/api/",
            "url: https://galaxy.ansible.com/api/\n    typo: true",
        );
        assert!(matches!(
            Config::parse("x.yml", &bad),
            Err(ConfigError::Yaml(_))
        ));
    }

    #[test]
    fn expansion_handles_multiple_variables_and_errors() {
        assert_eq!(
            expand_with("Token ${A}:${B}", |n| Some(n.to_lowercase())).unwrap(),
            "Token a:b"
        );
        assert!(matches!(
            expand_with("${MISSING}", |_| None),
            Err(ConfigError::MissingEnvironment { .. })
        ));
        assert!(expand_with("${BROKEN", |_| None).is_err());
        assert!(expand_with("${1BAD}", |_| None).is_err());
    }

    #[test]
    fn rejects_empty_and_duplicate_selectors() {
        let empty = YAML.replace(
            "url: https://galaxy.ansible.com/api/",
            "url: https://galaxy.ansible.com/api/\n    namespaces: []",
        );
        assert!(Config::parse("x.yaml", &empty).is_err());
        let duplicate = YAML.replace("url: https://galaxy.ansible.com/api/", "url: https://galaxy.ansible.com/api/\n    namespaces:\n      - name: acme\n        collections: [one, one]");
        assert!(Config::parse("x.yaml", &duplicate).is_err());
    }

    #[test]
    fn rejects_unsafe_values_invalid_urls_and_zero_concurrency() {
        for replacement in [
            "label: ../bad\n    url: https://example.test/",
            "label: ok\n    url: file:///tmp/x",
            "label: ok\n    url: https://example.test/\n    concurrency: 0",
        ] {
            let bad = YAML.replace(
                "label: community\n    url: https://galaxy.ansible.com/api/",
                replacement,
            );
            assert!(
                Config::parse("x.yaml", &bad).is_err(),
                "accepted {replacement}"
            );
        }
    }

    #[test]
    fn rejects_case_insensitive_duplicate_headers() {
        let bad = YAML.replace("url: https://galaxy.ansible.com/api/", "url: https://galaxy.ansible.com/api/\n    headers:\n      User-Agent: one\n      user-agent: two");
        assert!(Config::parse("x.yaml", &bad).is_err());
    }

    #[test]
    fn rejects_bad_extension_and_duration() {
        assert!(matches!(
            Config::parse("config.json", YAML),
            Err(ConfigError::UnsupportedFormat(_))
        ));
        assert!(Config::parse("x.yaml", &YAML.replace("6h", "never")).is_err());
    }

    #[test]
    fn rejects_unsupported_orphan_deletion() {
        let config = YAML.replace(
            "sync_interval: 6h",
            "sync_interval: 6h\ndelete_orphans: true",
        );
        let error = Config::parse("x.yaml", &config).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("delete_orphans is not supported")
        );
    }
}

use std::fmt;
use std::fmt::Display;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use serde::Deserialize;
use thiserror::Error;

use crate::uplink::UplinkConfig;

/// Must stay in line with the receiver's `MAX_RECORDS_PER_BATCH`.
const MAX_BATCH_SIZE: usize = 10_000;

/// A string that never shows up in logs or `Debug` output.
#[derive(Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(transparent)]
pub struct Secret(String);

impl Secret {
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.0.is_empty() {
            "<unset>"
        } else {
            "<redacted>"
        })
    }
}

impl From<&str> for Secret {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub device_id: String,
    pub iface: String,
    pub receiver_url: String,
    pub receiver_token: Secret,
    pub batch_size: usize,
    pub flush_interval_secs: f64,
    pub final_flush_secs: f64,
    pub outbox_path: PathBuf,
    pub outbox_cap: u64,
    pub health_interval_secs: f64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            device_id: "tractor-1".into(),
            iface: "vcan0".into(),
            receiver_url: "http://127.0.0.1:18000".into(),
            receiver_token: Secret::default(),
            batch_size: 500,
            flush_interval_secs: 5.0,
            final_flush_secs: 10.0,
            outbox_path: "edge-outbox.db".into(),
            outbox_cap: 500_000,
            health_interval_secs: 30.0,
        }
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("cannot read config file {}: {source}", path.display())]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("invalid config file {}: {source}", path.display())]
    Parse {
        path: PathBuf,
        source: toml::de::Error,
    },
    #[error("invalid {var}={value:?}: {reason}")]
    Env {
        var: String,
        value: String,
        reason: String,
    },
    #[error("invalid config: {}", .0.join("; "))]
    Invalid(Vec<String>),
}

impl Config {
    /// Defaults, overridden by the TOML file (if given), overridden by `EDGE_*` variables.
    ///
    /// `env` looks up one variable. `main` passes `std::env::var`, tests pass a map, so
    /// tests never have to modify the real process environment.
    pub fn load(
        path: Option<&Path>,
        env: impl Fn(&str) -> Option<String>,
    ) -> Result<Self, ConfigError> {
        let mut config = match path {
            Some(path) => {
                let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
                    path: path.to_owned(),
                    source,
                })?;
                toml::from_str(&text).map_err(|source| ConfigError::Parse {
                    path: path.to_owned(),
                    source,
                })?
            }
            None => Self::default(),
        };
        config.apply_env(&env)?;
        config.validate()?;
        Ok(config)
    }

    fn apply_env(&mut self, env: &impl Fn(&str) -> Option<String>) -> Result<(), ConfigError> {
        override_from(env, "EDGE_DEVICE_ID", &mut self.device_id)?;
        override_from(env, "EDGE_IFACE", &mut self.iface)?;
        override_from(env, "EDGE_RECEIVER_URL", &mut self.receiver_url)?;
        if let Some(token) = env("EDGE_RECEIVER_TOKEN") {
            self.receiver_token = Secret(token);
        }
        override_from(env, "EDGE_BATCH_SIZE", &mut self.batch_size)?;
        override_from(
            env,
            "EDGE_FLUSH_INTERVAL_SECS",
            &mut self.flush_interval_secs,
        )?;
        override_from(env, "EDGE_FINAL_FLUSH_SECS", &mut self.final_flush_secs)?;
        override_from(env, "EDGE_OUTBOX_PATH", &mut self.outbox_path)?;
        override_from(env, "EDGE_OUTBOX_CAP", &mut self.outbox_cap)?;
        override_from(
            env,
            "EDGE_HEALTH_INTERVAL_SECS",
            &mut self.health_interval_secs,
        )?;
        Ok(())
    }

    /// Reports every problem at once, so a broken config needs one fix-and-retry, not ten.
    fn validate(&self) -> Result<(), ConfigError> {
        let mut problems = Vec::new();

        let valid_id_char = |c: char| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-');
        if self.device_id.is_empty()
            || self.device_id.len() > 64
            || !self.device_id.chars().all(valid_id_char)
        {
            problems.push("device_id must be 1-64 characters of A-Z a-z 0-9 . _ -".to_owned());
        }
        if self.iface.is_empty() {
            problems.push("iface must not be empty".to_owned());
        }
        match reqwest::Url::parse(&self.receiver_url) {
            Ok(url) if matches!(url.scheme(), "http" | "https") => {}
            _ => problems.push(format!(
                "receiver_url {:?} is not an http(s) URL",
                self.receiver_url
            )),
        }
        if self.receiver_token.expose().is_empty() {
            problems.push("receiver_token is required (set EDGE_RECEIVER_TOKEN)".to_owned());
        }
        if !(1..=MAX_BATCH_SIZE).contains(&self.batch_size) {
            problems.push(format!("batch_size must be 1-{MAX_BATCH_SIZE}"));
        }
        for (name, secs) in [
            ("flush_interval_secs", self.flush_interval_secs),
            ("final_flush_secs", self.final_flush_secs),
            ("health_interval_secs", self.health_interval_secs),
        ] {
            if !(secs.is_finite() && secs > 0.0) {
                problems.push(format!("{name} must be a positive number"));
            }
        }
        if self.outbox_cap == 0 {
            problems.push("outbox_cap must be at least 1".to_owned());
        }

        if problems.is_empty() {
            Ok(())
        } else {
            Err(ConfigError::Invalid(problems))
        }
    }

    pub fn health_interval(&self) -> Duration {
        Duration::from_secs_f64(self.health_interval_secs)
    }

    pub fn uplink(&self) -> UplinkConfig {
        UplinkConfig {
            url: self.receiver_url.clone(),
            token: self.receiver_token.expose().to_owned(),
            device_id: self.device_id.clone(),
            batch_size: self.batch_size,
            flush_interval: Duration::from_secs_f64(self.flush_interval_secs),
            request_timeout: Duration::from_secs(10),
            final_flush_timeout: Duration::from_secs_f64(self.final_flush_secs),
            backoff_base: Duration::from_millis(500),
            backoff_max: Duration::from_secs(60),
        }
    }
}

fn override_from<T>(
    env: &impl Fn(&str) -> Option<String>,
    var: &str,
    target: &mut T,
) -> Result<(), ConfigError>
where
    T: FromStr,
    T::Err: Display,
{
    if let Some(value) = env(var) {
        *target = value.parse().map_err(|e: T::Err| ConfigError::Env {
            var: var.to_owned(),
            reason: e.to_string(),
            value,
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;

    fn env(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let vars: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name| vars.get(name).cloned()
    }

    fn toml_file(content: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file
    }

    fn problems(result: Result<Config, ConfigError>) -> Vec<String> {
        match result {
            Err(ConfigError::Invalid(problems)) => problems,
            other => panic!("expected validation errors, got {other:?}"),
        }
    }

    #[test]
    fn defaults_plus_token_are_valid() {
        let config = Config::load(None, env(&[("EDGE_RECEIVER_TOKEN", "t")])).unwrap();
        assert_eq!(
            config,
            Config {
                receiver_token: "t".into(),
                ..Config::default()
            }
        );
    }

    #[test]
    fn token_is_required() {
        assert_eq!(
            problems(Config::load(None, env(&[]))),
            ["receiver_token is required (set EDGE_RECEIVER_TOKEN)"]
        );
    }

    #[test]
    fn file_overrides_defaults_and_env_overrides_file() {
        let file = toml_file(
            r#"
            device_id = "from-file"
            iface = "can0"
            batch_size = 100
            receiver_token = "file-token"
            "#,
        );
        let config = Config::load(
            Some(file.path()),
            env(&[
                ("EDGE_BATCH_SIZE", "250"),
                ("EDGE_RECEIVER_TOKEN", "env-token"),
            ]),
        )
        .unwrap();
        assert_eq!(config.device_id, "from-file");
        assert_eq!(config.iface, "can0");
        assert_eq!(config.batch_size, 250);
        assert_eq!(config.receiver_token.expose(), "env-token");
        assert_eq!(config.outbox_cap, Config::default().outbox_cap);
    }

    #[test]
    fn every_field_has_an_env_variable() {
        let config = Config::load(
            None,
            env(&[
                ("EDGE_DEVICE_ID", "d"),
                ("EDGE_IFACE", "can1"),
                ("EDGE_RECEIVER_URL", "https://example.com"),
                ("EDGE_RECEIVER_TOKEN", "t"),
                ("EDGE_BATCH_SIZE", "7"),
                ("EDGE_FLUSH_INTERVAL_SECS", "1.5"),
                ("EDGE_FINAL_FLUSH_SECS", "3"),
                ("EDGE_OUTBOX_PATH", "/var/lib/x.db"),
                ("EDGE_OUTBOX_CAP", "99"),
                ("EDGE_HEALTH_INTERVAL_SECS", "60"),
            ]),
        )
        .unwrap();
        assert_eq!(
            config,
            Config {
                device_id: "d".into(),
                iface: "can1".into(),
                receiver_url: "https://example.com".into(),
                receiver_token: "t".into(),
                batch_size: 7,
                flush_interval_secs: 1.5,
                final_flush_secs: 3.0,
                outbox_path: "/var/lib/x.db".into(),
                outbox_cap: 99,
                health_interval_secs: 60.0,
            }
        );
    }

    #[test]
    fn bad_env_number_names_the_variable() {
        let err = Config::load(
            None,
            env(&[("EDGE_RECEIVER_TOKEN", "t"), ("EDGE_BATCH_SIZE", "lots")]),
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid EDGE_BATCH_SIZE=\"lots\": invalid digit found in string"
        );
    }

    #[test]
    fn unknown_keys_in_file_are_rejected() {
        let file = toml_file("batch_sise = 100\n");
        let err = Config::load(Some(file.path()), env(&[])).unwrap_err();
        assert!(matches!(err, ConfigError::Parse { .. }), "{err}");
        assert!(err.to_string().contains("batch_sise"), "{err}");
    }

    #[test]
    fn missing_file_is_an_error() {
        let err = Config::load(Some(Path::new("/nonexistent/edge.toml")), env(&[])).unwrap_err();
        assert!(matches!(err, ConfigError::Read { .. }));
    }

    #[test]
    fn reports_all_problems_at_once() {
        let problems = problems(Config::load(
            None,
            env(&[
                ("EDGE_RECEIVER_TOKEN", "t"),
                ("EDGE_DEVICE_ID", "tractor/1"),
                ("EDGE_RECEIVER_URL", "ftp://x"),
                ("EDGE_BATCH_SIZE", "0"),
                ("EDGE_FLUSH_INTERVAL_SECS", "-1"),
                ("EDGE_HEALTH_INTERVAL_SECS", "NaN"),
                ("EDGE_OUTBOX_CAP", "0"),
            ]),
        ));
        assert_eq!(problems.len(), 6, "{problems:#?}");
    }

    #[test]
    fn debug_output_hides_the_token() {
        let config = Config::load(None, env(&[("EDGE_RECEIVER_TOKEN", "s3cret")])).unwrap();
        let debug = format!("{config:?}");
        assert!(!debug.contains("s3cret"));
        assert!(debug.contains("<redacted>"));
    }

    #[test]
    fn example_config_in_deploy_is_valid() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../deploy/config.example.toml");
        let config = Config::load(Some(&path), env(&[("EDGE_RECEIVER_TOKEN", "t")])).unwrap();
        assert_eq!(
            config.outbox_path,
            Path::new("/var/lib/edge-agent/outbox.db")
        );
    }
}

use std::path::Path;
use std::time::Duration;

use derive_getters::Getters;
use eyre::{Result, WrapErr, eyre};
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use url::Url;

#[derive(Debug, Deserialize, Getters)]
pub(crate) struct ClientConfig {
    /// Public WebSocket URL of the server's control endpoint.
    server_url: Url,
    auth_token: SecretString,
    /// Local URL inbound public requests are forwarded to.
    local_target: Url,
    #[serde(default)]
    client_name: Option<String>,
    #[serde(default = "default_min_backoff")]
    reconnect_min_secs: u64,
    #[serde(default = "default_max_backoff")]
    reconnect_max_secs: u64,
}

fn default_min_backoff() -> u64 {
    1
}
fn default_max_backoff() -> u64 {
    30
}

impl ClientConfig {
    pub(crate) fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .wrap_err_with(|| format!("Failed to read client config from '{}'", path.display()))?;
        let cfg: Self = toml::from_str(&raw)
            .wrap_err_with(|| format!("Failed to parse client config at '{}'", path.display()))?;

        if cfg.auth_token.expose_secret().trim().is_empty() {
            return Err(eyre!("auth_token must not be empty"));
        }
        match cfg.server_url.scheme() {
            "ws" | "wss" => {}
            other => {
                return Err(eyre!(
                    "server_url scheme must be 'ws' or 'wss' (got '{other}')"
                ));
            }
        }
        match cfg.local_target.scheme() {
            "http" | "https" => {}
            other => {
                return Err(eyre!(
                    "local_target scheme must be 'http' or 'https' (got '{other}')"
                ));
            }
        }
        Ok(cfg)
    }

    pub(crate) fn min_backoff(&self) -> Duration {
        Duration::from_secs(self.reconnect_min_secs.max(1))
    }

    pub(crate) fn max_backoff(&self) -> Duration {
        Duration::from_secs(self.reconnect_max_secs.max(self.reconnect_min_secs.max(1)))
    }
}

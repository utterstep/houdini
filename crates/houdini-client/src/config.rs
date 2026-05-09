use std::path::Path;
use std::time::Duration;

use serde::Deserialize;
use url::Url;

#[derive(Debug, Clone, Deserialize)]
pub struct ClientConfig {
    /// Public URL of the server's control WebSocket endpoint.
    /// e.g. `wss://tunnel.example.com/_houdini/v1/control`.
    pub server_url: Url,
    /// Shared secret matching the server's `auth_token`.
    pub auth_token: String,
    /// Local URL to which inbound public requests are forwarded.
    /// e.g. `http://127.0.0.1:3000`.
    pub local_target: Url,
    /// Friendly name reported to the server. Optional.
    #[serde(default)]
    pub client_name: Option<String>,
    /// Reconnect backoff in seconds, capped. Defaults to (1, 30).
    #[serde(default = "default_min_backoff")]
    pub reconnect_min_secs: u64,
    #[serde(default = "default_max_backoff")]
    pub reconnect_max_secs: u64,
}

fn default_min_backoff() -> u64 {
    1
}
fn default_max_backoff() -> u64 {
    30
}

impl ClientConfig {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("read {}: {e}", path.display()))?;
        let cfg: Self = toml::from_str(&raw)?;
        if cfg.auth_token.trim().is_empty() {
            anyhow::bail!("auth_token must not be empty");
        }
        match cfg.server_url.scheme() {
            "ws" | "wss" => {}
            other => anyhow::bail!("server_url scheme must be ws or wss, got {other}"),
        }
        match cfg.local_target.scheme() {
            "http" | "https" => {}
            other => anyhow::bail!("local_target scheme must be http or https, got {other}"),
        }
        Ok(cfg)
    }

    pub fn min_backoff(&self) -> Duration {
        Duration::from_secs(self.reconnect_min_secs.max(1))
    }

    pub fn max_backoff(&self) -> Duration {
        Duration::from_secs(self.reconnect_max_secs.max(self.reconnect_min_secs.max(1)))
    }
}

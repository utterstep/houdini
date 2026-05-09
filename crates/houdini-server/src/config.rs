use std::net::SocketAddr;
use std::path::Path;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    /// Address to bind. Typically the upstream HAProxy points at this address.
    pub listen: SocketAddr,
    /// Shared secret presented by the client in its `Hello` frame.
    pub auth_token: String,
    /// Cosmetic name advertised back to the client in `HelloAck`.
    #[serde(default = "default_server_name")]
    pub server_name: String,
    /// Path under which the WebSocket control endpoint is mounted. Every
    /// other request is treated as public traffic and reverse-proxied
    /// through the active tunnel.
    #[serde(default = "default_control_path")]
    pub control_path: String,
}

fn default_server_name() -> String {
    "houdini".to_owned()
}

fn default_control_path() -> String {
    "/_houdini/v1/control".to_owned()
}

impl ServerConfig {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("read {}: {e}", path.display()))?;
        let cfg: Self = toml::from_str(&raw)?;
        if cfg.auth_token.trim().is_empty() {
            anyhow::bail!("auth_token must not be empty");
        }
        if !cfg.control_path.starts_with('/') {
            anyhow::bail!("control_path must start with '/'");
        }
        Ok(cfg)
    }
}

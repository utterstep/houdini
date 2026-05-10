use std::net::SocketAddr;
use std::path::Path;

use derive_getters::Getters;
use eyre::{Result, WrapErr, eyre};
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;

#[derive(Debug, Deserialize, Getters)]
pub(crate) struct ServerConfig {
    listen: SocketAddr,
    auth_token: SecretString,
    #[serde(default = "default_server_name")]
    server_name: String,
    #[serde(default = "default_control_path")]
    control_path: String,
}

fn default_server_name() -> String {
    "houdini".to_owned()
}

fn default_control_path() -> String {
    "/_houdini/v1/control".to_owned()
}

impl ServerConfig {
    pub(crate) fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .wrap_err_with(|| format!("Failed to read server config from '{}'", path.display()))?;
        let cfg: Self = toml::from_str(&raw)
            .wrap_err_with(|| format!("Failed to parse server config at '{}'", path.display()))?;

        if cfg.auth_token.expose_secret().trim().is_empty() {
            return Err(eyre!("auth_token must not be empty"));
        }
        if !cfg.control_path.starts_with('/') {
            return Err(eyre!(
                "control_path must start with '/' (got '{}')",
                cfg.control_path
            ));
        }
        Ok(cfg)
    }
}

use std::sync::Arc;

use houdini_protocol::MuxOpener;
use tokio::sync::RwLock;

use crate::config::ServerConfig;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<ServerConfig>,
    pub active: Arc<RwLock<Option<ActiveTunnel>>>,
}

pub struct ActiveTunnel {
    pub opener: MuxOpener,
    /// Reported by the client in its `Hello` frame; surfaced via the admin
    /// status endpoint and in disconnect logs.
    #[allow(dead_code)]
    pub client_name: Option<String>,
}

impl AppState {
    pub fn new(config: ServerConfig) -> Self {
        Self {
            config: Arc::new(config),
            active: Arc::new(RwLock::new(None)),
        }
    }
}

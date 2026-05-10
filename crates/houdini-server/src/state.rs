use std::ops::Deref;
use std::sync::Arc;

use derive_getters::Getters;
use tokio::sync::RwLock;

use houdini_protocol::MuxOpener;

use crate::config::ServerConfig;

#[derive(Clone)]
pub(crate) struct AppState(Arc<AppStateInner>);

impl Deref for AppState {
    type Target = AppStateInner;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Getters)]
pub(crate) struct AppStateInner {
    config: ServerConfig,
    active: RwLock<Option<ActiveTunnel>>,
}

impl AppState {
    pub(crate) fn new(config: ServerConfig) -> Self {
        Self(Arc::new(AppStateInner {
            config,
            active: RwLock::new(None),
        }))
    }
}

#[derive(Getters)]
pub(crate) struct ActiveTunnel {
    opener: MuxOpener,
    /// Reported by the client in its `Hello` frame; surfaced in disconnect
    /// logs and the (future) status endpoint.
    #[allow(dead_code)]
    client_name: Option<String>,
}

impl ActiveTunnel {
    pub(crate) fn new(opener: MuxOpener, client_name: Option<String>) -> Self {
        Self {
            opener,
            client_name,
        }
    }
}

//! Process-wide state shared by the control plane and the ingress.

use std::sync::Arc;

use norupo_core::auth::AuthProvider;
use norupo_core::registry::SharedRegistry;

use crate::config::ServerConfig;
use crate::session::SessionManager;

/// Everything a request handler needs, cheap to clone.
pub struct AppState {
    pub config: ServerConfig,
    /// Stable identity of this edge process.
    pub node_id: String,
    /// Address siblings dial to reach this node.
    pub node_addr: String,
    pub registry: SharedRegistry,
    pub auth: Arc<dyn AuthProvider>,
    pub sessions: Arc<SessionManager>,
}

impl AppState {
    #[must_use]
    pub fn version(&self) -> &'static str {
        env!("CARGO_PKG_VERSION")
    }
}

/// Convenient alias; the state is always behind an `Arc`.
pub type Shared = Arc<AppState>;

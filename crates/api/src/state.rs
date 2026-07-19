//! Cloneable dependencies shared by the public HTTP handlers.

use std::sync::Arc;

use run_anywhere_repository::Repository;
use tokio::sync::watch;

use crate::{
    config::Config, debug_token::DebugTokenIssuer, object_store::ObjectStore,
    observability::ApiMetrics, webhook::WebhookDispatcher,
};

#[derive(Clone)]
pub struct AppState {
    pub repository: Repository,
    pub object_store: Arc<dyn ObjectStore>,
    pub debug_tokens: DebugTokenIssuer,
    pub webhook_dispatcher: WebhookDispatcher,
    pub metrics: ApiMetrics,
    pub config: Arc<Config>,
    pub shutdown: watch::Receiver<bool>,
}

impl std::fmt::Debug for AppState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AppState")
            .field("repository", &self.repository)
            .field("debug_tokens", &self.debug_tokens)
            .field("webhook_dispatcher", &self.webhook_dispatcher)
            .field("metrics", &self.metrics)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

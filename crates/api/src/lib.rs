//! Run Android Anywhere control-plane API.

#![forbid(unsafe_code)]

mod app;

pub mod artifacts;
pub mod auth;
pub mod config;
pub mod debug_sessions;
pub mod debug_token;
pub mod error;
pub mod events;
pub mod extract;
pub mod jobs;
pub mod object_store;
pub mod observability;
pub mod params;
pub mod projects;
pub mod queue;
pub mod request_context;
pub mod router;
pub mod runtime_profiles;
mod service_error;
pub mod state;
pub mod uploads;
pub mod webhook;
pub mod webhooks;
pub mod workers;

pub use app::run;
pub use config::{Config, ConfigError, S3Config, SecretString};
pub use error::{ApiError, ApiResult};
pub use observability::{ApiMetrics, ObservabilityError, TelemetryGuard};
pub use router::public_router;
pub use state::AppState;

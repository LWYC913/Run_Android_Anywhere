//! Authenticated and TLS-aware NATS connection construction for the API.

use std::fs;

use async_nats::{Client, ConnectOptions};
use thiserror::Error;

use crate::config::NatsConfig;

/// The API identity's request/reply inbox namespace.
///
/// NATS server permissions should grant this API identity only
/// `_INBOX.api.>` subscriptions in addition to its `jobs.queued` publish
/// permission. Keeping this separate from scheduler and worker inboxes avoids
/// cross-principal reply consumption.
pub const API_NATS_INBOX_PREFIX: &str = "_INBOX.api";

#[derive(Debug, Error)]
pub enum NatsConnectionError {
    #[error("could not read the configured API NATS NKey seed file: {0}")]
    ReadNkey(#[source] std::io::Error),
    #[error("could not read the configured API NATS credentials file: {0}")]
    ReadCredentials(#[source] std::io::Error),
    #[error("could not connect the API producer to NATS: {0}")]
    Connect(#[source] async_nats::ConnectError),
}

/// Connect the API producer with its own client name and scoped inbox prefix.
///
/// Secret material is read only while constructing `ConnectOptions` and is
/// never included in the connector's diagnostics.
pub async fn connect_nats(config: &NatsConfig) -> Result<Client, NatsConnectionError> {
    let url = config.url.expose_secret();
    let mut options = ConnectOptions::new()
        .name("run-anywhere-api")
        .custom_inbox_prefix(API_NATS_INBOX_PREFIX);

    if url.starts_with("tls://") || url.starts_with("wss://") {
        options = options.require_tls(true);
    }
    if let Some(ca_file) = &config.tls_ca_file {
        options = options.add_root_certificates(ca_file.clone());
    }
    if let (Some(cert_file), Some(key_file)) =
        (&config.tls_client_cert_file, &config.tls_client_key_file)
    {
        options = options.add_client_certificate(cert_file.clone(), key_file.clone());
    }
    if let Some(credentials_file) = &config.credentials_file {
        options = options
            .credentials_file(credentials_file)
            .await
            .map_err(NatsConnectionError::ReadCredentials)?;
    } else if let Some(seed_file) = &config.nkey_seed_file {
        let seed = fs::read_to_string(seed_file)
            .map_err(NatsConnectionError::ReadNkey)?
            .trim()
            .to_owned();
        options = options.nkey(seed);
    }

    options
        .connect(url)
        .await
        .map_err(NatsConnectionError::Connect)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_inbox_prefix_is_scoped_away_from_other_principals() {
        assert_eq!(API_NATS_INBOX_PREFIX, "_INBOX.api");
        assert!(!API_NATS_INBOX_PREFIX.contains("scheduler"));
        assert!(!API_NATS_INBOX_PREFIX.contains("worker"));
    }
}

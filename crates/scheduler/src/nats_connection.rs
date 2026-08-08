//! Authenticated and TLS-aware NATS connection construction.

use std::fs;

use async_nats::{Client, ConnectOptions};
use thiserror::Error;

use crate::{config::NatsConfig, security::SCHEDULER_INBOX_PREFIX};

#[derive(Debug, Error)]
pub enum NatsConnectionError {
    #[error("could not read the configured NATS NKey seed file: {0}")]
    ReadNkey(#[source] std::io::Error),
    #[error("could not read the configured NATS credentials file: {0}")]
    ReadCredentials(#[source] std::io::Error),
    #[error("could not connect the scheduler to NATS: {0}")]
    Connect(#[source] async_nats::ConnectError),
}

pub async fn connect_nats(config: &NatsConfig) -> Result<Client, NatsConnectionError> {
    let url = config.url.expose_secret();
    let mut options = ConnectOptions::new()
        .name("run-anywhere-scheduler")
        .custom_inbox_prefix(SCHEDULER_INBOX_PREFIX);

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

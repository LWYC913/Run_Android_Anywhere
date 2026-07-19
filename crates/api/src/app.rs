use std::{error::Error, sync::Arc, time::Duration};

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use run_anywhere_repository::Repository;
use tokio::{net::TcpListener, sync::watch};
use uuid::Uuid;

use crate::{
    AppState, Config,
    debug_token::DebugTokenIssuer,
    object_store::S3ObjectStore,
    observability::{ApiMetrics, init_tracing, metrics_router},
    queue::{JetStreamPublisher, OutboxDispatcher, OutboxDispatcherConfig},
    router::public_router,
    webhook::{WebhookDispatcher, WebhookPolicy},
};

type BoxError = Box<dyn Error + Send + Sync>;

const UPLOAD_URL_TTL: Duration = Duration::from_secs(15 * 60);
const DOWNLOAD_URL_TTL: Duration = Duration::from_secs(15 * 60);
const WEBHOOK_MAX_CONCURRENCY: usize = 8;
const WEBHOOK_TIMEOUT: Duration = Duration::from_secs(3);
const NATS_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const NATS_RETRY_MIN: Duration = Duration::from_secs(1);
const NATS_RETRY_MAX: Duration = Duration::from_secs(30);

pub async fn run() -> Result<(), BoxError> {
    let config = Arc::new(Config::from_env()?);
    let _telemetry = init_tracing("run-anywhere-api", config.otel_endpoint.as_ref())?;

    let repository = Repository::connect(config.database_url.expose_secret()).await?;
    if config.run_migrations {
        repository.migrate().await?;
    }

    let metrics = ApiMetrics::default();
    let s3_client = build_s3_client(&config);
    let object_store = Arc::new(S3ObjectStore::new(
        s3_client,
        config.s3.bucket.clone(),
        UPLOAD_URL_TTL,
        DOWNLOAD_URL_TTL,
    )?);
    let debug_tokens = DebugTokenIssuer::from_ed25519_pkcs8_pem(
        config.jwt_signing_key.expose_secret().as_bytes(),
        config.jwt_kid.clone(),
    )?;
    let webhook_dispatcher = WebhookDispatcher::new(
        WEBHOOK_MAX_CONCURRENCY,
        WEBHOOK_TIMEOUT,
        WebhookPolicy {
            allow_private_networks: config.webhook_allow_private_networks,
        },
        metrics.clone(),
    )?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let outbox_repository = repository.clone();
    let outbox_metrics = metrics.clone();
    let nats_url = config.nats_url.expose_secret().to_owned();
    let outbox_task = tokio::spawn(async move {
        run_outbox_supervisor(outbox_repository, nats_url, outbox_metrics, shutdown_rx).await;
    });

    let state = AppState {
        repository,
        object_store,
        debug_tokens,
        webhook_dispatcher,
        metrics: metrics.clone(),
        config: config.clone(),
        shutdown: shutdown_tx.subscribe(),
    };
    let webhook_task = tokio::spawn(crate::webhooks::run_outbox_dispatcher(
        state.clone(),
        shutdown_tx.subscribe(),
    ));
    let api_listener = TcpListener::bind(config.api_bind_addr).await?;
    let metrics_listener = TcpListener::bind(config.metrics_bind_addr).await?;
    tracing::info!(address = %config.api_bind_addr, "public API listening");
    tracing::info!(address = %config.metrics_bind_addr, "metrics endpoint listening");

    let api_server = axum::serve(api_listener, public_router(state))
        .with_graceful_shutdown(wait_for_shutdown(shutdown_tx.subscribe()));
    let metrics_server = axum::serve(metrics_listener, metrics_router(metrics))
        .with_graceful_shutdown(wait_for_shutdown(shutdown_tx.subscribe()));
    let signal_tx = shutdown_tx.clone();
    let servers = tokio::try_join!(
        async { api_server.await.map_err(boxed_error) },
        async { metrics_server.await.map_err(boxed_error) },
        async move {
            shutdown_signal().await?;
            let _ = signal_tx.send(true);
            Ok::<(), BoxError>(())
        }
    );

    let _ = shutdown_tx.send(true);
    outbox_task.await?;
    webhook_task.await?;
    servers?;
    Ok(())
}

fn build_s3_client(config: &Config) -> aws_sdk_s3::Client {
    let credentials = Credentials::new(
        config.s3.access_key.expose_secret(),
        config.s3.secret_key.expose_secret(),
        None,
        None,
        "run-anywhere-config",
    );
    let sdk_config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url(&config.s3.endpoint)
        .region(Region::new(config.s3.region.clone()))
        .credentials_provider(credentials)
        .force_path_style(config.s3.force_path_style)
        .build();
    aws_sdk_s3::Client::from_conf(sdk_config)
}

async fn shutdown_signal() -> Result<(), BoxError> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    tracing::info!("shutdown signal received");
    Ok(())
}

async fn run_outbox_supervisor(
    repository: Repository,
    nats_url: String,
    metrics: ApiMetrics,
    mut shutdown: watch::Receiver<bool>,
) {
    let dispatcher_id = format!("api-{}", Uuid::new_v4().simple());
    let mut retry_delay = NATS_RETRY_MIN;
    loop {
        if *shutdown.borrow() {
            return;
        }
        let connect = tokio::time::timeout(NATS_CONNECT_TIMEOUT, async_nats::connect(&nats_url));
        let connection = tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
                continue;
            }
            result = connect => result,
        };
        match connection {
            Ok(Ok(client)) => {
                tracing::info!("connected to NATS; starting transactional outbox dispatcher");
                let publisher =
                    Arc::new(JetStreamPublisher::new(async_nats::jetstream::new(client)));
                let dispatcher = match OutboxDispatcher::new(
                    repository.clone(),
                    publisher,
                    OutboxDispatcherConfig::new(dispatcher_id.clone()),
                ) {
                    Ok(dispatcher) => dispatcher.with_metrics(metrics.clone()),
                    Err(error) => {
                        tracing::error!(error = %error, "outbox dispatcher configuration is invalid");
                        return;
                    }
                };
                dispatcher.run(shutdown).await;
                return;
            }
            Ok(Err(error)) => {
                tracing::warn!(error = %error, retry_delay_ms = retry_delay.as_millis(), "NATS is unavailable; jobs remain durable in the outbox");
            }
            Err(_) => {
                tracing::warn!(
                    retry_delay_ms = retry_delay.as_millis(),
                    "NATS connection attempt timed out; jobs remain durable in the outbox"
                );
            }
        }
        if let Ok(backlog) = repository.pending_outbox_count().await {
            metrics.set_outbox_backlog(i64::try_from(backlog).unwrap_or(i64::MAX));
        }
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
            () = tokio::time::sleep(retry_delay) => {}
        }
        retry_delay = retry_delay.saturating_mul(2).min(NATS_RETRY_MAX);
    }
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    while !*shutdown.borrow() {
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

fn boxed_error(error: std::io::Error) -> BoxError {
    Box::new(error)
}

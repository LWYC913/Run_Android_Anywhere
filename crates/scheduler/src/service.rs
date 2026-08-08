//! Standalone active/passive scheduler service composition.

use std::{error::Error, sync::Arc, time::Duration};

use async_nats::jetstream;
use run_anywhere_repository::Repository;
use tokio::{net::TcpListener, sync::watch, task::JoinSet};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

use crate::{
    AckRegistry, AdvisoryRecovery, Config, ControlPlane, Dispatcher, DlqPublisher,
    DockerEngineReaper, LeaderGuard, ProvisionedTopology, Reconciler, RuntimeReaper,
    SchedulerMetrics, TopologyConfig, connect_nats, metrics_router, provision_topology,
};

type BoxError = Box<dyn Error + Send + Sync>;

const STANDBY_RETRY: Duration = Duration::from_secs(2);
const LEADER_HEALTH_INTERVAL: Duration = Duration::from_secs(1);
const CONTROL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
struct LeaderContext {
    config: Arc<Config>,
    repository: Repository,
    nats: async_nats::Client,
    jetstream: jetstream::Context,
    topology: ProvisionedTopology,
    metrics: SchedulerMetrics,
    reaper: Arc<dyn RuntimeReaper>,
}

/// Run the scheduler until an operating-system shutdown signal is received.
pub async fn run() -> Result<(), BoxError> {
    init_tracing()?;
    let config = Arc::new(Config::from_env()?);
    let repository = Repository::connect(config.database_url.expose_secret()).await?;
    if config.run_migrations {
        repository.migrate().await?;
    }

    let nats = connect_nats(&config.nats).await?;
    let jetstream = jetstream::new(nats.clone());
    let topology_config = TopologyConfig::try_from(config.as_ref())?;
    let topology = provision_topology(&jetstream, &topology_config).await?;
    let metrics = SchedulerMetrics::default();
    let reaper: Arc<dyn RuntimeReaper> = Arc::new(DockerEngineReaper::new(
        &config.docker.endpoint,
        config.docker.request_timeout,
    )?);

    let metrics_listener = TcpListener::bind(config.metrics_bind_addr).await?;
    tracing::info!(address = %config.metrics_bind_addr, "scheduler metrics endpoint listening");
    tracing::info!("JetStream topology is compatible and ready");

    let context = LeaderContext {
        config,
        repository,
        nats,
        jetstream,
        topology,
        metrics: metrics.clone(),
        reaper,
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let metrics_server = axum::serve(metrics_listener, metrics_router(metrics))
        .with_graceful_shutdown(wait_for_shutdown(shutdown_rx.clone()));

    tokio::try_join!(
        async { metrics_server.await.map_err(boxed_io_error) },
        async {
            run_leadership_supervisor(context, shutdown_rx).await;
            Ok::<(), BoxError>(())
        },
        async move {
            shutdown_signal().await?;
            let _ = shutdown_tx.send(true);
            Ok::<(), BoxError>(())
        }
    )?;
    Ok(())
}

async fn run_leadership_supervisor(context: LeaderContext, mut shutdown: watch::Receiver<bool>) {
    while !*shutdown.borrow() {
        match LeaderGuard::try_acquire(context.config.database_url.expose_secret()).await {
            Ok(Some(guard)) => {
                tracing::info!("scheduler leadership acquired");
                if let Err(error) = run_leader_cycle(&context, guard, shutdown.clone()).await {
                    tracing::warn!(error = %error, "leader cycle stopped and will be retried");
                }
            }
            Ok(None) => tracing::debug!("scheduler is passive; another instance is leader"),
            Err(error) => {
                tracing::warn!(error = %error, "could not inspect scheduler leadership");
            }
        }

        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
            () = tokio::time::sleep(STANDBY_RETRY) => {}
        }
    }
}

async fn run_leader_cycle(
    context: &LeaderContext,
    mut guard: LeaderGuard,
    shutdown: watch::Receiver<bool>,
) -> Result<(), BoxError> {
    let acknowledgements = AckRegistry::default();
    let dlq = DlqPublisher::new(
        context.repository.clone(),
        context.jetstream.clone(),
        format!("scheduler-{}", Uuid::new_v4().simple()),
    );
    let control = ControlPlane::new(
        context.nats.clone(),
        context.repository.clone(),
        context.config.as_ref().clone(),
        acknowledgements.clone(),
    )
    .start()
    .await?;

    let dispatcher = Dispatcher::new(
        context.nats.clone(),
        context.repository.clone(),
        context.topology.job_consumer.clone(),
        context.topology.job_stream.clone(),
        acknowledgements.clone(),
        dlq.clone(),
        context.config.as_ref().clone(),
        context.metrics.clone(),
    );
    let advisory = AdvisoryRecovery::new(
        context.jetstream.clone(),
        context.repository.clone(),
        context.topology.advisory_consumer.clone(),
        context.topology.job_stream.clone(),
        dlq.clone(),
        context.config.max_run_attempts,
        context.config.topology.pull_batch_size,
        context.metrics.clone(),
    );
    let reconciler = Reconciler::new(
        context.repository.clone(),
        acknowledgements,
        dlq,
        context.topology.job_stream.clone(),
        context.reaper.clone(),
        context.config.timing.clone(),
        context.config.max_run_attempts,
        context.metrics.clone(),
    );

    let (leader_shutdown_tx, leader_shutdown_rx) = watch::channel(false);
    let mut tasks = JoinSet::new();
    let dispatcher_shutdown = leader_shutdown_rx.clone();
    tasks.spawn(async move {
        dispatcher
            .run(dispatcher_shutdown)
            .await
            .map_err(|error| error.to_string())
    });
    tasks.spawn(async move {
        advisory
            .run(leader_shutdown_rx)
            .await
            .map_err(|error| error.to_string())
    });
    tasks.spawn(run_reconciler_loop(
        reconciler,
        context.config.timing.reconciliation_interval,
        leader_shutdown_tx.subscribe(),
    ));

    let mut health_tick = tokio::time::interval(LEADER_HEALTH_INTERVAL);
    health_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let stop_reason = loop {
        tokio::select! {
            biased;
            () = wait_for_shutdown(shutdown.clone()) => break "service shutdown".to_owned(),
            task = tasks.join_next() => {
                break match task {
                    Some(Ok(Ok(()))) => "leader task exited unexpectedly".to_owned(),
                    Some(Ok(Err(error))) => format!("leader task failed: {error}"),
                    Some(Err(error)) => format!("leader task panicked or was cancelled: {error}"),
                    None => "all leader tasks exited unexpectedly".to_owned(),
                };
            }
            _ = health_tick.tick() => {
                match guard.is_held().await {
                    Ok(true) if !control.is_finished() => {}
                    Ok(true) => break "worker control plane exited unexpectedly".to_owned(),
                    Ok(false) => break "PostgreSQL advisory lock was lost".to_owned(),
                    Err(error) => break format!("leadership session failed: {error}"),
                }
            }
        }
    };

    let _ = leader_shutdown_tx.send(true);
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    match tokio::time::timeout(CONTROL_SHUTDOWN_TIMEOUT, control.shutdown()).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::warn!(error = %error, "control plane shutdown failed"),
        Err(_) => tracing::warn!("control plane shutdown timed out"),
    }
    if let Err(error) = guard.release().await {
        tracing::debug!(error = %error, "leadership session was already unavailable");
    }
    tracing::info!(reason = %stop_reason, "scheduler leadership stopped");
    Ok(())
}

async fn run_reconciler_loop(
    reconciler: Reconciler,
    interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), String> {
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                let _ = changed;
                return Ok(());
            }
            _ = tick.tick() => {
                match reconciler.run_once().await {
                    Ok(report) => tracing::debug!(
                        requeued = report.requeued,
                        exhausted = report.exhausted,
                        finalized = report.finalized,
                        debug_sessions_ended = report.debug_sessions_ended,
                        runtimes_reaped = report.runtimes_reaped,
                        "reconciliation pass completed"
                    ),
                    Err(error) => tracing::warn!(error = %error, "reconciliation pass failed"),
                }
            }
        }
    }
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    while !*shutdown.borrow() {
        if shutdown.changed().await.is_err() {
            return;
        }
    }
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
    tracing::info!("scheduler shutdown signal received");
    Ok(())
}

fn init_tracing() -> Result<(), BoxError> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("run_anywhere_scheduler=info"));
    tracing_subscriber::fmt()
        .json()
        .flatten_event(true)
        .with_env_filter(filter)
        .try_init()?;
    Ok(())
}

fn boxed_io_error(error: std::io::Error) -> BoxError {
    Box::new(error)
}

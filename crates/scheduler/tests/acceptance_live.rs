mod common;

use std::{env, fs, sync::Arc, time::Duration};

use async_nats::{Client, ConnectOptions, jetstream};
use futures_util::StreamExt as _;
use run_anywhere_contracts::{
    ArtifactSelection, AutomationSpec, ControlResponse, CreateJobRequest, DurationSeconds,
    HostArch, IsolationTier, JobDeadLetter, JobDispatch, JobId, JobLeaseExtension, JobMode,
    JobOutcome, JobResult, JobState, JobStateTransitionRequest, RuntimeKind, RuntimeProfileId,
    Sha256, TransitionEvidence, UploadKind, WorkerHeartbeat, WorkerId, WorkerRegistration,
};
use run_anywhere_repository::{CreatedJob, Repository};
use run_anywhere_scheduler::{
    AckRegistry, Config, ControlPlane, ControlPlaneHandle, Dispatcher, DispatcherError,
    DlqPublisher, NoopRuntimeReaper, ProvisionedTopology, Reconciler, SchedulerMetrics,
    TopologyConfig, connect_nats, provision_topology,
    subjects::{
        JOBS_DEAD_SUBJECT, JOBS_QUEUED_SUBJECT, job_result_subject, job_transition_subject,
        worker_dispatch_subject, worker_heartbeat_subject, worker_registration_subject,
    },
    worker_inbox_prefix,
};
use serde::Serialize;
use sqlx::PgPool;
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
    time::{Instant, sleep, timeout},
};
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const EMULATOR_PROFILE: &str = "rtp_android_35_pixel_6_x86_64_emulator";
const CONTROL_TIMEOUT: Duration = Duration::from_secs(5);

fn integration_enabled() -> bool {
    env::var("RUN_SCHEDULER_INTEGRATION").is_ok_and(|value| value.eq_ignore_ascii_case("true"))
}

fn require(condition: bool, message: impl Into<String>) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message.into()).into())
    }
}

#[derive(Clone)]
struct ProjectFixture {
    repository: Repository,
    project_id: run_anywhere_contracts::ProjectId,
    apk_upload_id: run_anywhere_contracts::UploadId,
}

impl ProjectFixture {
    async fn create(repository: Repository, suffix: &str) -> TestResult<Self> {
        let project = repository
            .create_project(
                format!("scheduler acceptance {suffix}"),
                "part-04-live-test",
            )
            .await?;
        let upload = repository
            .create_upload(
                &project.id,
                UploadKind::Apk,
                format!("acceptance/{suffix}/app.apk"),
                Sha256::new("a".repeat(64))?,
                4_096,
            )
            .await?;
        Ok(Self {
            repository,
            project_id: project.id,
            apk_upload_id: upload.id,
        })
    }

    async fn create_job(
        &self,
        idempotency_key: &str,
        profile: &str,
        isolation: IsolationTier,
    ) -> TestResult<CreatedJob> {
        Ok(self
            .repository
            .create_job(
                CreateJobRequest {
                    project_id: self.project_id.clone(),
                    apk_upload_id: self.apk_upload_id.clone(),
                    test_upload_id: None,
                    runtime_profile: RuntimeProfileId::new(profile)?,
                    mode: JobMode::HeadlessCi,
                    min_isolation: isolation,
                    automation: AutomationSpec::BuiltInSmoke,
                    artifacts: ArtifactSelection {
                        screenshots: true,
                        video: false,
                        logcat: true,
                        junit: true,
                    },
                    timeout_seconds: DurationSeconds::new(300)?,
                },
                idempotency_key,
            )
            .await?)
    }
}

struct FakeWorker {
    worker_id: WorkerId,
    client: Client,
    dispatches: mpsc::UnboundedReceiver<JobDispatch>,
    responder: JoinHandle<()>,
}

impl FakeWorker {
    async fn connect(config: &Config, worker_id: WorkerId, role: &str) -> TestResult<Self> {
        let client = connect_worker(config, &worker_id, role).await?;
        let mut subscriber = client
            .subscribe(worker_dispatch_subject(&worker_id))
            .await?;
        client.flush().await?;
        let responder_client = client.clone();
        let (dispatch_tx, dispatches) = mpsc::unbounded_channel();
        let responder = tokio::spawn(async move {
            while let Some(message) = subscriber.next().await {
                let Ok(dispatch) = serde_json::from_slice::<JobDispatch>(&message.payload) else {
                    continue;
                };
                let Some(reply) = message.reply else {
                    continue;
                };
                let Ok(payload) = serde_json::to_vec(&ControlResponse::Accepted) else {
                    continue;
                };
                if responder_client
                    .publish(reply, payload.into())
                    .await
                    .is_ok()
                {
                    let _ = dispatch_tx.send(dispatch);
                }
            }
        });
        Ok(Self {
            worker_id,
            client,
            dispatches,
            responder,
        })
    }

    async fn next_dispatch(&mut self, wait: Duration) -> TestResult<JobDispatch> {
        timeout(wait, self.dispatches.recv())
            .await
            .map_err(|_| std::io::Error::other("worker dispatch timed out"))?
            .ok_or_else(|| std::io::Error::other("worker dispatch channel closed").into())
    }

    fn stop(self) {
        self.responder.abort();
    }
}

struct SchedulerRuntime {
    shutdown: watch::Sender<bool>,
    dispatcher: JoinHandle<Result<(), DispatcherError>>,
    control: ControlPlaneHandle,
    acknowledgements: AckRegistry,
    metrics: SchedulerMetrics,
}

impl SchedulerRuntime {
    async fn start(
        client: &Client,
        repository: &Repository,
        topology: &ProvisionedTopology,
        jetstream: &jetstream::Context,
        config: &Config,
    ) -> TestResult<Self> {
        let acknowledgements = AckRegistry::default();
        let dlq = DlqPublisher::new(
            repository.clone(),
            jetstream.clone(),
            format!("scheduler-acceptance-{}", Uuid::new_v4().simple()),
        );
        let metrics = SchedulerMetrics::default();
        let control = ControlPlane::new(
            client.clone(),
            repository.clone(),
            config.clone(),
            acknowledgements.clone(),
        )
        .start()
        .await?;
        let dispatcher = Dispatcher::new(
            client.clone(),
            repository.clone(),
            topology.job_consumer.clone(),
            topology.job_stream.clone(),
            acknowledgements.clone(),
            dlq,
            config.clone(),
            metrics.clone(),
        );
        let (shutdown, receiver) = watch::channel(false);
        let dispatcher = tokio::spawn(dispatcher.run(receiver));
        Ok(Self {
            shutdown,
            dispatcher,
            control,
            acknowledgements,
            metrics,
        })
    }

    async fn stop(self) -> TestResult {
        let _ = self.shutdown.send(true);
        let dispatcher = timeout(CONTROL_TIMEOUT, self.dispatcher)
            .await
            .map_err(|_| std::io::Error::other("dispatcher shutdown timed out"))??;
        dispatcher?;
        self.control.shutdown().await?;
        Ok(())
    }
}

async fn connect_worker(config: &Config, worker_id: &WorkerId, role: &str) -> TestResult<Client> {
    let url = config.nats.url.expose_secret();
    let mut options = ConnectOptions::new()
        .name(format!("run-anywhere-acceptance-{role}"))
        .custom_inbox_prefix(worker_inbox_prefix(worker_id));
    if url.starts_with("tls://") || url.starts_with("wss://") {
        options = options.require_tls(true);
    }
    if let Some(ca_file) = &config.nats.tls_ca_file {
        options = options.add_root_certificates(ca_file.clone());
    }
    if let (Some(cert_file), Some(key_file)) = (
        &config.nats.tls_client_cert_file,
        &config.nats.tls_client_key_file,
    ) {
        options = options.add_client_certificate(cert_file.clone(), key_file.clone());
    }

    let env_prefix = format!("SCHEDULER_TEST_WORKER_{}_NATS_", role.to_ascii_uppercase());
    let credentials_file = env::var(format!("{env_prefix}CREDENTIALS_FILE"))
        .ok()
        .map(Into::into)
        .or_else(|| config.nats.credentials_file.clone());
    let nkey_seed_file = env::var(format!("{env_prefix}NKEY_SEED_FILE"))
        .ok()
        .map(Into::into)
        .or_else(|| config.nats.nkey_seed_file.clone());
    let username = env::var(format!("{env_prefix}USER")).ok();
    let password = env::var(format!("{env_prefix}PASSWORD")).ok();
    if username.is_some() != password.is_some() {
        return Err(std::io::Error::other(format!(
            "{env_prefix}USER and {env_prefix}PASSWORD must be set together"
        ))
        .into());
    }
    if let (Some(username), Some(password)) = (username, password) {
        options = options.user_and_password(username, password);
    } else if let Some(credentials_file) = credentials_file {
        options = options.credentials_file(credentials_file).await?;
    } else if let Some(seed_file) = nkey_seed_file {
        options = options.nkey(fs::read_to_string(seed_file)?.trim().to_owned());
    }
    Ok(options.connect(url).await?)
}

async fn request<T: Serialize>(
    worker: &FakeWorker,
    subject: String,
    payload: &T,
) -> TestResult<ControlResponse> {
    let response = timeout(
        CONTROL_TIMEOUT,
        worker
            .client
            .request(subject, serde_json::to_vec(payload)?.into()),
    )
    .await
    .map_err(|_| std::io::Error::other("worker control request timed out"))??;
    Ok(serde_json::from_slice(&response.payload)?)
}

async fn register(worker: &FakeWorker) -> TestResult {
    let response = request(
        worker,
        worker_registration_subject(&worker.worker_id),
        &WorkerRegistration {
            worker_id: worker.worker_id.clone(),
            runtimes: vec![RuntimeKind::AndroidEmulatorContainer],
            kvm: true,
            gpu: false,
            arch: HostArch::X86_64,
            capacity: 2,
        },
    )
    .await?;
    require(
        response == ControlResponse::Accepted,
        format!("worker registration was not accepted: {response:?}"),
    )
}

async fn heartbeat(
    worker: &FakeWorker,
    extension: JobLeaseExtension,
    capacity: u32,
) -> TestResult<ControlResponse> {
    request(
        worker,
        worker_heartbeat_subject(&worker.worker_id),
        &WorkerHeartbeat {
            worker_id: worker.worker_id.clone(),
            active_jobs: 1,
            capacity,
            runtimes: vec![RuntimeKind::AndroidEmulatorContainer],
            kvm: true,
            gpu: false,
            arch: HostArch::X86_64,
            lease_extends: vec![extension],
            last_seen: chrono::Utc::now(),
        },
    )
    .await
}

async fn transition(
    worker: &FakeWorker,
    dispatch: &JobDispatch,
    from: JobState,
    to: JobState,
) -> TestResult {
    let response = request(
        worker,
        job_transition_subject(&worker.worker_id),
        &JobStateTransitionRequest {
            job_id: dispatch.claim.job_id.clone(),
            worker_id: worker.worker_id.clone(),
            lease_id: dispatch.claim.lease_id.clone(),
            from,
            to,
            evidence: TransitionEvidence::default(),
        },
    )
    .await?;
    require(
        response == ControlResponse::Accepted,
        format!("worker transition {from:?} -> {to:?} was rejected: {response:?}"),
    )
}

async fn advance_to_running_tests(worker: &FakeWorker, dispatch: &JobDispatch) -> TestResult {
    for (from, to) in [
        (JobState::Claimed, JobState::ProvisioningRuntime),
        (JobState::ProvisioningRuntime, JobState::Booting),
        (JobState::Booting, JobState::InstallingApk),
        (JobState::InstallingApk, JobState::RunningTests),
    ] {
        transition(worker, dispatch, from, to).await?;
    }
    Ok(())
}

async fn publish_job(
    context: &jetstream::Context,
    repository: &Repository,
    job_id: &JobId,
) -> TestResult<u64> {
    let queued = repository
        .get_job_scheduling_snapshot(job_id)
        .await?
        .ok_or_else(|| std::io::Error::other("created job disappeared before publication"))?
        .queued;
    let acknowledgement = context
        .publish(JOBS_QUEUED_SUBJECT, serde_json::to_vec(&queued)?.into())
        .await?
        .await?;
    Ok(acknowledgement.sequence)
}

async fn wait_for_binding(
    registry: &AckRegistry,
    dispatch: &JobDispatch,
    wait: Duration,
) -> TestResult {
    let deadline = Instant::now() + wait;
    loop {
        if registry
            .binding(
                &dispatch.claim.job_id,
                &dispatch.claim.worker_id,
                &dispatch.claim.lease_id,
            )
            .await
            .is_ok()
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(
                std::io::Error::other("redelivery did not rebind to the live lease").into(),
            );
        }
        sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_redelivery(
    mut consumer: jetstream::consumer::PullConsumer,
    baseline: usize,
    wait: Duration,
) -> TestResult<usize> {
    let deadline = Instant::now() + wait;
    loop {
        let redelivered = consumer.info().await?.num_redelivered;
        if redelivered > baseline {
            return Ok(redelivered);
        }
        if Instant::now() >= deadline {
            return Err(std::io::Error::other("JetStream did not redeliver after restart").into());
        }
        sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_source_removal(stream: &jetstream::stream::Stream, sequence: u64) -> TestResult {
    let deadline = Instant::now() + CONTROL_TIMEOUT;
    loop {
        if stream.get_raw_message(sequence).await.is_err() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(std::io::Error::other(
                "terminal database commit was not followed by source acknowledgement",
            )
            .into());
        }
        sleep(Duration::from_millis(100)).await;
    }
}

async fn assert_no_dlq_since(
    mut dlq_stream: jetstream::stream::Stream,
    first_sequence: u64,
    job_ids: &[JobId],
) -> TestResult {
    let last_sequence = dlq_stream.info().await?.state.last_sequence;
    for sequence in first_sequence..=last_sequence {
        let Ok(message) = dlq_stream.get_raw_message(sequence).await else {
            continue;
        };
        let Ok(dead_letter) = serde_json::from_slice::<JobDeadLetter>(&message.payload) else {
            continue;
        };
        require(
            dead_letter
                .job_id
                .as_ref()
                .is_none_or(|job_id| !job_ids.contains(job_id)),
            format!("blocked job unexpectedly entered JetStream DLQ: {dead_letter:?}"),
        )?;
    }
    Ok(())
}

async fn cleanup_database(
    pool: &PgPool,
    projects: &[run_anywhere_contracts::ProjectId],
    workers: &[WorkerId],
    jobs: &[JobId],
) -> TestResult {
    let job_ids = jobs.iter().map(ToString::to_string).collect::<Vec<_>>();
    let project_ids = projects.iter().map(ToString::to_string).collect::<Vec<_>>();
    let worker_ids = workers.iter().map(ToString::to_string).collect::<Vec<_>>();
    sqlx::query(
        "DELETE FROM outbox_messages WHERE event_key = ANY($1) \
         OR payload->>'job_id' = ANY($1)",
    )
    .bind(&job_ids)
    .execute(pool)
    .await?;
    sqlx::query("DELETE FROM projects WHERE id = ANY($1)")
        .bind(&project_ids)
        .execute(pool)
        .await?;
    sqlx::query("DELETE FROM workers WHERE id = ANY($1)")
        .bind(&worker_ids)
        .execute(pool)
        .await?;
    Ok(())
}

fn passed_result(dispatch: &JobDispatch) -> JobResult {
    JobResult {
        job_id: dispatch.claim.job_id.clone(),
        worker_id: dispatch.claim.worker_id.clone(),
        lease_id: dispatch.claim.lease_id.clone(),
        outcome: JobOutcome::Passed,
        artifact_ids: Vec::new(),
        artifacts_finalized: true,
        cleanup_completed: true,
        error: None,
        completed_at: chrono::Utc::now(),
    }
}

fn metric_value(metrics: &SchedulerMetrics, name: &str) -> Option<u64> {
    metrics.render().lines().find_map(|line| {
        let value = line.strip_prefix(name)?.strip_prefix(' ')?;
        value.parse().ok()
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_scheduler_restart_fencing_recovery_and_pending_work() -> TestResult {
    if !integration_enabled() {
        eprintln!("skipping live scheduler acceptance test; set RUN_SCHEDULER_INTEGRATION=true");
        return Ok(());
    }

    let base_config = Config::from_env()?;
    let _topology_lock =
        common::acquire_shared_nats_topology_lock(base_config.database_url.expose_secret()).await?;
    let restore_topology = TopologyConfig::try_from(&base_config)?;
    let mut config = base_config.clone();
    config.timing.worker_heartbeat = Duration::from_secs(1);
    config.timing.stale_worker_threshold = Duration::from_secs(2);
    config.timing.ack_wait = Duration::from_secs(3);
    // Leave enough margin for a loaded CI runner to observe the three-second
    // JetStream redelivery and run the quota/capacity assertions while the
    // exact database lease remains valid. Production defaults to 75 seconds.
    config.timing.database_lease_ttl = Duration::from_secs(30);
    config.timing.reconciliation_interval = Duration::from_secs(1);
    config.timing.reap_grace = Duration::from_secs(1);
    config.topology.pull_batch_size = config.topology.pull_batch_size.min(8);
    config.fairness.buffer_size = config.fairness.buffer_size.max(8);

    let repository = Repository::connect(config.database_url.expose_secret()).await?;
    repository.migrate().await?;
    let scheduler_client = connect_nats(&config.nats).await?;
    let context = jetstream::new(scheduler_client.clone());
    // The durable names are intentionally the production names, so this
    // destructive crash/restart probe must run against a dedicated empty
    // integration JetStream rather than a shared developer queue.
    let baseline_topology = provision_topology(&context, &restore_topology).await?;
    let mut baseline_job_stream = baseline_topology.job_stream.clone();
    require(
        baseline_job_stream.info().await?.state.messages == 0,
        "live scheduler acceptance test requires an empty dedicated JOB_QUEUE stream",
    )?;
    let mut baseline_consumer = baseline_topology.job_consumer.clone();
    let baseline_consumer_info = baseline_consumer.info().await?;
    require(
        baseline_consumer_info.num_pending == 0
            && baseline_consumer_info.num_ack_pending == 0
            && baseline_consumer_info.num_waiting == 0,
        "live scheduler acceptance test requires a dedicated idle job-dispatch-v1 consumer",
    )?;
    let topology_config = TopologyConfig::try_from(&config)?;
    let topology = provision_topology(&context, &topology_config).await?;

    let suffix = Uuid::new_v4().simple().to_string();
    let main_fixture =
        ProjectFixture::create(repository.clone(), &format!("main-{suffix}")).await?;
    sqlx::query("UPDATE projects SET max_concurrent_jobs = 1 WHERE id = $1")
        .bind(main_fixture.project_id.as_str())
        .execute(repository.pool())
        .await?;
    let capacity_fixture =
        ProjectFixture::create(repository.clone(), &format!("capacity-{suffix}")).await?;
    let main_job = main_fixture
        .create_job(
            &format!("main-{suffix}"),
            EMULATOR_PROFILE,
            IsolationTier::VmIsolated,
        )
        .await?
        .job;
    let quota_job = main_fixture
        .create_job(
            &format!("quota-{suffix}"),
            EMULATOR_PROFILE,
            IsolationTier::VmIsolated,
        )
        .await?
        .job;
    let capacity_job = capacity_fixture
        .create_job(
            &format!("capacity-{suffix}"),
            EMULATOR_PROFILE,
            IsolationTier::VmIsolated,
        )
        .await?
        .job;

    let worker_a_id = WorkerId::new(format!("wrk_accept_a_{suffix}"))?;
    let worker_b_id = WorkerId::new(format!("wrk_accept_b_{suffix}"))?;
    let mut worker_a = FakeWorker::connect(&config, worker_a_id.clone(), "a").await?;
    let mut worker_b = FakeWorker::connect(&config, worker_b_id.clone(), "b").await?;
    let mut runtime = Some(
        SchedulerRuntime::start(&scheduler_client, &repository, &topology, &context, &config)
            .await?,
    );
    let mut queue_sequences = Vec::new();
    let mut dlq_stream = topology.dlq_stream.clone();
    let dlq_baseline = dlq_stream.info().await?.state.last_sequence;
    let mut consumer_info = topology.job_consumer.clone();
    let redelivery_baseline = consumer_info.info().await?.num_redelivered;

    let exercise: TestResult = async {
        register(&worker_a).await?;
        let main_sequence = publish_job(&context, &repository, &main_job.id).await?;
        queue_sequences.push(main_sequence);
        let dispatch_a = worker_a.next_dispatch(CONTROL_TIMEOUT).await?;
        require(
            dispatch_a.claim.job_id == main_job.id,
            "worker A received a dispatch for the wrong job",
        )?;
        let first_snapshot = repository
            .get_job_scheduling_snapshot(&main_job.id)
            .await?
            .ok_or_else(|| std::io::Error::other("claimed job disappeared"))?;
        require(
            first_snapshot.delivery_attempts == 1,
            "initial dispatch did not increment the database attempt exactly once",
        )?;

        runtime
            .take()
            .ok_or_else(|| std::io::Error::other("scheduler runtime was absent"))?
            .stop()
            .await?;
        runtime = Some(
            SchedulerRuntime::start(
                &scheduler_client,
                &repository,
                &topology,
                &context,
                &config,
            )
            .await?,
        );
        let restarted = runtime
            .as_ref()
            .ok_or_else(|| std::io::Error::other("restarted scheduler was absent"))?;
        wait_for_binding(
            &restarted.acknowledgements,
            &dispatch_a,
            config.timing.ack_wait + Duration::from_secs(3),
        )
        .await?;
        let redelivered = wait_for_redelivery(
            topology.job_consumer.clone(),
            redelivery_baseline,
            Duration::from_secs(1),
        )
        .await?;
        let rebound = repository
            .get_job_scheduling_snapshot(&main_job.id)
            .await?
            .ok_or_else(|| std::io::Error::other("rebound job disappeared"))?;
        require(
            rebound.delivery_attempts == 1 && rebound.lease == first_snapshot.lease,
            "redelivery changed the execution attempt or lease ownership",
        )?;

        let extension_a = JobLeaseExtension {
            job_id: main_job.id.clone(),
            lease_id: dispatch_a.claim.lease_id.clone(),
        };
        let before_extension = rebound
            .lease_expires_at
            .ok_or_else(|| std::io::Error::other("claimed job had no lease expiry"))?;
        let response = heartbeat(&worker_a, extension_a.clone(), 2).await?;
        require(
            matches!(response, ControlResponse::Heartbeat { ref extended, .. } if extended == &vec![extension_a.clone()]),
            format!("healthy heartbeat did not extend the exact lease: {response:?}"),
        )?;
        sleep(Duration::from_secs(2)).await;
        let response = heartbeat(&worker_a, extension_a, 2).await?;
        require(
            matches!(response, ControlResponse::Heartbeat { ref extended, .. } if extended.len() == 1),
            format!("second healthy heartbeat was rejected: {response:?}"),
        )?;
        sleep(Duration::from_secs(2)).await;
        let after_extension = repository
            .get_job_scheduling_snapshot(&main_job.id)
            .await?
            .ok_or_else(|| std::io::Error::other("heartbeat job disappeared"))?;
        require(
            after_extension
                .lease_expires_at
                .is_some_and(|expires_at| expires_at > before_extension),
            "heartbeat did not move the PostgreSQL lease expiry forward",
        )?;
        let stable_redeliveries = consumer_info.info().await?.num_redelivered;
        require(
            stable_redeliveries == redelivered,
            "JetStream redelivered despite heartbeat Progress beyond AckWait",
        )?;

        register(&worker_b).await?;
        let dlq = DlqPublisher::new(
            repository.clone(),
            context.clone(),
            format!("scheduler-acceptance-reconcile-{suffix}"),
        );
        let reconciler = Reconciler::new(
            repository.clone(),
            restarted.acknowledgements.clone(),
            dlq,
            topology.job_stream.clone(),
            Arc::new(NoopRuntimeReaper),
            config.timing.clone(),
            config.max_run_attempts,
            SchedulerMetrics::default(),
        );
        let report = reconciler.run_once().await?;
        require(report.requeued >= 1, "reconciler did not recover stale worker A")?;
        let dispatch_b = worker_b.next_dispatch(CONTROL_TIMEOUT).await?;
        require(
            dispatch_b.claim.job_id == main_job.id,
            "recovered job did not reach worker B",
        )?;
        let claimed_by_b = repository
            .get_job_scheduling_snapshot(&main_job.id)
            .await?
            .ok_or_else(|| std::io::Error::other("worker B job disappeared"))?;
        require(
            claimed_by_b.delivery_attempts == 2
                && claimed_by_b
                    .lease
                    .as_ref()
                    .is_some_and(|lease| lease.worker_id == worker_b.worker_id),
            "worker B did not receive the sole second execution attempt",
        )?;
        let extension_b = JobLeaseExtension {
            job_id: main_job.id.clone(),
            lease_id: dispatch_b.claim.lease_id.clone(),
        };
        let response = heartbeat(&worker_b, extension_b.clone(), 2).await?;
        require(
            matches!(response, ControlResponse::Heartbeat { ref extended, .. } if extended == &vec![extension_b.clone()]),
            format!("worker B heartbeat was rejected: {response:?}"),
        )?;
        advance_to_running_tests(&worker_b, &dispatch_b).await?;
        let running = repository
            .get_job_scheduling_snapshot(&main_job.id)
            .await?
            .ok_or_else(|| std::io::Error::other("worker B job disappeared after transitions"))?;
        require(
            running.job.state == JobState::RunningTests
                && running.lease.as_ref().is_some_and(|lease| {
                    lease.worker_id == worker_b.worker_id
                        && lease.lease_id == dispatch_b.claim.lease_id
                }),
            "worker B did not reach running_tests with its exact lease",
        )?;

        let late_a = request(
            &worker_a,
            job_result_subject(&worker_a.worker_id),
            &passed_result(&dispatch_a),
        )
        .await?;
        require(
            late_a == ControlResponse::StaleLease,
            format!("late worker A result was not fenced: {late_a:?}"),
        )?;

        let quota_sequence = publish_job(&context, &repository, &quota_job.id).await?;
        queue_sequences.push(quota_sequence);
        sleep(Duration::from_millis(1_500)).await;
        let response = heartbeat(&worker_b, extension_b.clone(), 2).await?;
        require(
            matches!(response, ControlResponse::Heartbeat { ref extended, .. } if extended.len() == 1),
            format!("worker B lease refresh was rejected: {response:?}"),
        )?;
        sleep(Duration::from_secs(1)).await;
        let quota_snapshot = repository
            .get_job_scheduling_snapshot(&quota_job.id)
            .await?
            .ok_or_else(|| std::io::Error::other("quota-blocked job disappeared"))?;
        require(
            quota_snapshot.job.state == JobState::Queued
                && quota_snapshot.delivery_attempts == 0,
            "project concurrency block consumed an execution attempt",
        )?;
        require(
            metric_value(&restarted.metrics, "raa_scheduler_quota_blocked_total")
                .is_some_and(|blocked| blocked > 0),
            "dispatcher did not exercise the typed project concurrency quota path",
        )?;

        // Lowering capacity to the active reservation is legal and makes the
        // next compatible project observe no spare worker capacity.
        let response = heartbeat(&worker_b, extension_b.clone(), 1).await?;
        require(
            matches!(response, ControlResponse::Heartbeat { ref extended, .. } if extended.len() == 1),
            format!("worker B capacity update was rejected: {response:?}"),
        )?;
        let capacity_sequence = publish_job(&context, &repository, &capacity_job.id).await?;
        queue_sequences.push(capacity_sequence);
        sleep(Duration::from_millis(1_500)).await;
        let response = heartbeat(&worker_b, extension_b.clone(), 1).await?;
        require(
            matches!(response, ControlResponse::Heartbeat { ref extended, .. } if extended.len() == 1),
            format!("worker B capacity-saturated heartbeat was rejected: {response:?}"),
        )?;
        sleep(Duration::from_secs(1)).await;
        for blocked in [&quota_job.id, &capacity_job.id] {
            let snapshot = repository
                .get_job_scheduling_snapshot(blocked)
                .await?
                .ok_or_else(|| std::io::Error::other("blocked job disappeared"))?;
            require(
                snapshot.job.state == JobState::Queued && snapshot.delivery_attempts == 0,
                format!("blocked job {blocked} consumed an execution attempt"),
            )?;
        }
        let blocked_ids = vec![quota_job.id.clone(), capacity_job.id.clone()];
        let blocked_raw = blocked_ids.iter().map(ToString::to_string).collect::<Vec<_>>();
        let durable_dlq_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM outbox_messages WHERE subject = $1 \
             AND payload->>'job_id' = ANY($2)",
        )
        .bind(JOBS_DEAD_SUBJECT)
        .bind(&blocked_raw)
        .fetch_one(repository.pool())
        .await?;
        require(
            durable_dlq_count == 0,
            "blocked work unexpectedly produced a durable dead letter",
        )?;
        assert_no_dlq_since(topology.dlq_stream.clone(), dlq_baseline + 1, &blocked_ids).await?;

        // The assertion work above stands in for a running worker. Refresh the
        // exact fence immediately before completion so a slow CI runner tests
        // result ordering rather than accidentally testing lease expiry.
        let response = heartbeat(&worker_b, extension_b.clone(), 1).await?;
        require(
            matches!(response, ControlResponse::Heartbeat { ref extended, .. } if extended == &vec![extension_b]),
            format!("worker B final lease refresh was rejected: {response:?}"),
        )?;
        let result_b = request(
            &worker_b,
            job_result_subject(&worker_b.worker_id),
            &passed_result(&dispatch_b),
        )
        .await?;
        require(
            result_b == ControlResponse::Accepted,
            format!("worker B terminal result was rejected: {result_b:?}"),
        )?;
        let terminal = repository
            .get_job_scheduling_snapshot(&main_job.id)
            .await?
            .ok_or_else(|| std::io::Error::other("terminal job disappeared"))?;
        require(
            terminal.job.state == JobState::Passed
                && terminal.job.outcome == Some(JobOutcome::Passed),
            "worker B result was not durably committed before the response",
        )?;
        wait_for_source_removal(&topology.job_stream, main_sequence).await?;
        Ok(())
    }
    .await;

    let runtime_cleanup = if let Some(active) = runtime.take() {
        active.stop().await
    } else {
        Ok(())
    };
    worker_a.stop();
    worker_b.stop();
    for sequence in &queue_sequences {
        let _ = topology.job_stream.delete_message(*sequence).await;
    }
    let jobs = vec![main_job.id, quota_job.id, capacity_job.id];
    let projects = vec![main_fixture.project_id, capacity_fixture.project_id];
    let workers = vec![worker_a_id, worker_b_id];
    let database_cleanup = cleanup_database(repository.pool(), &projects, &workers, &jobs).await;
    let dlq_cleanup = async {
        let last_sequence = dlq_stream.info().await?.state.last_sequence;
        for sequence in (dlq_baseline + 1)..=last_sequence {
            let Ok(message) = dlq_stream.get_raw_message(sequence).await else {
                continue;
            };
            let Ok(dead_letter) = serde_json::from_slice::<JobDeadLetter>(&message.payload) else {
                continue;
            };
            if dead_letter
                .job_id
                .as_ref()
                .is_some_and(|job_id| jobs.contains(job_id))
            {
                let _ = dlq_stream.delete_message(sequence).await;
            }
        }
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;
    let topology_restore: TestResult = provision_topology(&context, &restore_topology)
        .await
        .map(|_| ())
        .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>);

    exercise?;
    runtime_cleanup?;
    database_cleanup?;
    dlq_cleanup?;
    topology_restore?;
    Ok(())
}

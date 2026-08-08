mod common;

use std::{env, error::Error, io, sync::Arc, time::Duration};

use async_nats::{HeaderMap, jetstream};
use chrono::{Duration as ChronoDuration, Utc};
use futures_util::{Stream, StreamExt as _};
use run_anywhere_contracts::{
    ArtifactSelection, AutomationSpec, CreateJobRequest, DurationSeconds, HostArch, IsolationTier,
    JobDeadLetter, JobDeadLetterReason, JobId, JobMode, JobOutcome, JobQueued, JobResult, JobState,
    LeaseId, RuntimeKind, RuntimeProfileId, Sha256, UploadKind, WorkerId, WorkerRegistration,
};
use run_anywhere_repository::{Repository, RepositoryError};
use run_anywhere_scheduler::{
    AckBinding, AckRegistry, Config, DlqPublisher, NoopRuntimeReaper, Reconciler, SchedulerMetrics,
    TopologyConfig,
    config::TimingSettings,
    connect_nats,
    dlq::exhaustion_event_key,
    provision_topology,
    subjects::{JOBS_DEAD_SUBJECT, JOBS_QUEUED_SUBJECT},
};
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::time::timeout;
use url::Url;
use uuid::Uuid;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

const EMULATOR_PROFILE: &str = "rtp_android_35_pixel_6_x86_64_emulator";
const MAX_RUN_ATTEMPTS: u32 = 3;
const NATS_MESSAGE_ID_HEADER: &str = "Nats-Msg-Id";

#[derive(Default)]
struct NatsArtifacts {
    job_id: Option<JobId>,
    source_sequence: Option<u64>,
    dlq_scan_after: u64,
}

fn integration_enabled() -> bool {
    env::var("RUN_SCHEDULER_INTEGRATION").is_ok_and(|value| value.eq_ignore_ascii_case("true"))
}

fn check(condition: bool, message: impl Into<String>) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(io::Error::other(message.into()).into())
    }
}

#[tokio::test]
async fn three_abandoned_claims_publish_one_dlq_record_and_retire_source() -> TestResult {
    if !integration_enabled() {
        eprintln!("skipping live scheduler DLQ recovery test; set RUN_SCHEDULER_INTEGRATION=true");
        return Ok(());
    }

    let config = Config::from_env()?;
    let _topology_lock =
        common::acquire_shared_nats_topology_lock(config.database_url.expose_secret()).await?;
    let nats = connect_nats(&config.nats).await?;
    let jetstream = jetstream::new(nats);
    let topology = provision_topology(&jetstream, &TopologyConfig::try_from(&config)?).await?;
    let mut dlq_stream = topology.dlq_stream;
    let mut artifacts = NatsArtifacts {
        dlq_scan_after: dlq_stream.info().await?.state.last_sequence,
        ..NatsArtifacts::default()
    };

    let admin_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(config.database_url.expose_secret())
        .await?;
    let database_name = format!("raa_dlq_live_{}", Uuid::new_v4().simple());
    let database_url = database_url(config.database_url.expose_secret(), &database_name)?;
    create_test_database(&admin_pool, &database_name).await?;

    let pool = match PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
    {
        Ok(pool) => pool,
        Err(error) => {
            drop_test_database(&admin_pool, &database_name).await?;
            return Err(error.into());
        }
    };
    let repository = Repository::new(pool.clone());
    let exercise = match repository.migrate().await {
        Ok(()) => {
            run_recovery_case(
                repository,
                jetstream,
                topology.job_stream.clone(),
                topology.job_consumer,
                &mut dlq_stream,
                &mut artifacts,
            )
            .await
        }
        Err(error) => Err(error.into()),
    };

    let nats_cleanup = cleanup_nats(&topology.job_stream, &dlq_stream, &artifacts).await;
    pool.close().await;
    let database_cleanup = drop_test_database(&admin_pool, &database_name).await;
    admin_pool.close().await;

    exercise?;
    nats_cleanup?;
    database_cleanup?;
    Ok(())
}

async fn run_recovery_case(
    repository: Repository,
    jetstream: jetstream::Context,
    job_stream: jetstream::stream::Stream,
    consumer: jetstream::consumer::PullConsumer,
    dlq_stream: &mut jetstream::stream::Stream,
    artifacts: &mut NatsArtifacts,
) -> TestResult {
    let suffix = Uuid::new_v4().simple().to_string();
    let project = repository
        .create_project(format!("DLQ recovery live {suffix}"), "scheduler-live-test")
        .await?;
    let upload = repository
        .create_upload(
            &project.id,
            UploadKind::Apk,
            format!("scheduler-live/{suffix}/app.apk"),
            Sha256::new("a".repeat(64))?,
            4_096,
        )
        .await?;
    let profile_id = RuntimeProfileId::new(EMULATOR_PROFILE)?;
    let created = repository
        .create_job(
            CreateJobRequest {
                project_id: project.id.clone(),
                apk_upload_id: upload.id,
                test_upload_id: None,
                runtime_profile: profile_id,
                mode: JobMode::HeadlessCi,
                min_isolation: IsolationTier::VmIsolated,
                automation: AutomationSpec::BuiltInSmoke,
                artifacts: ArtifactSelection {
                    screenshots: false,
                    video: false,
                    logcat: false,
                    junit: false,
                },
                timeout_seconds: DurationSeconds::new(30)?,
            },
            format!("dlq-live-{suffix}"),
        )
        .await?;
    let job_id = created.job.id;
    artifacts.job_id = Some(job_id.clone());
    let queued = repository
        .get_job_scheduling_snapshot(&job_id)
        .await?
        .ok_or_else(|| io::Error::other("new job has no scheduling snapshot"))?
        .queued;

    let worker_id = WorkerId::new(format!("wrk_dlq_{suffix}"))?;
    repository
        .upsert_worker(WorkerRegistration {
            worker_id: worker_id.clone(),
            runtimes: vec![RuntimeKind::AndroidEmulatorContainer],
            kvm: true,
            gpu: false,
            arch: HostArch::X86_64,
            capacity: 1,
        })
        .await?;

    let mut headers = HeaderMap::new();
    headers.insert(
        NATS_MESSAGE_ID_HEADER,
        format!("dlq-live:{job_id}:{suffix}"),
    );
    let publish = jetstream
        .publish_with_headers(
            JOBS_QUEUED_SUBJECT,
            headers,
            serde_json::to_vec(&queued)?.into(),
        )
        .await?
        .await?;
    artifacts.source_sequence = Some(publish.sequence);

    let ack_registry = AckRegistry::default();
    let dlq = DlqPublisher::new(
        repository.clone(),
        jetstream,
        format!("scheduler-dlq-live-{suffix}"),
    );
    let reconciler = Reconciler::new(
        repository.clone(),
        ack_registry.clone(),
        dlq,
        job_stream.clone(),
        Arc::new(NoopRuntimeReaper),
        TimingSettings {
            worker_heartbeat: Duration::from_millis(50),
            stale_worker_threshold: Duration::from_secs(1),
            ack_wait: Duration::from_secs(2),
            database_lease_ttl: Duration::from_secs(3),
            reconciliation_interval: Duration::from_millis(100),
            reap_grace: Duration::from_secs(1),
        },
        MAX_RUN_ATTEMPTS,
        SchedulerMetrics::default(),
    );
    let mut deliveries = consumer
        .stream()
        .max_messages_per_batch(64)
        .messages()
        .await?;
    let mut first_lease = None;

    for attempt in 1..=MAX_RUN_ATTEMPTS {
        let message = next_delivery_for_job(&mut deliveries, &job_id).await?;
        let info = message.info()?;
        check(
            info.stream_sequence == publish.sequence,
            format!(
                "attempt {attempt} used source sequence {}, expected {}",
                info.stream_sequence, publish.sequence
            ),
        )?;
        check(
            u32::try_from(info.delivered).unwrap_or(u32::MAX) == attempt,
            format!(
                "claim {attempt} arrived as JetStream delivery {}",
                info.delivered
            ),
        )?;

        let lease_id = LeaseId::new(format!("lease_dlq_{suffix}_{attempt}"))?;
        if first_lease.is_none() {
            first_lease = Some(lease_id.clone());
        }
        repository
            .claim_job(
                &job_id,
                &worker_id,
                &lease_id,
                Utc::now() + ChronoDuration::seconds(30),
            )
            .await?;
        ack_registry
            .bind(
                job_id.clone(),
                AckBinding {
                    worker_id: worker_id.clone(),
                    lease_id: lease_id.clone(),
                    stream_sequence: info.stream_sequence,
                    message,
                },
            )
            .await;
        expire_lease(repository.pool(), &job_id, &lease_id).await?;

        let report = reconciler.run_once().await?;
        if attempt < MAX_RUN_ATTEMPTS {
            check(
                report.requeued == 1 && report.exhausted == 0,
                format!("attempt {attempt} was not requeued exactly once: {report:?}"),
            )?;
        } else {
            check(
                report.exhausted == 1 && report.finalized == 1,
                format!("third abandonment did not finalize once: {report:?}"),
            )?;
        }

        if attempt == 1 {
            let late_result = repository
                .record_job_result(JobResult {
                    job_id: job_id.clone(),
                    worker_id: worker_id.clone(),
                    lease_id: first_lease
                        .clone()
                        .ok_or_else(|| io::Error::other("first lease was not captured"))?,
                    outcome: JobOutcome::Passed,
                    artifact_ids: Vec::new(),
                    artifacts_finalized: true,
                    cleanup_completed: true,
                    error: None,
                    completed_at: Utc::now(),
                })
                .await;
            check(
                matches!(late_result, Err(RepositoryError::CompareAndSwapLost { .. })),
                "the first abandoned lease accepted a late result",
            )?;
        }
    }

    check(
        ack_registry.is_empty().await,
        "the exhausted source acknowledgement remained bound",
    )?;
    let snapshot = repository
        .get_job_scheduling_snapshot(&job_id)
        .await?
        .ok_or_else(|| io::Error::other("exhausted job disappeared"))?;
    check(
        snapshot.job.state == JobState::InfraFailed
            && snapshot.delivery_attempts == MAX_RUN_ATTEMPTS
            && snapshot.lease.is_none(),
        format!("unexpected exhausted job snapshot: {snapshot:?}"),
    )?;

    let event_key = exhaustion_event_key(&job_id, MAX_RUN_ATTEMPTS);
    let outbox_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM outbox_messages WHERE event_key = $1 AND subject = $2",
    )
    .bind(&event_key)
    .bind(JOBS_DEAD_SUBJECT)
    .fetch_one(repository.pool())
    .await?;
    check(
        outbox_count == 1,
        "exhaustion did not create exactly one DLQ outbox row",
    )?;
    let outbox = repository
        .get_outbox_message(&event_key)
        .await?
        .ok_or_else(|| io::Error::other("DLQ outbox row is missing"))?;
    check(
        outbox.published_at.is_some(),
        "DLQ outbox publication was not confirmed",
    )?;
    let outbox_dead_letter: JobDeadLetter = serde_json::from_value(outbox.payload)?;
    verify_dead_letter(&outbox_dead_letter, &job_id, &project.id)?;

    // A second pass models leader restart after the terminal commit. It must
    // neither publish a duplicate nor recreate ownership of the source record.
    let replay_report = reconciler.run_once().await?;
    check(
        replay_report == Default::default(),
        format!("idempotent reconciliation still changed state: {replay_report:?}"),
    )?;
    let dlq_records = matching_dlq_records(
        dlq_stream,
        artifacts.dlq_scan_after.saturating_add(1),
        &job_id,
    )
    .await?;
    check(
        dlq_records.len() == 1,
        format!(
            "expected one jobs.dead record for {job_id}, found {}",
            dlq_records.len()
        ),
    )?;
    verify_dead_letter(&dlq_records[0].1, &job_id, &project.id)?;
    check(
        job_stream.get_raw_message(publish.sequence).await.is_err(),
        "the exhausted source message was not deleted",
    )?;

    let late_terminal_result = repository
        .record_job_result(JobResult {
            job_id,
            worker_id,
            lease_id: first_lease.ok_or_else(|| io::Error::other("first lease is missing"))?,
            outcome: JobOutcome::Passed,
            artifact_ids: Vec::new(),
            artifacts_finalized: true,
            cleanup_completed: true,
            error: None,
            completed_at: Utc::now(),
        })
        .await;
    check(
        matches!(
            late_terminal_result,
            Err(RepositoryError::CompareAndSwapLost { .. })
        ),
        "a terminal job accepted the first abandoned lease",
    )?;
    Ok(())
}

async fn next_delivery_for_job<S, E>(
    messages: &mut S,
    job_id: &JobId,
) -> TestResult<jetstream::Message>
where
    S: Stream<Item = Result<jetstream::Message, E>> + Unpin,
    E: Error + Send + Sync + 'static,
{
    timeout(Duration::from_secs(5), async {
        loop {
            let message = messages
                .next()
                .await
                .ok_or_else(|| io::Error::other("job consumer ended"))??;
            let belongs_to_job = serde_json::from_slice::<JobQueued>(&message.payload)
                .is_ok_and(|queued| queued.job_id == *job_id);
            if belongs_to_job {
                return Ok(message);
            }
            // Preserve any unrelated delivery owned by another concurrently
            // running live test while allowing this test's unique record through.
            message.ack_with(jetstream::AckKind::Progress).await?;
        }
    })
    .await
    .map_err(|_| io::Error::other(format!("timed out waiting for delivery of {job_id}")))?
}

async fn expire_lease(pool: &PgPool, job_id: &JobId, lease_id: &LeaseId) -> TestResult {
    let updated = sqlx::query(
        "UPDATE jobs SET last_lease_extended_at = clock_timestamp() - interval '2 minutes', \
         lease_expires_at = clock_timestamp() - interval '1 minute' \
         WHERE id = $1 AND lease_id = $2",
    )
    .bind(job_id.as_str())
    .bind(lease_id.as_str())
    .execute(pool)
    .await?;
    check(
        updated.rows_affected() == 1,
        "could not expire the exact job lease",
    )
}

fn verify_dead_letter(
    dead_letter: &JobDeadLetter,
    job_id: &JobId,
    project_id: &run_anywhere_contracts::ProjectId,
) -> TestResult {
    check(
        dead_letter.job_id.as_ref() == Some(job_id)
            && dead_letter.project_id.as_ref() == Some(project_id)
            && dead_letter.source_stream_sequence.is_none()
            && dead_letter.attempt == MAX_RUN_ATTEMPTS
            && dead_letter.reason == JobDeadLetterReason::AttemptsExhausted,
        format!("DLQ record is not the sanitized exhaustion contract: {dead_letter:?}"),
    )
}

async fn matching_dlq_records(
    stream: &mut jetstream::stream::Stream,
    first_sequence: u64,
    job_id: &JobId,
) -> TestResult<Vec<(u64, JobDeadLetter)>> {
    let last_sequence = stream.info().await?.state.last_sequence;
    let mut records = Vec::new();
    for sequence in first_sequence..=last_sequence {
        let Ok(message) = stream.get_raw_message(sequence).await else {
            continue;
        };
        let Ok(dead_letter) = serde_json::from_slice::<JobDeadLetter>(&message.payload) else {
            continue;
        };
        if dead_letter.job_id.as_ref() == Some(job_id) {
            records.push((sequence, dead_letter));
        }
    }
    Ok(records)
}

async fn cleanup_nats(
    job_stream: &jetstream::stream::Stream,
    dlq_stream: &jetstream::stream::Stream,
    artifacts: &NatsArtifacts,
) -> TestResult {
    if let Some(sequence) = artifacts.source_sequence {
        let _ = job_stream.delete_message(sequence).await;
    }
    let Some(job_id) = artifacts.job_id.as_ref() else {
        return Ok(());
    };
    let mut scan_stream = dlq_stream.clone();
    let records = matching_dlq_records(
        &mut scan_stream,
        artifacts.dlq_scan_after.saturating_add(1),
        job_id,
    )
    .await?;
    for (sequence, _) in records {
        scan_stream.delete_message(sequence).await?;
    }
    Ok(())
}

fn database_url(base: &str, database_name: &str) -> TestResult<String> {
    let mut url = Url::parse(base)?;
    url.set_path(&format!("/{database_name}"));
    Ok(url.into())
}

async fn create_test_database(admin_pool: &PgPool, database_name: &str) -> TestResult {
    check(
        database_name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_'),
        "generated database name is not a safe PostgreSQL identifier",
    )?;
    sqlx::query(&format!("CREATE DATABASE \"{database_name}\""))
        .execute(admin_pool)
        .await?;
    Ok(())
}

async fn drop_test_database(admin_pool: &PgPool, database_name: &str) -> TestResult {
    sqlx::query(&format!(
        "DROP DATABASE IF EXISTS \"{database_name}\" WITH (FORCE)"
    ))
    .execute(admin_pool)
    .await?;
    Ok(())
}

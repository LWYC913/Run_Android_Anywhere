use std::collections::{BTreeMap, HashSet};

use chrono::{Duration, Utc};
use run_anywhere_contracts::{
    AndroidAbi, ArtifactKind, ArtifactSelection, AuthScope, AutomationSpec, CreateJobRequest,
    CreateWebhookRequest, DebugSessionMode, DurationSeconds, ErrorCode, HostArch, IsolationTier,
    JobLeaseExtension, JobMode, JobOutcome, JobResult, JobState, JobSummary, LeaseId, ProjectId,
    RuntimeKind, RuntimeProfile, RuntimeProfileId, Sha256, TransitionEvidence, UploadId,
    UploadKind, Uri, WebhookEvent, WorkerHeartbeat, WorkerId, WorkerRegistration, WorkerState,
};
use run_anywhere_repository::{
    CreatedJob, JobListQuery, LeaseGuard, MIGRATOR, RecoveryDisposition, Repository,
    RepositoryError, StaleJobCriteria,
};
use serde_json::json;
use sqlx::PgPool;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const EMULATOR_PROFILE: &str = "rtp_android_35_pixel_6_x86_64_emulator";
const REDROID_PROFILE: &str = "rtp_android_34_generic_x86_64_redroid";

#[derive(Clone)]
struct Fixture {
    repository: Repository,
    project_id: ProjectId,
    apk_upload_id: UploadId,
    profile: RuntimeProfile,
}

impl Fixture {
    async fn new(pool: &PgPool) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let repository = Repository::new(pool.clone());
        let project = repository
            .create_project("integration project", "test-owner")
            .await?;
        let upload = repository
            .create_upload(
                &project.id,
                UploadKind::Apk,
                "projects/integration/app.apk",
                digest('a'),
                4_096,
            )
            .await?;
        let profile_id = RuntimeProfileId::new(EMULATOR_PROFILE)?;
        let profile = repository
            .get_runtime_profile(&profile_id)
            .await?
            .expect("seeded emulator profile must exist");

        Ok(Self {
            repository,
            project_id: project.id,
            apk_upload_id: upload.id,
            profile,
        })
    }

    fn request(&self) -> CreateJobRequest {
        CreateJobRequest {
            project_id: self.project_id.clone(),
            apk_upload_id: self.apk_upload_id.clone(),
            test_upload_id: None,
            runtime_profile: self.profile.id.clone(),
            mode: JobMode::HeadlessCi,
            min_isolation: IsolationTier::VmIsolated,
            automation: AutomationSpec::BuiltInSmoke,
            artifacts: ArtifactSelection {
                screenshots: true,
                video: false,
                logcat: true,
                junit: true,
            },
            timeout_seconds: DurationSeconds::new(300).expect("positive duration"),
        }
    }

    async fn create_job(&self, idempotency_key: &str) -> Result<CreatedJob, RepositoryError> {
        self.repository
            .create_job(self.request(), idempotency_key)
            .await
    }
}

fn digest(character: char) -> Sha256 {
    Sha256::new(character.to_string().repeat(64)).expect("test digest is canonical")
}

async fn register_worker(
    repository: &Repository,
    worker_id: &str,
    runtimes: Vec<RuntimeKind>,
    kvm: bool,
    arch: HostArch,
    capacity: u32,
) -> Result<WorkerId, Box<dyn std::error::Error + Send + Sync>> {
    let worker_id = WorkerId::new(worker_id)?;
    repository
        .upsert_worker(WorkerRegistration {
            worker_id: worker_id.clone(),
            runtimes,
            kvm,
            gpu: false,
            arch,
            capacity,
        })
        .await?;
    Ok(worker_id)
}

fn assert_append_only_error(error: &sqlx::Error) {
    let code = error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::code);
    assert_eq!(code.as_deref(), Some("55000"));
    assert!(error.to_string().contains("append-only"));
}

#[sqlx::test(migrator = "run_anywhere_repository::MIGRATOR")]
async fn migrations_are_reversible_and_seed_exact_profiles(pool: PgPool) -> TestResult {
    let repository = Repository::new(pool.clone());
    let profiles = repository.list_runtime_profiles().await?;
    assert_eq!(profiles.len(), 4);

    let profile_ids = profiles
        .iter()
        .map(|profile| profile.id.as_str())
        .collect::<HashSet<_>>();
    assert_eq!(
        profile_ids,
        HashSet::from([
            "rtp_android_35_pixel_6_x86_64_emulator",
            "rtp_android_34_pixel_5_x86_64_emulator",
            "rtp_android_35_generic_arm64_redroid",
            "rtp_android_34_generic_x86_64_redroid",
        ])
    );
    for profile in &profiles {
        profile.validate()?;
        assert!(profile.image_ref.starts_with("ghcr.io/lwyc913/"));
        assert!(profile.image_ref.ends_with("-v1"));
        assert_ne!(profile.runtime_kind, RuntimeKind::BrowserNativeWasm);
    }

    let emulator = profiles
        .iter()
        .find(|profile| profile.id.as_str() == EMULATOR_PROFILE)
        .expect("emulator seed");
    assert_eq!(emulator.android_api, 35);
    assert_eq!(emulator.device_profile, "pixel_6");
    assert_eq!(emulator.abi, AndroidAbi::X86_64);
    assert_eq!(emulator.host_arch, HostArch::X86_64);
    assert_eq!(emulator.runtime_kind, RuntimeKind::AndroidEmulatorContainer);
    assert_eq!(emulator.isolation_tier, IsolationTier::VmIsolated);

    MIGRATOR.undo(&pool, 0).await?;
    let jobs_table: Option<String> = sqlx::query_scalar("SELECT to_regclass('public.jobs')::TEXT")
        .fetch_one(&pool)
        .await?;
    assert!(jobs_table.is_none());

    MIGRATOR.run(&pool).await?;
    assert_eq!(repository.list_runtime_profiles().await?.len(), 4);
    let jobs_table: Option<String> = sqlx::query_scalar("SELECT to_regclass('public.jobs')::TEXT")
        .fetch_one(&pool)
        .await?;
    assert_eq!(jobs_table.as_deref(), Some("jobs"));
    Ok(())
}

#[sqlx::test(migrator = "run_anywhere_repository::MIGRATOR")]
async fn concurrent_job_creation_is_idempotent(pool: PgPool) -> TestResult {
    let fixture = Fixture::new(&pool).await?;
    let left_repository = fixture.repository.clone();
    let right_repository = fixture.repository.clone();
    let left_request = fixture.request();
    let right_request = fixture.request();

    let (left, right) = tokio::join!(
        left_repository.create_job(left_request, "same-request"),
        right_repository.create_job(right_request, "same-request")
    );
    let left = left?;
    let right = right?;

    assert_eq!(left.job.id, right.job.id);
    assert_ne!(left.was_created, right.was_created);
    let event_count = fixture
        .repository
        .list_job_events_after(&left.job.id, 0, 10)
        .await?
        .len();
    assert_eq!(event_count, 1, "only the winning insert emits job.queued");
    let persisted_jobs: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs")
        .fetch_one(&pool)
        .await?;
    assert_eq!(persisted_jobs, 1);

    let mut changed_retry = fixture.request();
    changed_retry.apk_upload_id = UploadId::new("upl_missing_retry_body")?;
    let retry = fixture
        .repository
        .create_job(changed_retry, "same-request")
        .await?;
    assert!(!retry.was_created);
    assert_eq!(retry.job.id, left.job.id);
    Ok(())
}

#[sqlx::test(migrator = "run_anywhere_repository::MIGRATOR")]
async fn job_creation_rejects_foreign_wrong_kind_and_weak_isolation_uploads(
    pool: PgPool,
) -> TestResult {
    let fixture = Fixture::new(&pool).await?;
    let foreign_project = fixture
        .repository
        .create_project("foreign project", "other-owner")
        .await?;
    let foreign_apk = fixture
        .repository
        .create_upload(
            &foreign_project.id,
            UploadKind::Apk,
            "projects/foreign/app.apk",
            digest('c'),
            10,
        )
        .await?;
    let wrong_kind = fixture
        .repository
        .create_upload(
            &fixture.project_id,
            UploadKind::Test,
            "projects/integration/tests.zip",
            digest('d'),
            20,
        )
        .await?;

    let mut foreign_request = fixture.request();
    foreign_request.apk_upload_id = foreign_apk.id;
    assert!(matches!(
        fixture
            .repository
            .create_job(foreign_request, "foreign-upload")
            .await,
        Err(RepositoryError::Validation(message)) if message.contains("does not belong")
    ));

    let mut wrong_kind_request = fixture.request();
    wrong_kind_request.apk_upload_id = wrong_kind.id;
    assert!(matches!(
        fixture
            .repository
            .create_job(wrong_kind_request, "wrong-kind")
            .await,
        Err(RepositoryError::Validation(message)) if message.contains("must be Apk")
    ));

    let mut weak_isolation_request = fixture.request();
    weak_isolation_request.runtime_profile = RuntimeProfileId::new(REDROID_PROFILE)?;
    assert!(matches!(
        fixture
            .repository
            .create_job(weak_isolation_request, "weak-isolation")
            .await,
        Err(RepositoryError::Validation(message)) if message.contains("does not satisfy")
    ));

    let persisted_jobs: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs")
        .fetch_one(&pool)
        .await?;
    assert_eq!(persisted_jobs, 0);
    Ok(())
}

#[sqlx::test(migrator = "run_anywhere_repository::MIGRATOR")]
async fn keyset_pagination_is_complete_with_and_without_state_filter(pool: PgPool) -> TestResult {
    let fixture = Fixture::new(&pool).await?;
    let mut created_ids = HashSet::new();
    for index in 0..6 {
        let job = fixture
            .create_job(&format!("pagination-{index}"))
            .await?
            .job;
        created_ids.insert(job.id.clone());
        if index == 1 || index == 4 {
            fixture
                .repository
                .transition_job_state(
                    &job.id,
                    JobState::Queued,
                    JobState::CollectingArtifacts,
                    TransitionEvidence {
                        pending_outcome: Some(JobOutcome::Cancelled),
                        artifacts_finalized: false,
                        cleanup_completed: false,
                    },
                    None,
                    BTreeMap::new(),
                )
                .await?;
        }
    }

    let all_jobs = collect_job_pages(&fixture.repository, &fixture.project_id, None).await?;
    assert_eq!(all_jobs.len(), 6);
    assert_eq!(
        all_jobs
            .iter()
            .map(|job| job.id.clone())
            .collect::<HashSet<_>>(),
        created_ids
    );
    assert!(all_jobs.windows(2).all(|pair| {
        pair[0].created_at > pair[1].created_at
            || (pair[0].created_at == pair[1].created_at && pair[0].id > pair[1].id)
    }));

    let queued_jobs = collect_job_pages(
        &fixture.repository,
        &fixture.project_id,
        Some(JobState::Queued),
    )
    .await?;
    assert_eq!(queued_jobs.len(), 4);
    assert!(queued_jobs.iter().all(|job| job.state == JobState::Queued));
    assert_eq!(
        queued_jobs
            .iter()
            .map(|job| &job.id)
            .collect::<HashSet<_>>()
            .len(),
        4
    );
    Ok(())
}

async fn collect_job_pages(
    repository: &Repository,
    project_id: &ProjectId,
    state: Option<JobState>,
) -> Result<Vec<JobSummary>, RepositoryError> {
    let mut jobs = Vec::new();
    let mut cursor = None;
    loop {
        let page = repository
            .list_jobs(JobListQuery {
                project_id: project_id.clone(),
                state,
                cursor,
                limit: 2,
            })
            .await?;
        jobs.extend(page.items);
        let Some(next_cursor) = page.next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }
    Ok(jobs)
}

#[sqlx::test(migrator = "run_anywhere_repository::MIGRATOR")]
async fn transitions_validate_and_only_one_compare_and_swap_wins(pool: PgPool) -> TestResult {
    let fixture = Fixture::new(&pool).await?;
    let job = fixture.create_job("transition-job").await?.job;
    let worker_id = register_worker(
        &fixture.repository,
        "wrk_transition",
        vec![RuntimeKind::AndroidEmulatorContainer],
        true,
        HostArch::X86_64,
        1,
    )
    .await?;
    let lease_id = LeaseId::new("lease_transition")?;
    fixture
        .repository
        .claim_job(
            &job.id,
            &worker_id,
            &lease_id,
            Utc::now() + Duration::hours(1),
        )
        .await?;
    let guard = LeaseGuard {
        worker_id,
        lease_id,
    };

    let invalid = fixture
        .repository
        .transition_job_state(
            &job.id,
            JobState::Claimed,
            JobState::Passed,
            TransitionEvidence {
                pending_outcome: Some(JobOutcome::Passed),
                artifacts_finalized: true,
                cleanup_completed: true,
            },
            Some(&guard),
            BTreeMap::new(),
        )
        .await
        .expect_err("claimed -> passed must be rejected before SQL");
    assert!(matches!(invalid, RepositoryError::InvalidTransition(_)));

    let left_repository = fixture.repository.clone();
    let right_repository = fixture.repository.clone();
    let left_job_id = job.id.clone();
    let right_job_id = job.id.clone();
    let left_guard = guard.clone();
    let right_guard = guard;
    let (left, right) = tokio::join!(
        left_repository.transition_job_state(
            &left_job_id,
            JobState::Claimed,
            JobState::ProvisioningRuntime,
            TransitionEvidence::default(),
            Some(&left_guard),
            BTreeMap::new(),
        ),
        right_repository.transition_job_state(
            &right_job_id,
            JobState::Claimed,
            JobState::ProvisioningRuntime,
            TransitionEvidence::default(),
            Some(&right_guard),
            BTreeMap::new(),
        )
    );
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    let loser = if left.is_err() { left } else { right };
    assert!(matches!(
        loser,
        Err(RepositoryError::CompareAndSwapLost { .. })
    ));
    assert_eq!(
        fixture
            .repository
            .get_job(&job.id)
            .await?
            .expect("job exists")
            .state,
        JobState::ProvisioningRuntime
    );
    Ok(())
}

#[sqlx::test(migrator = "run_anywhere_repository::MIGRATOR")]
async fn claim_race_has_one_winner_and_capacity_cannot_overbook(pool: PgPool) -> TestResult {
    let fixture = Fixture::new(&pool).await?;
    let contested_job = fixture.create_job("contested-job").await?.job;
    let second_job = fixture.create_job("second-job").await?.job;
    let worker_a = register_worker(
        &fixture.repository,
        "wrk_claim_a",
        vec![RuntimeKind::AndroidEmulatorContainer],
        true,
        HostArch::X86_64,
        1,
    )
    .await?;
    let worker_b = register_worker(
        &fixture.repository,
        "wrk_claim_b",
        vec![RuntimeKind::AndroidEmulatorContainer],
        true,
        HostArch::X86_64,
        1,
    )
    .await?;
    let lease_a = LeaseId::new("lease_claim_a")?;
    let lease_b = LeaseId::new("lease_claim_b")?;
    let left_repository = fixture.repository.clone();
    let right_repository = fixture.repository.clone();
    let left_job_id = contested_job.id.clone();
    let right_job_id = contested_job.id.clone();
    let expires_at = Utc::now() + Duration::hours(1);

    let (left, right) = tokio::join!(
        left_repository.claim_job(&left_job_id, &worker_a, &lease_a, expires_at),
        right_repository.claim_job(&right_job_id, &worker_b, &lease_b, expires_at)
    );
    assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
    let winning_worker = match (&left, &right) {
        (Ok(claim), Err(RepositoryError::CompareAndSwapLost { .. }))
        | (Err(RepositoryError::CompareAndSwapLost { .. }), Ok(claim)) => claim.worker_id.clone(),
        result => panic!("unexpected claim race result: {result:?}"),
    };

    let capacity_error = fixture
        .repository
        .claim_job(
            &second_job.id,
            &winning_worker,
            &LeaseId::new("lease_capacity")?,
            Utc::now() + Duration::hours(1),
        )
        .await
        .expect_err("capacity-one winner cannot claim a second job");
    assert!(matches!(capacity_error, RepositoryError::Conflict(_)));

    let workers = fixture.repository.list_workers().await?;
    assert_eq!(
        workers.iter().map(|worker| worker.active_jobs).sum::<u32>(),
        1
    );
    assert!(
        workers
            .iter()
            .all(|worker| worker.active_jobs <= worker.capacity)
    );
    Ok(())
}

#[sqlx::test(migrator = "run_anywhere_repository::MIGRATOR")]
async fn heartbeat_extends_only_the_exact_owned_lease(pool: PgPool) -> TestResult {
    let fixture = Fixture::new(&pool).await?;
    let job = fixture.create_job("heartbeat-job").await?.job;
    let worker_id = register_worker(
        &fixture.repository,
        "wrk_heartbeat",
        vec![RuntimeKind::AndroidEmulatorContainer],
        true,
        HostArch::X86_64,
        1,
    )
    .await?;
    let lease_id = LeaseId::new("lease_heartbeat")?;
    fixture
        .repository
        .claim_job(
            &job.id,
            &worker_id,
            &lease_id,
            Utc::now() + Duration::hours(1),
        )
        .await?;

    let accepted = JobLeaseExtension {
        job_id: job.id.clone(),
        lease_id: lease_id.clone(),
    };
    let rejected = JobLeaseExtension {
        job_id: job.id.clone(),
        lease_id: LeaseId::new("lease_not_owned")?,
    };
    let receipt = fixture
        .repository
        .record_heartbeat(
            WorkerHeartbeat {
                worker_id: worker_id.clone(),
                active_jobs: 1,
                capacity: 1,
                runtimes: vec![RuntimeKind::AndroidEmulatorContainer],
                kvm: true,
                gpu: false,
                arch: HostArch::X86_64,
                lease_extends: vec![accepted.clone(), rejected.clone()],
                last_seen: Utc::now(),
            },
            Duration::minutes(3),
        )
        .await?;
    assert_eq!(receipt.extended, vec![accepted]);
    assert_eq!(receipt.rejected, vec![rejected]);

    let (lease_expires_at, last_extended_at): (chrono::DateTime<Utc>, chrono::DateTime<Utc>) =
        sqlx::query_as("SELECT lease_expires_at, last_lease_extended_at FROM jobs WHERE id = $1")
            .bind(job.id.as_str())
            .fetch_one(&pool)
            .await?;
    assert_eq!(last_extended_at, receipt.recorded_at);
    assert_eq!(lease_expires_at, receipt.recorded_at + Duration::minutes(3));

    expire_lease(&pool, job.id.as_str(), None).await?;
    let expired_extension = JobLeaseExtension {
        job_id: job.id.clone(),
        lease_id: lease_id.clone(),
    };
    let receipt = fixture
        .repository
        .record_heartbeat(
            WorkerHeartbeat {
                worker_id: worker_id.clone(),
                active_jobs: 1,
                capacity: 1,
                runtimes: vec![RuntimeKind::AndroidEmulatorContainer],
                kvm: true,
                gpu: false,
                arch: HostArch::X86_64,
                lease_extends: vec![expired_extension.clone()],
                last_seen: Utc::now(),
            },
            Duration::minutes(3),
        )
        .await?;
    assert!(receipt.extended.is_empty());
    assert_eq!(receipt.rejected, vec![expired_extension]);

    let guard = LeaseGuard {
        worker_id: worker_id.clone(),
        lease_id: lease_id.clone(),
    };
    assert!(matches!(
        fixture
            .repository
            .transition_job_state(
                &job.id,
                JobState::Claimed,
                JobState::ProvisioningRuntime,
                TransitionEvidence::default(),
                Some(&guard),
                BTreeMap::new(),
            )
            .await,
        Err(RepositoryError::CompareAndSwapLost { .. })
    ));
    assert!(matches!(
        fixture
            .repository
            .record_job_result(JobResult {
                job_id: job.id,
                worker_id,
                lease_id,
                outcome: JobOutcome::Passed,
                artifact_ids: Vec::new(),
                artifacts_finalized: false,
                cleanup_completed: false,
                error: None,
                completed_at: Utc::now(),
            })
            .await,
        Err(RepositoryError::CompareAndSwapLost { .. })
    ));
    Ok(())
}

#[sqlx::test(migrator = "run_anywhere_repository::MIGRATOR")]
async fn stale_recovery_requeues_then_routes_exhaustion_to_finalization(
    pool: PgPool,
) -> TestResult {
    let fixture = Fixture::new(&pool).await?;
    let worker_id = register_worker(
        &fixture.repository,
        "wrk_recovery",
        vec![RuntimeKind::AndroidEmulatorContainer],
        true,
        HostArch::X86_64,
        1,
    )
    .await?;

    let requeue_job = fixture.create_job("recover-requeue").await?.job;
    fixture
        .repository
        .claim_job(
            &requeue_job.id,
            &worker_id,
            &LeaseId::new("lease_requeue")?,
            Utc::now() + Duration::hours(1),
        )
        .await?;
    expire_lease(&pool, requeue_job.id.as_str(), None).await?;
    let stale = fixture
        .repository
        .find_stale_jobs(StaleJobCriteria {
            lease_expired_before: Utc::now() + Duration::seconds(1),
            worker_heartbeat_before: Utc::now() - Duration::hours(1),
            limit: 10,
        })
        .await?;
    assert_eq!(stale.len(), 1);
    let recovered = fixture.repository.recover_stale_job(&stale[0], 2).await?;
    let RecoveryDisposition::Requeued(recovered_job) = recovered else {
        panic!("first delivery should be requeued")
    };
    assert_eq!(recovered_job.state, JobState::Queued);
    assert!(recovered_job.worker_id.is_none());
    assert_eq!(
        fixture.repository.recover_stale_job(&stale[0], 2).await?,
        RecoveryDisposition::LostRace
    );

    let exhausted_job = fixture.create_job("recover-exhausted").await?.job;
    fixture
        .repository
        .claim_job(
            &exhausted_job.id,
            &worker_id,
            &LeaseId::new("lease_exhausted")?,
            Utc::now() + Duration::hours(1),
        )
        .await?;
    expire_lease(&pool, exhausted_job.id.as_str(), Some(2)).await?;
    let stale = fixture
        .repository
        .find_stale_jobs(StaleJobCriteria {
            lease_expired_before: Utc::now() + Duration::seconds(1),
            worker_heartbeat_before: Utc::now() - Duration::hours(1),
            limit: 10,
        })
        .await?;
    assert_eq!(stale.len(), 1);
    let exhausted = fixture.repository.recover_stale_job(&stale[0], 2).await?;
    let RecoveryDisposition::Finalizing(exhausted_job) = exhausted else {
        panic!("max deliveries should enter the finalizer path")
    };
    assert_eq!(exhausted_job.state, JobState::CollectingArtifacts);
    assert_eq!(
        exhausted_job.failure.as_ref().map(|failure| failure.code),
        Some(ErrorCode::InfraFailed)
    );
    assert!(exhausted_job.outcome.is_none());
    assert_eq!(fixture.repository.list_workers().await?[0].active_jobs, 0);

    let heartbeat_stale_job = fixture.create_job("recover-stale-worker").await?.job;
    fixture
        .repository
        .claim_job(
            &heartbeat_stale_job.id,
            &worker_id,
            &LeaseId::new("lease_stale_worker")?,
            Utc::now() + Duration::hours(1),
        )
        .await?;
    sqlx::query(
        "UPDATE workers SET last_heartbeat_at = now() - interval '10 minutes' WHERE id = $1",
    )
    .bind(worker_id.as_str())
    .execute(&pool)
    .await?;
    let stale = fixture
        .repository
        .find_stale_jobs(StaleJobCriteria {
            lease_expired_before: Utc::now(),
            worker_heartbeat_before: Utc::now() - Duration::minutes(2),
            limit: 10,
        })
        .await?;
    assert_eq!(stale.len(), 1);
    assert_eq!(stale[0].job_id, heartbeat_stale_job.id);
    assert!(stale[0].lease_expires_at > stale[0].lease_expired_before);
    assert!(stale[0].worker_last_heartbeat_at <= stale[0].worker_heartbeat_before);
    assert!(matches!(
        fixture.repository.recover_stale_job(&stale[0], 2).await?,
        RecoveryDisposition::Requeued(_)
    ));
    Ok(())
}

async fn expire_lease(
    pool: &PgPool,
    job_id: &str,
    delivery_attempts: Option<i64>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE jobs SET last_lease_extended_at = now() - interval '2 minutes', \
         lease_expires_at = now() - interval '1 minute', \
         delivery_attempts = COALESCE($2, delivery_attempts) WHERE id = $1",
    )
    .bind(job_id)
    .bind(delivery_attempts)
    .execute(pool)
    .await?;
    Ok(())
}

#[sqlx::test(migrator = "run_anywhere_repository::MIGRATOR")]
async fn matcher_excludes_every_ineligible_worker(pool: PgPool) -> TestResult {
    let fixture = Fixture::new(&pool).await?;
    let eligible = register_worker(
        &fixture.repository,
        "wrk_match_eligible",
        vec![RuntimeKind::AndroidEmulatorContainer],
        true,
        HostArch::X86_64,
        1,
    )
    .await?;
    register_worker(
        &fixture.repository,
        "wrk_match_no_kvm",
        vec![RuntimeKind::AndroidEmulatorContainer],
        false,
        HostArch::X86_64,
        1,
    )
    .await?;
    register_worker(
        &fixture.repository,
        "wrk_match_runtime",
        vec![RuntimeKind::Redroid],
        true,
        HostArch::X86_64,
        1,
    )
    .await?;
    register_worker(
        &fixture.repository,
        "wrk_match_arch",
        vec![RuntimeKind::AndroidEmulatorContainer],
        true,
        HostArch::Aarch64,
        1,
    )
    .await?;
    let offline = register_worker(
        &fixture.repository,
        "wrk_match_offline",
        vec![RuntimeKind::AndroidEmulatorContainer],
        true,
        HostArch::X86_64,
        1,
    )
    .await?;
    fixture
        .repository
        .set_worker_state(&offline, WorkerState::Offline)
        .await?;
    let full = register_worker(
        &fixture.repository,
        "wrk_match_full",
        vec![RuntimeKind::AndroidEmulatorContainer],
        true,
        HostArch::X86_64,
        1,
    )
    .await?;
    sqlx::query("UPDATE workers SET active_jobs = capacity WHERE id = $1")
        .bind(full.as_str())
        .execute(&pool)
        .await?;
    let stale = register_worker(
        &fixture.repository,
        "wrk_match_stale",
        vec![RuntimeKind::AndroidEmulatorContainer],
        true,
        HostArch::X86_64,
        1,
    )
    .await?;
    sqlx::query(
        "UPDATE workers SET last_heartbeat_at = now() - interval '5 minutes' WHERE id = $1",
    )
    .bind(stale.as_str())
    .execute(&pool)
    .await?;

    let matches = fixture
        .repository
        .find_workers_matching(
            &fixture.profile,
            IsolationTier::VmIsolated,
            Utc::now() - Duration::minutes(2),
            50,
        )
        .await?;
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].worker_id, eligible);

    let redroid = fixture
        .repository
        .get_runtime_profile(&RuntimeProfileId::new(REDROID_PROFILE)?)
        .await?
        .expect("seeded redroid profile");
    assert!(
        fixture
            .repository
            .find_workers_matching(
                &redroid,
                IsolationTier::VmIsolated,
                Utc::now() - Duration::minutes(2),
                50,
            )
            .await?
            .is_empty(),
        "shared-kernel profile cannot satisfy a VM-isolated job"
    );
    Ok(())
}

#[sqlx::test(migrator = "run_anywhere_repository::MIGRATOR")]
async fn events_are_ordered_cursor_readable_and_append_only(pool: PgPool) -> TestResult {
    let fixture = Fixture::new(&pool).await?;
    let job = fixture.create_job("event-job").await?.job;
    let left_repository = fixture.repository.clone();
    let right_repository = fixture.repository.clone();
    let left_job_id = job.id.clone();
    let right_job_id = job.id.clone();
    let (left, right) = tokio::join!(
        left_repository.append_job_event(
            &left_job_id,
            "job.custom_left",
            None,
            BTreeMap::from([("side".to_owned(), json!("left"))]),
        ),
        right_repository.append_job_event(
            &right_job_id,
            "job.custom_right",
            None,
            BTreeMap::from([("side".to_owned(), json!("right"))]),
        )
    );
    left?;
    right?;

    let events = fixture
        .repository
        .list_job_events_after(&job.id, 0, 10)
        .await?;
    assert_eq!(events.len(), 3);
    assert!(
        events
            .windows(2)
            .all(|pair| pair[0].sequence < pair[1].sequence)
    );
    let after_first = fixture
        .repository
        .list_job_events_after(&job.id, events[0].sequence, 10)
        .await?;
    assert_eq!(after_first, events[1..]);

    let mutation_error = sqlx::query("UPDATE job_events SET event_type = 'tampered' WHERE id = $1")
        .bind(events[0].id.as_str())
        .execute(&pool)
        .await
        .expect_err("events are immutable");
    assert_append_only_error(&mutation_error);
    let deletion_error = sqlx::query("DELETE FROM job_events WHERE id = $1")
        .bind(events[0].id.as_str())
        .execute(&pool)
        .await
        .expect_err("events cannot be deleted");
    assert_append_only_error(&deletion_error);
    Ok(())
}

#[sqlx::test(migrator = "run_anywhere_repository::MIGRATOR")]
async fn api_keys_are_hashed_redacted_touchable_and_revocable(pool: PgPool) -> TestResult {
    let repository = Repository::new(pool.clone());
    let project = repository.create_project("auth project", "owner").await?;
    let created = repository
        .create_api_key(
            &project.id,
            vec![AuthScope::ProjectRead, AuthScope::ProjectWrite],
        )
        .await?;
    let plaintext = created.key.expose_secret().to_owned();
    assert!(plaintext.starts_with("raa_sk_"));
    assert!(!format!("{created:?}").contains(&plaintext));

    let hash = Repository::hash_api_key(&plaintext);
    let found = repository
        .find_api_key_by_hash(hash)
        .await?
        .expect("hash lookup finds key");
    assert_eq!(found.id, created.record.id);
    assert!(found.last_used_at.is_none());
    let touched = repository.touch_api_key_last_used(&found.id).await?;
    assert!(touched.last_used_at.is_some());
    let revoked = repository.revoke_api_key(&found.id).await?;
    assert!(revoked.revoked_at.is_some());
    assert!(matches!(
        repository.touch_api_key_last_used(&found.id).await,
        Err(RepositoryError::NotFound { .. })
    ));

    let hash_length: i32 =
        sqlx::query_scalar("SELECT octet_length(key_hash) FROM api_keys WHERE id = $1")
            .bind(&found.id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(hash_length, 32);
    let columns: Vec<String> = sqlx::query_scalar(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'api_keys'",
    )
    .fetch_all(&pool)
    .await?;
    assert!(!columns.iter().any(|column| column.contains("plaintext")));
    Ok(())
}

#[sqlx::test(migrator = "run_anywhere_repository::MIGRATOR")]
async fn artifact_debug_webhook_and_audit_guards_hold(pool: PgPool) -> TestResult {
    let fixture = Fixture::new(&pool).await?;
    let job = fixture.create_job("misc-job").await?.job;

    let first_artifact = fixture
        .repository
        .add_artifact(
            &job.id,
            ArtifactKind::Logcat,
            "jobs/misc/logcat.txt",
            Some("logcat.txt".to_owned()),
            128,
            digest('b'),
        )
        .await?;
    let repeated_artifact = fixture
        .repository
        .add_artifact(
            &job.id,
            ArtifactKind::Logcat,
            "jobs/misc/logcat.txt",
            Some("logcat.txt".to_owned()),
            128,
            digest('b'),
        )
        .await?;
    assert_eq!(first_artifact, repeated_artifact);
    assert_eq!(fixture.repository.list_artifacts(&job.id).await?.len(), 1);
    assert!(matches!(
        fixture
            .repository
            .add_artifact(
                &job.id,
                ArtifactKind::Logcat,
                "jobs/misc/logcat.txt",
                Some("logcat.txt".to_owned()),
                129,
                digest('b'),
            )
            .await,
        Err(RepositoryError::Conflict(_))
    ));

    let expired = fixture
        .repository
        .create_debug_session(
            &job.id,
            "jti-expired",
            "debugger",
            DebugSessionMode::Viewer,
            Utc::now() + Duration::hours(1),
        )
        .await?;
    let ended = fixture
        .repository
        .create_debug_session(
            &job.id,
            "jti-ended",
            "debugger",
            DebugSessionMode::Controller,
            Utc::now() + Duration::hours(1),
        )
        .await?;
    fixture.repository.end_debug_session(&ended.id).await?;
    for session_id in [&expired.id, &ended.id] {
        sqlx::query(
            "UPDATE debug_sessions SET created_at = now() - interval '10 minutes', \
             expires_at = now() - interval '5 minutes' WHERE id = $1",
        )
        .bind(session_id.as_str())
        .execute(&pool)
        .await?;
    }
    let unexpired = fixture
        .repository
        .create_debug_session(
            &job.id,
            "jti-unexpired",
            "debugger",
            DebugSessionMode::Viewer,
            Utc::now() + Duration::hours(1),
        )
        .await?;
    let expired_sessions = fixture
        .repository
        .find_expired_debug_sessions(Utc::now(), 10)
        .await?;
    assert_eq!(expired_sessions.len(), 1);
    assert_eq!(expired_sessions[0].id, expired.id);
    assert_ne!(expired_sessions[0].id, unexpired.id);

    let webhook_request = CreateWebhookRequest {
        project_id: fixture.project_id.clone(),
        url: Uri::new("https://hooks.example.test/jobs")?,
        events: vec![WebhookEvent::JobStateChanged],
    };
    let webhook = fixture
        .repository
        .create_webhook(webhook_request.clone())
        .await?;
    assert_eq!(
        fixture
            .repository
            .list_active_webhooks(&fixture.project_id, Some(WebhookEvent::JobStateChanged),)
            .await?,
        vec![webhook.clone()]
    );
    assert!(matches!(
        fixture.repository.create_webhook(webhook_request).await,
        Err(RepositoryError::Conflict(_))
    ));
    assert!(
        !fixture
            .repository
            .deactivate_webhook(&webhook.id)
            .await?
            .active
    );
    assert!(
        fixture
            .repository
            .list_active_webhooks(&fixture.project_id, None)
            .await?
            .is_empty()
    );

    let audit = fixture
        .repository
        .append_audit(
            "integration-test",
            "job.inspected",
            job.id.to_string(),
            BTreeMap::from([("source".to_owned(), json!("test"))]),
        )
        .await?;
    let mutation_error = sqlx::query("UPDATE audit_log SET action = 'tampered' WHERE id = $1")
        .bind(audit.id)
        .execute(&pool)
        .await
        .expect_err("audit entries are immutable");
    assert_append_only_error(&mutation_error);
    let deletion_error = sqlx::query("DELETE FROM audit_log WHERE id = $1")
        .bind(audit.id)
        .execute(&pool)
        .await
        .expect_err("audit entries cannot be deleted");
    assert_append_only_error(&deletion_error);
    Ok(())
}

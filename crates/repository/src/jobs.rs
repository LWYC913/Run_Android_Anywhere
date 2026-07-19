use std::collections::BTreeMap;

use chrono::{DateTime, Duration, Utc};
use run_anywhere_contracts::{
    AndroidAbi, ArtifactKind, CreateJobRequest, ErrorCode, FailureDetail, HostArch, IsolationTier,
    Job, JobClaim, JobEvent, JobId, JobOutcome, JobPage, JobResult, JobState, LeaseId, RuntimeKind,
    RuntimeProfile, Sha256, TransitionEvidence, UploadKind, WorkerId, validate_transition,
};
use serde_json::{Value, json};
use sqlx::{Postgres, QueryBuilder};

use crate::{
    CreatedJob, JobCursor, JobListQuery, LeaseGuard, RecoveryDisposition, Repository,
    RepositoryError, RepositoryResult, StaleJob, StaleJobCriteria, StoredArtifact,
    auth::new_id,
    codec::{checked_i64, decode_enum, encode_enum, encode_json, to_u32},
    rows::{ArtifactRow, JobEventRow, JobRow, RuntimeProfileRow, WorkerRow},
};

const JOB_COLUMNS: &str = "id, project_id, apk_upload_id, test_upload_id, runtime_profile_id, \
    worker_id, mode, min_isolation, automation, requested_artifacts, timeout_seconds, \
    idempotency_key, state, pending_outcome, outcome, failure, artifacts_finalized, \
    cleanup_completed, lease_id, lease_expires_at, last_lease_extended_at, delivery_attempts, \
    created_at, started_at, finished_at";

const PROFILE_COLUMNS: &str =
    "id, android_api, device_profile, abi, host_arch, runtime_kind, image_ref, isolation_tier";

impl Repository {
    pub async fn create_job(
        &self,
        request: CreateJobRequest,
        idempotency_key: impl Into<String>,
    ) -> RepositoryResult<CreatedJob> {
        let idempotency_key = idempotency_key.into();
        if idempotency_key.is_empty()
            || idempotency_key.len() > 255
            || !idempotency_key
                .bytes()
                .all(|byte| (0x21..=0x7e).contains(&byte))
        {
            return Err(RepositoryError::Validation(
                "idempotency key must be 1..=255 visible ASCII bytes".to_owned(),
            ));
        }
        let mut tx = self.pool.begin().await?;

        // Serialize a key before validating its request body. This gives concurrent
        // retries the same semantics as later retries: once a winner exists, its
        // original job is returned regardless of changes in the retry body.
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))")
            .bind(request.project_id.as_str())
            .bind(&idempotency_key)
            .fetch_one(&mut *tx)
            .await?;
        let select_existing = format!(
            "SELECT {JOB_COLUMNS} FROM jobs WHERE project_id = $1 AND idempotency_key = $2"
        );
        if let Some(row) = sqlx::query_as::<_, JobRow>(&select_existing)
            .bind(request.project_id.as_str())
            .bind(&idempotency_key)
            .fetch_optional(&mut *tx)
            .await?
        {
            tx.commit().await?;
            return Ok(CreatedJob {
                job: row.into_job()?,
                was_created: false,
            });
        }

        if request.test_upload_id.as_ref() == Some(&request.apk_upload_id) {
            return Err(RepositoryError::Validation(
                "APK and test uploads must be different".to_owned(),
            ));
        }

        require_upload_kind(
            &mut tx,
            request.project_id.as_str(),
            request.apk_upload_id.as_str(),
            UploadKind::Apk,
        )
        .await?;
        if let Some(test_upload_id) = request.test_upload_id.as_ref() {
            require_upload_kind(
                &mut tx,
                request.project_id.as_str(),
                test_upload_id.as_str(),
                UploadKind::Test,
            )
            .await?;
        }

        let profile = load_profile(&mut tx, request.runtime_profile.as_str())
            .await?
            .ok_or_else(|| {
                RepositoryError::not_found("runtime profile", request.runtime_profile.as_str())
            })?;
        if !profile.isolation_tier.satisfies(request.min_isolation) {
            return Err(RepositoryError::Validation(format!(
                "runtime profile {} does not satisfy {:?} isolation",
                profile.id, request.min_isolation
            )));
        }

        let id = new_id("job_");
        let mode = encode_enum(request.mode)?;
        let min_isolation = encode_enum(request.min_isolation)?;
        let automation = encode_json("automation", &request.automation)?;
        let artifacts = encode_json("requested_artifacts", request.artifacts)?;
        let timeout = checked_i64("timeout_seconds", request.timeout_seconds.get())?;
        let insert = format!(
            "INSERT INTO jobs (id, project_id, apk_upload_id, test_upload_id, runtime_profile_id, \
             mode, min_isolation, automation, requested_artifacts, timeout_seconds, idempotency_key) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) \
             ON CONFLICT (project_id, idempotency_key) DO NOTHING RETURNING {JOB_COLUMNS}"
        );
        let inserted = sqlx::query_as::<_, JobRow>(&insert)
            .bind(id)
            .bind(request.project_id.as_str())
            .bind(request.apk_upload_id.as_str())
            .bind(request.test_upload_id.as_ref().map(|id| id.as_str()))
            .bind(request.runtime_profile.as_str())
            .bind(mode)
            .bind(min_isolation)
            .bind(automation)
            .bind(artifacts)
            .bind(timeout)
            .bind(&idempotency_key)
            .fetch_optional(&mut *tx)
            .await?;

        let (row, was_created) = if let Some(row) = inserted {
            append_event(
                &mut tx,
                row.id.as_str(),
                "job.queued",
                Some(JobState::Queued),
                BTreeMap::new(),
            )
            .await?;
            (row, true)
        } else {
            let select = format!(
                "SELECT {JOB_COLUMNS} FROM jobs WHERE project_id = $1 AND idempotency_key = $2"
            );
            let row = sqlx::query_as::<_, JobRow>(&select)
                .bind(request.project_id.as_str())
                .bind(&idempotency_key)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or_else(|| {
                    RepositoryError::Conflict(
                        "idempotent job winner was not visible after conflict".to_owned(),
                    )
                })?;
            (row, false)
        };
        tx.commit().await?;
        Ok(CreatedJob {
            job: row.into_job()?,
            was_created,
        })
    }

    pub async fn get_job(&self, job_id: &JobId) -> RepositoryResult<Option<Job>> {
        let query = format!("SELECT {JOB_COLUMNS} FROM jobs WHERE id = $1");
        sqlx::query_as::<_, JobRow>(&query)
            .bind(job_id.as_str())
            .fetch_optional(&self.pool)
            .await?
            .map(JobRow::into_job)
            .transpose()
    }

    pub async fn list_jobs(&self, query: JobListQuery) -> RepositoryResult<JobPage> {
        let limit = crate::models::checked_limit(query.limit)?;
        let cursor = query.cursor.as_deref().map(JobCursor::decode).transpose()?;
        let mut builder = QueryBuilder::<Postgres>::new(format!(
            "SELECT {JOB_COLUMNS} FROM jobs WHERE project_id = "
        ));
        builder.push_bind(query.project_id.as_str());
        if let Some(state) = query.state {
            builder.push(" AND state = ");
            builder.push_bind(encode_enum(state)?);
        }
        if let Some(cursor) = cursor {
            builder.push(" AND (created_at, id) < (");
            builder.push_bind(cursor.created_at);
            builder.push(", ");
            builder.push_bind(cursor.job_id.into_inner());
            builder.push(")");
        }
        builder.push(" ORDER BY created_at DESC, id DESC LIMIT ");
        builder.push_bind(limit + 1);
        let mut rows = builder
            .build_query_as::<JobRow>()
            .fetch_all(&self.pool)
            .await?;
        let has_more = rows.len() > usize::try_from(limit).expect("positive bounded limit");
        if has_more {
            rows.pop();
        }
        let items = rows
            .iter()
            .map(JobRow::to_summary)
            .collect::<RepositoryResult<Vec<_>>>()?;
        let next_cursor = if has_more {
            items
                .last()
                .map(|item| {
                    JobCursor {
                        created_at: item.created_at,
                        job_id: item.id.clone(),
                    }
                    .encode()
                })
                .transpose()?
        } else {
            None
        };
        Ok(JobPage { items, next_cursor })
    }

    pub async fn claim_job(
        &self,
        job_id: &JobId,
        worker_id: &WorkerId,
        lease_id: &LeaseId,
        lease_expires_at: DateTime<Utc>,
    ) -> RepositoryResult<JobClaim> {
        let mut tx = self.pool.begin().await?;
        let job = lock_job(&mut tx, job_id.as_str())
            .await?
            .ok_or_else(|| RepositoryError::not_found("job", job_id.as_str()))?;
        let current_state: JobState = decode_enum("jobs.state", job.state.clone())?;
        if current_state != JobState::Queued || job.lease_id.is_some() || job.worker_id.is_some() {
            return Err(RepositoryError::CompareAndSwapLost {
                entity: "job",
                id: job_id.to_string(),
            });
        }
        let worker = lock_worker(&mut tx, worker_id.as_str())
            .await?
            .ok_or_else(|| RepositoryError::not_found("worker", worker_id.as_str()))?;
        let profile = load_profile(&mut tx, &job.runtime_profile_id)
            .await?
            .ok_or_else(|| {
                RepositoryError::not_found("runtime profile", &job.runtime_profile_id)
            })?;
        let minimum: IsolationTier = decode_enum("jobs.min_isolation", job.min_isolation.clone())?;
        let claimed_at: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&mut *tx)
            .await?;
        if lease_expires_at <= claimed_at {
            return Err(RepositoryError::Validation(
                "lease expiry must be later than database time".to_owned(),
            ));
        }
        validate_worker_match(&worker, &profile, minimum, claimed_at)?;

        let reserved = sqlx::query(
            "UPDATE workers SET active_jobs = active_jobs + 1, updated_at = now() \
             WHERE id = $1 AND state = 'online' AND active_jobs < capacity",
        )
        .bind(worker_id.as_str())
        .execute(&mut *tx)
        .await?;
        if reserved.rows_affected() != 1 {
            return Err(RepositoryError::CompareAndSwapLost {
                entity: "worker capacity",
                id: worker_id.to_string(),
            });
        }

        let update = format!(
            "UPDATE jobs SET state = 'claimed', worker_id = $2, lease_id = $3, \
             lease_expires_at = $4, last_lease_extended_at = $5, \
             delivery_attempts = delivery_attempts + 1, started_at = COALESCE(started_at, $5) \
             WHERE id = $1 AND state = 'queued' AND worker_id IS NULL AND lease_id IS NULL \
             RETURNING {JOB_COLUMNS}"
        );
        let claimed = sqlx::query_as::<_, JobRow>(&update)
            .bind(job_id.as_str())
            .bind(worker_id.as_str())
            .bind(lease_id.as_str())
            .bind(lease_expires_at)
            .bind(claimed_at)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| RepositoryError::CompareAndSwapLost {
                entity: "job",
                id: job_id.to_string(),
            })?;
        append_event(
            &mut tx,
            job_id.as_str(),
            "job.claimed",
            Some(JobState::Claimed),
            BTreeMap::from([
                ("worker_id".to_owned(), json!(worker_id)),
                ("lease_id".to_owned(), json!(lease_id)),
            ]),
        )
        .await?;
        tx.commit().await?;

        let claimed_job = claimed.into_job()?;
        Ok(JobClaim {
            job_id: claimed_job.id,
            project_id: claimed_job.project_id,
            worker_id: worker_id.clone(),
            lease_id: lease_id.clone(),
            apk_upload_id: claimed_job.apk_upload_id,
            test_upload_id: claimed_job.test_upload_id,
            runtime_profile: profile,
            mode: claimed_job.mode,
            min_isolation: claimed_job.min_isolation,
            automation: claimed_job.automation,
            artifacts: claimed_job.artifacts,
            timeout_seconds: claimed_job.timeout_seconds,
            claimed_at,
            lease_expires_at,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn transition_job_state(
        &self,
        job_id: &JobId,
        expected_from: JobState,
        to: JobState,
        evidence: TransitionEvidence,
        lease: Option<&LeaseGuard>,
        event_payload: BTreeMap<String, Value>,
    ) -> RepositoryResult<Job> {
        validate_transition(expected_from, to, &evidence)?;
        let expected = encode_enum(expected_from)?;
        let target = encode_enum(to)?;
        let pending = evidence.pending_outcome.map(encode_enum).transpose()?;
        let terminal_outcome = to.terminal_outcome().map(encode_enum).transpose()?;
        let guard_worker = lease.map(|guard| guard.worker_id.as_str());
        let guard_lease = lease.map(|guard| guard.lease_id.as_str());

        let mut tx = self.pool.begin().await?;
        let update = format!(
            "UPDATE jobs SET state = $3, pending_outcome = COALESCE($4, pending_outcome), \
             artifacts_finalized = artifacts_finalized OR $5, \
             cleanup_completed = cleanup_completed OR $6, outcome = $7, \
             finished_at = CASE WHEN $7::TEXT IS NOT NULL THEN now() ELSE finished_at END, \
             lease_id = CASE WHEN $7::TEXT IS NOT NULL THEN NULL ELSE lease_id END, \
             lease_expires_at = CASE WHEN $7::TEXT IS NOT NULL THEN NULL ELSE lease_expires_at END, \
             last_lease_extended_at = CASE WHEN $7::TEXT IS NOT NULL THEN NULL ELSE last_lease_extended_at END \
             WHERE id = $1 AND state = $2 \
             AND ($4::TEXT IS NULL OR pending_outcome IS NULL OR pending_outcome = $4) \
             AND ($8::TEXT IS NULL OR (worker_id = $8 AND lease_id = $9 \
                 AND lease_expires_at > statement_timestamp())) \
             RETURNING {JOB_COLUMNS}"
        );
        let row = sqlx::query_as::<_, JobRow>(&update)
            .bind(job_id.as_str())
            .bind(expected)
            .bind(&target)
            .bind(pending)
            .bind(evidence.artifacts_finalized)
            .bind(evidence.cleanup_completed)
            .bind(terminal_outcome)
            .bind(guard_worker)
            .bind(guard_lease)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| RepositoryError::CompareAndSwapLost {
                entity: "job",
                id: job_id.to_string(),
            })?;
        if to.is_terminal() {
            if let Some(worker_id) = row.worker_id.as_deref() {
                release_worker_capacity(&mut tx, worker_id).await?;
            }
        }
        append_event(
            &mut tx,
            job_id.as_str(),
            &format!("job.{target}"),
            Some(to),
            event_payload,
        )
        .await?;
        tx.commit().await?;
        row.into_job()
    }

    pub async fn record_job_result(&self, result: JobResult) -> RepositoryResult<Job> {
        if result.cleanup_completed && !result.artifacts_finalized {
            return Err(RepositoryError::Validation(
                "cleanup completion requires finalized artifacts".to_owned(),
            ));
        }
        if result.outcome == JobOutcome::Passed && result.error.is_some() {
            return Err(RepositoryError::Validation(
                "a passed result cannot include an error".to_owned(),
            ));
        }
        let mut tx = self.pool.begin().await?;
        let current = lock_job(&mut tx, result.job_id.as_str())
            .await?
            .ok_or_else(|| RepositoryError::not_found("job", result.job_id.as_str()))?;
        if current.worker_id.as_deref() != Some(result.worker_id.as_str())
            || current.lease_id.as_deref() != Some(result.lease_id.as_str())
        {
            return Err(RepositoryError::CompareAndSwapLost {
                entity: "job lease",
                id: result.job_id.to_string(),
            });
        }
        let state: JobState = decode_enum("jobs.state", current.state)?;
        if state.is_terminal() {
            return Err(RepositoryError::Conflict(
                "cannot record a result for a terminal job".to_owned(),
            ));
        }
        if result.cleanup_completed && !(current.artifacts_finalized || result.artifacts_finalized)
        {
            return Err(RepositoryError::Validation(
                "cleanup completion requires finalized artifacts".to_owned(),
            ));
        }
        if let Some(existing) = current.pending_outcome.as_ref() {
            let existing: JobOutcome = decode_enum("jobs.pending_outcome", existing.clone())?;
            if existing != result.outcome {
                return Err(RepositoryError::Conflict(format!(
                    "job already has pending outcome {existing:?}"
                )));
            }
        }
        verify_artifact_ids(&mut tx, &result.job_id, &result.artifact_ids).await?;
        let pending = encode_enum(result.outcome)?;
        let failure = result
            .error
            .as_ref()
            .map(|failure| encode_json("failure", failure))
            .transpose()?;
        let update = format!(
            "UPDATE jobs SET pending_outcome = $4, failure = COALESCE($5, failure), \
             artifacts_finalized = artifacts_finalized OR $6, \
             cleanup_completed = cleanup_completed OR $7 \
             WHERE id = $1 AND worker_id = $2 AND lease_id = $3 \
             AND lease_expires_at > statement_timestamp() \
             AND (pending_outcome IS NULL OR pending_outcome = $4) \
             AND state NOT IN ('passed','failed','cancelled','timed_out','infra_failed') \
             RETURNING {JOB_COLUMNS}"
        );
        let row = sqlx::query_as::<_, JobRow>(&update)
            .bind(result.job_id.as_str())
            .bind(result.worker_id.as_str())
            .bind(result.lease_id.as_str())
            .bind(pending)
            .bind(failure)
            .bind(result.artifacts_finalized)
            .bind(result.cleanup_completed)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| RepositoryError::CompareAndSwapLost {
                entity: "job result",
                id: result.job_id.to_string(),
            })?;
        append_event(
            &mut tx,
            result.job_id.as_str(),
            "job.result_recorded",
            Some(state),
            BTreeMap::from([
                ("outcome".to_owned(), json!(result.outcome)),
                ("completed_at".to_owned(), json!(result.completed_at)),
                ("artifact_ids".to_owned(), json!(result.artifact_ids)),
            ]),
        )
        .await?;
        tx.commit().await?;
        row.into_job()
    }

    pub async fn find_stale_jobs(
        &self,
        criteria: StaleJobCriteria,
    ) -> RepositoryResult<Vec<StaleJob>> {
        let limit = crate::models::checked_limit(criteria.limit)?;
        let rows = sqlx::query_as::<_, StaleJobRow>(
            "SELECT jobs.id, jobs.worker_id, jobs.lease_id, jobs.state, \
             jobs.delivery_attempts, jobs.lease_expires_at, \
             workers.last_heartbeat_at AS worker_last_heartbeat_at, \
             $1::TIMESTAMPTZ AS lease_expired_before, \
             $2::TIMESTAMPTZ AS worker_heartbeat_before \
             FROM jobs JOIN workers ON workers.id = jobs.worker_id \
             WHERE jobs.lease_id IS NOT NULL \
             AND jobs.state NOT IN ('queued','passed','failed','cancelled','timed_out','infra_failed') \
             AND (jobs.lease_expires_at <= $1 OR workers.last_heartbeat_at <= $2) \
             ORDER BY LEAST(jobs.lease_expires_at, workers.last_heartbeat_at), jobs.id LIMIT $3",
        )
        .bind(criteria.lease_expired_before)
        .bind(criteria.worker_heartbeat_before)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(StaleJobRow::try_into_model).collect()
    }

    pub async fn recover_stale_job(
        &self,
        stale: &StaleJob,
        max_deliver: u32,
    ) -> RepositoryResult<RecoveryDisposition> {
        if max_deliver == 0 {
            return Err(RepositoryError::Validation(
                "max_deliver must be positive".to_owned(),
            ));
        }
        let mut tx = self.pool.begin().await?;
        let Some(current) = lock_job(&mut tx, stale.job_id.as_str()).await? else {
            tx.rollback().await?;
            return Ok(RecoveryDisposition::LostRace);
        };
        let current_state: JobState = decode_enum("jobs.state", current.state.clone())?;
        let snapshot_matches = current.worker_id.as_deref() == Some(stale.worker_id.as_str())
            && current.lease_id.as_deref() == Some(stale.lease_id.as_str())
            && current_state == stale.state
            && to_u32("jobs.delivery_attempts", current.delivery_attempts)?
                == stale.delivery_attempts
            && current.lease_expires_at.as_ref() == Some(&stale.lease_expires_at);
        if !snapshot_matches || current_state == JobState::Queued || current_state.is_terminal() {
            tx.rollback().await?;
            return Ok(RecoveryDisposition::LostRace);
        }
        let Some(worker) = lock_worker(&mut tx, stale.worker_id.as_str()).await? else {
            tx.rollback().await?;
            return Ok(RecoveryDisposition::LostRace);
        };
        let lease_expired = stale.lease_expires_at <= stale.lease_expired_before;
        let worker_stale = worker.last_heartbeat_at <= stale.worker_heartbeat_before;
        if !lease_expired && !worker_stale {
            tx.rollback().await?;
            return Ok(RecoveryDisposition::LostRace);
        }

        let requeue = stale.delivery_attempts < max_deliver;
        let failure = if requeue {
            None
        } else {
            Some(encode_json(
                "failure",
                FailureDetail {
                    code: ErrorCode::InfraFailed,
                    message: "job lease was abandoned after maximum delivery attempts".to_owned(),
                },
            )?)
        };
        let update = if requeue {
            format!(
                "UPDATE jobs SET state = 'queued', worker_id = NULL, lease_id = NULL, \
                 lease_expires_at = NULL, last_lease_extended_at = NULL, pending_outcome = NULL, \
                 outcome = NULL, failure = NULL, artifacts_finalized = FALSE, cleanup_completed = FALSE \
                 WHERE id = $1 AND worker_id = $2 AND lease_id = $3 \
                 AND lease_expires_at = $4 AND state = $5 AND delivery_attempts = $6 \
                 RETURNING {JOB_COLUMNS}"
            )
        } else {
            format!(
                "UPDATE jobs SET state = CASE WHEN state = 'cleaning_up' THEN state ELSE 'collecting_artifacts' END, \
                 worker_id = NULL, lease_id = NULL, lease_expires_at = NULL, \
                 last_lease_extended_at = NULL, pending_outcome = 'infra_failed', outcome = NULL, \
                 failure = $7, artifacts_finalized = CASE WHEN state = 'cleaning_up' THEN artifacts_finalized ELSE FALSE END, \
                 cleanup_completed = CASE WHEN state = 'cleaning_up' THEN cleanup_completed ELSE FALSE END \
                 WHERE id = $1 AND worker_id = $2 AND lease_id = $3 \
                 AND lease_expires_at = $4 AND state = $5 AND delivery_attempts = $6 \
                 RETURNING {JOB_COLUMNS}"
            )
        };
        let mut query = sqlx::query_as::<_, JobRow>(&update)
            .bind(stale.job_id.as_str())
            .bind(stale.worker_id.as_str())
            .bind(stale.lease_id.as_str())
            .bind(stale.lease_expires_at)
            .bind(encode_enum(stale.state)?)
            .bind(i64::from(stale.delivery_attempts));
        if !requeue {
            query = query.bind(failure);
        }
        let Some(row) = query.fetch_optional(&mut *tx).await? else {
            tx.rollback().await?;
            return Ok(RecoveryDisposition::LostRace);
        };
        release_worker_capacity(&mut tx, stale.worker_id.as_str()).await?;
        let new_state = if requeue {
            JobState::Queued
        } else if stale.state == JobState::CleaningUp {
            JobState::CleaningUp
        } else {
            JobState::CollectingArtifacts
        };
        append_event(
            &mut tx,
            stale.job_id.as_str(),
            if requeue {
                "job.recovered_requeued"
            } else {
                "job.recovery_exhausted"
            },
            Some(new_state),
            BTreeMap::from([
                (
                    "delivery_attempts".to_owned(),
                    json!(stale.delivery_attempts),
                ),
                ("lease_expired".to_owned(), json!(lease_expired)),
                ("worker_stale".to_owned(), json!(worker_stale)),
            ]),
        )
        .await?;
        tx.commit().await?;
        let job = row.into_job()?;
        Ok(if requeue {
            RecoveryDisposition::Requeued(job)
        } else {
            RecoveryDisposition::Finalizing(job)
        })
    }

    pub async fn append_job_event(
        &self,
        job_id: &JobId,
        event_type: impl Into<String>,
        state: Option<JobState>,
        payload: BTreeMap<String, Value>,
    ) -> RepositoryResult<JobEvent> {
        let event_type = event_type.into();
        let mut tx = self.pool.begin().await?;
        let event = append_event(&mut tx, job_id.as_str(), &event_type, state, payload).await?;
        tx.commit().await?;
        Ok(event)
    }

    pub async fn list_job_events_after(
        &self,
        job_id: &JobId,
        after_sequence: u64,
        limit: u32,
    ) -> RepositoryResult<Vec<JobEvent>> {
        let after_sequence = checked_i64("after_sequence", after_sequence)?;
        let limit = crate::models::checked_limit(limit)?;
        sqlx::query_as::<_, JobEventRow>(
            "SELECT id, sequence, job_id, timestamp, event_type, state, payload \
             FROM job_events WHERE job_id = $1 AND sequence > $2 \
             ORDER BY sequence LIMIT $3",
        )
        .bind(job_id.as_str())
        .bind(after_sequence)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(TryInto::try_into)
        .collect()
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn add_artifact(
        &self,
        job_id: &JobId,
        kind: ArtifactKind,
        s3_key: impl Into<String>,
        file_name: Option<String>,
        size_bytes: u64,
        sha256: Sha256,
    ) -> RepositoryResult<StoredArtifact> {
        let s3_key = s3_key.into();
        if s3_key.trim().is_empty() {
            return Err(RepositoryError::Validation(
                "artifact object key must not be blank".to_owned(),
            ));
        }
        if file_name
            .as_ref()
            .is_some_and(|name| name.trim().is_empty())
        {
            return Err(RepositoryError::Validation(
                "artifact file name must not be blank".to_owned(),
            ));
        }
        let kind_wire = encode_enum(kind)?;
        let size = checked_i64("size_bytes", size_bytes)?;
        let row = sqlx::query_as::<_, ArtifactRow>(
            "INSERT INTO artifacts (id, job_id, kind, s3_key, file_name, size_bytes, sha256) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (job_id, s3_key) DO NOTHING \
             RETURNING id, job_id, kind, s3_key, file_name, size_bytes, sha256, created_at",
        )
        .bind(new_id("art_"))
        .bind(job_id.as_str())
        .bind(&kind_wire)
        .bind(&s3_key)
        .bind(&file_name)
        .bind(size)
        .bind(sha256.as_str())
        .fetch_optional(&self.pool)
        .await?;
        let stored: StoredArtifact = match row {
            Some(row) => row.try_into()?,
            None => {
                let row = sqlx::query_as::<_, ArtifactRow>(
                    "SELECT id, job_id, kind, s3_key, file_name, size_bytes, sha256, created_at \
                     FROM artifacts WHERE job_id = $1 AND s3_key = $2",
                )
                .bind(job_id.as_str())
                .bind(&s3_key)
                .fetch_one(&self.pool)
                .await?;
                row.try_into()?
            }
        };
        if stored.artifact.kind != kind
            || stored.artifact.file_name != file_name
            || stored.artifact.size_bytes != size_bytes
            || stored.artifact.sha256 != sha256
        {
            return Err(RepositoryError::Conflict(
                "artifact object key was already recorded with different metadata".to_owned(),
            ));
        }
        Ok(stored)
    }

    pub async fn list_artifacts(&self, job_id: &JobId) -> RepositoryResult<Vec<StoredArtifact>> {
        sqlx::query_as::<_, ArtifactRow>(
            "SELECT id, job_id, kind, s3_key, file_name, size_bytes, sha256, created_at \
             FROM artifacts WHERE job_id = $1 ORDER BY created_at, id",
        )
        .bind(job_id.as_str())
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(TryInto::try_into)
        .collect()
    }
}

#[derive(Debug, sqlx::FromRow)]
struct StaleJobRow {
    id: String,
    worker_id: String,
    lease_id: String,
    state: String,
    delivery_attempts: i64,
    lease_expires_at: DateTime<Utc>,
    worker_last_heartbeat_at: DateTime<Utc>,
    lease_expired_before: DateTime<Utc>,
    worker_heartbeat_before: DateTime<Utc>,
}

impl StaleJobRow {
    fn try_into_model(self) -> RepositoryResult<StaleJob> {
        Ok(StaleJob {
            job_id: JobId::new(self.id)
                .map_err(|error| RepositoryError::decode("jobs.id", error))?,
            worker_id: WorkerId::new(self.worker_id)
                .map_err(|error| RepositoryError::decode("jobs.worker_id", error))?,
            lease_id: LeaseId::new(self.lease_id)
                .map_err(|error| RepositoryError::decode("jobs.lease_id", error))?,
            state: decode_enum("jobs.state", self.state)?,
            delivery_attempts: to_u32("jobs.delivery_attempts", self.delivery_attempts)?,
            lease_expires_at: self.lease_expires_at,
            worker_last_heartbeat_at: self.worker_last_heartbeat_at,
            lease_expired_before: self.lease_expired_before,
            worker_heartbeat_before: self.worker_heartbeat_before,
        })
    }
}

async fn require_upload_kind(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    project_id: &str,
    upload_id: &str,
    expected: UploadKind,
) -> RepositoryResult<()> {
    let kind: Option<String> =
        sqlx::query_scalar("SELECT kind FROM uploads WHERE project_id = $1 AND id = $2")
            .bind(project_id)
            .bind(upload_id)
            .fetch_optional(&mut **tx)
            .await?;
    let Some(kind) = kind else {
        return Err(RepositoryError::Validation(format!(
            "upload {upload_id} does not belong to project {project_id}"
        )));
    };
    let actual: UploadKind = decode_enum("uploads.kind", kind)?;
    if actual != expected {
        return Err(RepositoryError::Validation(format!(
            "upload {upload_id} must be {expected:?}, found {actual:?}"
        )));
    }
    Ok(())
}

async fn load_profile(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    profile_id: &str,
) -> RepositoryResult<Option<RuntimeProfile>> {
    let query = format!("SELECT {PROFILE_COLUMNS} FROM runtime_profiles WHERE id = $1");
    sqlx::query_as::<_, RuntimeProfileRow>(&query)
        .bind(profile_id)
        .fetch_optional(&mut **tx)
        .await?
        .map(TryInto::try_into)
        .transpose()
}

async fn lock_job(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    job_id: &str,
) -> RepositoryResult<Option<JobRow>> {
    let query = format!("SELECT {JOB_COLUMNS} FROM jobs WHERE id = $1 FOR UPDATE");
    Ok(sqlx::query_as::<_, JobRow>(&query)
        .bind(job_id)
        .fetch_optional(&mut **tx)
        .await?)
}

async fn lock_worker(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    worker_id: &str,
) -> RepositoryResult<Option<WorkerRow>> {
    Ok(sqlx::query_as::<_, WorkerRow>(
        "SELECT id, runtimes, kvm, gpu, arch, capacity, active_jobs, state, \
         last_heartbeat_at, reported_last_seen_at, registered_at, updated_at \
         FROM workers WHERE id = $1 FOR UPDATE",
    )
    .bind(worker_id)
    .fetch_optional(&mut **tx)
    .await?)
}

fn validate_worker_match(
    worker: &WorkerRow,
    profile: &RuntimeProfile,
    minimum: IsolationTier,
    database_now: DateTime<Utc>,
) -> RepositoryResult<()> {
    if worker.state != "online" {
        return Err(RepositoryError::Conflict("worker is not online".to_owned()));
    }
    if worker.active_jobs >= worker.capacity {
        return Err(RepositoryError::Conflict(
            "worker has no spare capacity".to_owned(),
        ));
    }
    if worker.last_heartbeat_at < database_now - Duration::seconds(120) {
        return Err(RepositoryError::Conflict(
            "worker heartbeat is stale".to_owned(),
        ));
    }
    let runtime = encode_enum(profile.runtime_kind)?;
    if !worker.runtimes.contains(&runtime) {
        return Err(RepositoryError::Conflict(
            "worker does not support the runtime".to_owned(),
        ));
    }
    let arch: HostArch = decode_enum("workers.arch", worker.arch.clone())?;
    if arch != profile.host_arch || !abi_supported(profile.abi, arch) {
        return Err(RepositoryError::Conflict(
            "worker architecture is incompatible with the runtime profile".to_owned(),
        ));
    }
    if (profile.isolation_tier == IsolationTier::VmIsolated
        || matches!(
            profile.runtime_kind,
            RuntimeKind::AndroidEmulatorContainer | RuntimeKind::Cuttlefish
        ))
        && !worker.kvm
    {
        return Err(RepositoryError::Conflict(
            "worker lacks KVM required by the runtime".to_owned(),
        ));
    }
    if !profile.isolation_tier.satisfies(minimum) {
        return Err(RepositoryError::Conflict(
            "runtime isolation does not satisfy the job minimum".to_owned(),
        ));
    }
    Ok(())
}

const fn abi_supported(abi: AndroidAbi, arch: HostArch) -> bool {
    matches!(
        (abi, arch),
        (AndroidAbi::X86 | AndroidAbi::X86_64, HostArch::X86_64)
            | (
                AndroidAbi::ArmeabiV7a | AndroidAbi::Arm64V8a,
                HostArch::Aarch64
            )
    )
}

async fn release_worker_capacity(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    worker_id: &str,
) -> RepositoryResult<()> {
    sqlx::query(
        "UPDATE workers SET active_jobs = GREATEST(active_jobs - 1, 0), updated_at = now() \
         WHERE id = $1",
    )
    .bind(worker_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn append_event(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    job_id: &str,
    event_type: &str,
    state: Option<JobState>,
    payload: BTreeMap<String, Value>,
) -> RepositoryResult<JobEvent> {
    if event_type.trim().is_empty() {
        return Err(RepositoryError::Validation(
            "job event type must not be blank".to_owned(),
        ));
    }
    // Sequence allocation is global and PostgreSQL sequences are not commit ordered.
    // Serializing on the job row prevents an `after_sequence` consumer from observing
    // a later same-job event before an earlier transaction commits.
    let exists: Option<String> = sqlx::query_scalar("SELECT id FROM jobs WHERE id = $1 FOR UPDATE")
        .bind(job_id)
        .fetch_optional(&mut **tx)
        .await?;
    if exists.is_none() {
        return Err(RepositoryError::not_found("job", job_id));
    }
    let state = state.map(encode_enum).transpose()?;
    let payload = encode_json("job_events.payload", payload)?;
    let row = sqlx::query_as::<_, JobEventRow>(
        "INSERT INTO job_events (id, job_id, event_type, state, payload) \
         VALUES ($1, $2, $3, $4, $5) \
         RETURNING id, sequence, job_id, timestamp, event_type, state, payload",
    )
    .bind(new_id("evt_"))
    .bind(job_id)
    .bind(event_type)
    .bind(state)
    .bind(payload)
    .fetch_one(&mut **tx)
    .await?;
    row.try_into()
}

async fn verify_artifact_ids(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    job_id: &JobId,
    artifact_ids: &[run_anywhere_contracts::ArtifactId],
) -> RepositoryResult<()> {
    if artifact_ids.is_empty() {
        return Ok(());
    }
    let ids = artifact_ids
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM artifacts WHERE job_id = $1 AND id = ANY($2)")
            .bind(job_id.as_str())
            .bind(&ids)
            .fetch_one(&mut **tx)
            .await?;
    if count != i64::try_from(ids.len()).expect("artifact list length fits i64") {
        return Err(RepositoryError::Validation(
            "one or more result artifact IDs do not belong to the job".to_owned(),
        ));
    }
    Ok(())
}

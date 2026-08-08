//! Leader-only repair of stale leases, finalizers, sessions, and runtimes.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use async_nats::jetstream;
use chrono::{Duration as ChronoDuration, Utc};
use run_anywhere_contracts::{
    JobDeadLetter, JobDeadLetterReason, JobId, JobOutcome, JobState, TransitionEvidence,
};
use run_anywhere_repository::{LeaseGuard, RecoveryDisposition, Repository, StaleJobCriteria};
use serde_json::json;
use thiserror::Error;
use tokio::sync::Mutex;

use crate::{
    AckRegistry, DlqPublishOutcome, DlqPublisher, SchedulerMetrics,
    config::TimingSettings,
    dlq::{exhaustion_event_key, is_matching_exhaustion_marker},
    metrics::{DlqMetricReason, ReaperMetricResult, RecoveryMetricResult},
    reaper::{
        ReapDecision, ReapOutcome, RuntimeOwnership, RuntimeReaper, RuntimeResource, evaluate_reap,
    },
};

const RECONCILE_BATCH: u32 = 200;
const RECONCILER_ACTOR: &str = "scheduler-reconciler";
type OrphanObservations = HashMap<String, (RuntimeResource, chrono::DateTime<Utc>)>;

#[derive(Clone)]
pub struct Reconciler {
    repository: Repository,
    ack_registry: AckRegistry,
    dlq: DlqPublisher,
    job_stream: jetstream::stream::Stream,
    reaper: Arc<dyn RuntimeReaper>,
    timing: TimingSettings,
    max_run_attempts: u32,
    metrics: SchedulerMetrics,
    pending_exhausted: Arc<Mutex<HashMap<JobId, LeaseGuard>>>,
    pending_cancellations: Arc<Mutex<HashMap<JobId, LeaseGuard>>>,
    orphan_observations: Arc<Mutex<OrphanObservations>>,
}

impl std::fmt::Debug for Reconciler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Reconciler")
            .field("timing", &self.timing)
            .field("max_run_attempts", &self.max_run_attempts)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    pub requeued: u32,
    pub exhausted: u32,
    pub finalized: u32,
    pub debug_sessions_ended: u32,
    pub runtimes_reaped: u32,
}

#[derive(Debug, Error)]
pub enum ReconcilerError {
    #[error(transparent)]
    Repository(#[from] run_anywhere_repository::RepositoryError),
    #[error("reconciler SQL query failed: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    Dlq(#[from] crate::DlqError),
    #[error("runtime inventory failed: {0}")]
    Reaper(#[from] crate::ReaperError),
    #[error("database contains an invalid job ID `{0}`")]
    InvalidJobId(String),
    #[error("invalid durable exhaustion marker `{0}`")]
    InvalidExhaustionMarker(String),
}

impl Reconciler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        repository: Repository,
        ack_registry: AckRegistry,
        dlq: DlqPublisher,
        job_stream: jetstream::stream::Stream,
        reaper: Arc<dyn RuntimeReaper>,
        timing: TimingSettings,
        max_run_attempts: u32,
        metrics: SchedulerMetrics,
    ) -> Self {
        Self {
            repository,
            ack_registry,
            dlq,
            job_stream,
            reaper,
            timing,
            max_run_attempts,
            metrics,
            pending_exhausted: Arc::new(Mutex::new(HashMap::new())),
            pending_cancellations: Arc::new(Mutex::new(HashMap::new())),
            orphan_observations: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn run_once(&self) -> Result<ReconcileReport, ReconcilerError> {
        let now = Utc::now();
        let stale_threshold = ChronoDuration::from_std(self.timing.stale_worker_threshold)
            .expect("validated scheduler duration fits chrono");
        let stale_jobs = self
            .repository
            .find_stale_jobs(StaleJobCriteria {
                lease_expired_before: now,
                worker_heartbeat_before: now - stale_threshold,
                limit: RECONCILE_BATCH,
            })
            .await?;
        let mut report = ReconcileReport::default();

        for stale in stale_jobs {
            let lease = LeaseGuard {
                worker_id: stale.worker_id.clone(),
                lease_id: stale.lease_id.clone(),
            };
            match self
                .repository
                .recover_stale_job(&stale, self.max_run_attempts)
                .await?
            {
                RecoveryDisposition::Requeued(_) => {
                    report.requeued = report.requeued.saturating_add(1);
                    self.metrics.record_recovery(RecoveryMetricResult::Requeued);
                    if let Err(error) = self
                        .ack_registry
                        .nak(
                            &stale.job_id,
                            &stale.worker_id,
                            &stale.lease_id,
                            Some(std::time::Duration::from_millis(100)),
                        )
                        .await
                    {
                        // A missing binding is expected after leader failover; AckWait
                        // or the advisory path still makes the source record recoverable.
                        tracing::debug!(job_id = %stale.job_id, error = %error, "stale queue binding was not local");
                    }
                }
                RecoveryDisposition::Finalizing(_) => {
                    report.exhausted = report.exhausted.saturating_add(1);
                    self.metrics
                        .record_recovery(RecoveryMetricResult::InfraFailed);
                    self.pending_exhausted
                        .lock()
                        .await
                        .insert(stale.job_id, lease);
                }
                RecoveryDisposition::Cancelling(_) => {
                    self.pending_cancellations
                        .lock()
                        .await
                        .insert(stale.job_id, lease);
                }
                RecoveryDisposition::LostRace => {
                    self.metrics
                        .record_recovery(RecoveryMetricResult::StaleLease);
                }
            }
        }

        match self.reconcile_runtimes(now).await {
            Ok((runtime_jobs, reaped)) => {
                report.runtimes_reaped = reaped;
                report.finalized = self.finalize_ready_jobs(&runtime_jobs).await?;
            }
            Err(error) => {
                // Runtime-dependent finalization fails closed, but unrelated
                // durable recovery (sessions and the DLQ outbox) must continue.
                self.metrics.record_reaper(ReaperMetricResult::Error);
                tracing::warn!(error = %error, "runtime reconciliation failed closed");
            }
        }
        report.debug_sessions_ended = u32::try_from(
            self.repository
                .end_expired_or_terminal_debug_sessions_with_audit(
                    now,
                    RECONCILER_ACTOR,
                    RECONCILE_BATCH,
                )
                .await?
                .len(),
        )
        .unwrap_or(u32::MAX);
        self.dlq.publish_ready().await?;
        self.retire_published_exhausted().await?;
        Ok(report)
    }

    async fn reconcile_runtimes(
        &self,
        now: chrono::DateTime<Utc>,
    ) -> Result<(HashSet<JobId>, u32), ReconcilerError> {
        let resources = self.reaper.inventory().await?;
        let mut remaining_jobs = HashSet::new();
        let mut inventoried_resources = HashSet::new();
        let mut reaped = 0_u32;
        for resource in resources {
            inventoried_resources.insert(resource.resource_id.clone());
            let ownership = self.runtime_ownership(&resource).await;
            let unsafe_since = self
                .observe_runtime_ownership(&resource, ownership, now)
                .await;
            match evaluate_reap(
                &resource,
                ownership,
                unsafe_since,
                now,
                self.timing.reap_grace,
            ) {
                ReapDecision::Reap => match self.reaper.reap(&resource).await {
                    Ok(ReapOutcome::Reaped) => {
                        self.orphan_observations
                            .lock()
                            .await
                            .remove(&resource.resource_id);
                        reaped = reaped.saturating_add(1);
                        self.metrics.record_reaper(ReaperMetricResult::Reaped);
                    }
                    Ok(ReapOutcome::AlreadyAbsent) => {
                        self.orphan_observations
                            .lock()
                            .await
                            .remove(&resource.resource_id);
                        self.metrics
                            .record_reaper(ReaperMetricResult::AlreadyAbsent);
                    }
                    Ok(ReapOutcome::Skipped) => {
                        remaining_jobs.insert(resource.job_id);
                        self.metrics.record_reaper(ReaperMetricResult::Protected);
                    }
                    Err(error) => {
                        remaining_jobs.insert(resource.job_id.clone());
                        self.metrics.record_reaper(ReaperMetricResult::Error);
                        tracing::warn!(job_id = %resource.job_id, resource_id = %resource.resource_id, error = %error, "runtime reap failed closed");
                    }
                },
                ReapDecision::KeepInGrace => {
                    remaining_jobs.insert(resource.job_id);
                    self.metrics.record_reaper(ReaperMetricResult::InGrace);
                }
                ReapDecision::KeepActive | ReapDecision::KeepUnknown => {
                    remaining_jobs.insert(resource.job_id);
                    self.metrics.record_reaper(ReaperMetricResult::Protected);
                }
            }
        }
        self.orphan_observations
            .lock()
            .await
            .retain(|resource_id, _| inventoried_resources.contains(resource_id));
        Ok((remaining_jobs, reaped))
    }

    async fn observe_runtime_ownership(
        &self,
        resource: &RuntimeResource,
        ownership: RuntimeOwnership,
        now: chrono::DateTime<Utc>,
    ) -> Option<chrono::DateTime<Utc>> {
        let mut observations = self.orphan_observations.lock().await;
        match ownership {
            RuntimeOwnership::ActiveMatchingLease | RuntimeOwnership::Unknown => {
                observations.remove(&resource.resource_id);
                None
            }
            RuntimeOwnership::JobMissing
            | RuntimeOwnership::JobTerminal
            | RuntimeOwnership::LeaseMismatch => {
                let observation = observations
                    .entry(resource.resource_id.clone())
                    .and_modify(|(observed, first_seen)| {
                        if observed != resource {
                            *observed = resource.clone();
                            *first_seen = now;
                        }
                    })
                    .or_insert_with(|| (resource.clone(), now));
                Some(observation.1)
            }
        }
    }

    async fn runtime_ownership(&self, resource: &RuntimeResource) -> RuntimeOwnership {
        match self
            .repository
            .get_job_scheduling_snapshot(&resource.job_id)
            .await
        {
            Ok(None) => RuntimeOwnership::JobMissing,
            Ok(Some(snapshot)) if snapshot.job.state.is_terminal() => RuntimeOwnership::JobTerminal,
            Ok(Some(snapshot))
                if snapshot.lease.as_ref().is_some_and(|lease| {
                    lease.worker_id == resource.worker_id && lease.lease_id == resource.lease_id
                }) =>
            {
                RuntimeOwnership::ActiveMatchingLease
            }
            Ok(Some(_)) => RuntimeOwnership::LeaseMismatch,
            Err(error) => {
                tracing::warn!(job_id = %resource.job_id, error = %error, "runtime ownership lookup failed closed");
                RuntimeOwnership::Unknown
            }
        }
    }

    async fn finalize_ready_jobs(
        &self,
        runtime_jobs: &HashSet<JobId>,
    ) -> Result<u32, ReconcilerError> {
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM jobs \
             WHERE state IN ('collecting_artifacts', 'cleaning_up') \
             AND pending_outcome IN ('cancelled', 'infra_failed') \
             ORDER BY created_at, id LIMIT $1",
        )
        .bind(i64::from(RECONCILE_BATCH))
        .fetch_all(self.repository.pool())
        .await?;
        let mut finalized = 0_u32;
        for raw_id in ids {
            let job_id =
                JobId::new(raw_id.clone()).map_err(|_| ReconcilerError::InvalidJobId(raw_id))?;
            if runtime_jobs.contains(&job_id) {
                continue;
            }
            let Some(mut snapshot) = self.repository.get_job_scheduling_snapshot(&job_id).await?
            else {
                continue;
            };
            // A worker-owned finalizer remains fenced to that live lease. Only
            // stale recovery or the worker may clear it before the leader uses
            // the lease-free recovery path below.
            if snapshot.lease.is_some() {
                continue;
            }
            let Some(outcome @ (JobOutcome::Cancelled | JobOutcome::InfraFailed)) =
                snapshot.pending_outcome
            else {
                continue;
            };
            let evidence = TransitionEvidence {
                pending_outcome: Some(outcome),
                artifacts_finalized: true,
                cleanup_completed: false,
            };
            if snapshot.job.state == JobState::CollectingArtifacts {
                self.repository
                    .transition_job_state(
                        &job_id,
                        JobState::CollectingArtifacts,
                        JobState::CleaningUp,
                        evidence,
                        None,
                        BTreeMap::from([(
                            "finalizer".to_owned(),
                            json!("confirmed_empty_or_unrecoverable"),
                        )]),
                    )
                    .await?;
                snapshot = self
                    .repository
                    .get_job_scheduling_snapshot(&job_id)
                    .await?
                    .expect("transitioned job still exists");
            }
            if snapshot.job.state != JobState::CleaningUp {
                continue;
            }
            let terminal = match outcome {
                JobOutcome::Cancelled => JobState::Cancelled,
                JobOutcome::InfraFailed => JobState::InfraFailed,
                _ => unreachable!("outcome pattern is restricted above"),
            };
            self.repository
                .transition_job_state(
                    &job_id,
                    JobState::CleaningUp,
                    terminal,
                    TransitionEvidence {
                        pending_outcome: Some(outcome),
                        artifacts_finalized: true,
                        cleanup_completed: true,
                    },
                    None,
                    BTreeMap::from([("cleanup".to_owned(), json!("reconciled"))]),
                )
                .await?;
            finalized = finalized.saturating_add(1);

            if outcome == JobOutcome::Cancelled {
                self.metrics
                    .record_recovery(RecoveryMetricResult::Cancelled);
                if let Some(lease) = self
                    .pending_cancellations
                    .lock()
                    .await
                    .get(&job_id)
                    .cloned()
                {
                    match self
                        .ack_registry
                        .confirm_ack(&job_id, &lease.worker_id, &lease.lease_id)
                        .await
                    {
                        Ok(()) | Err(crate::AckRegistryError::Missing(_)) => {
                            self.pending_cancellations.lock().await.remove(&job_id);
                        }
                        Err(error) => tracing::warn!(
                            job_id = %job_id,
                            error = %error,
                            "cancelled job queue acknowledgement remains pending"
                        ),
                    }
                }
                continue;
            }
            let dead_letter = JobDeadLetter::new(
                Some(snapshot.job.id.clone()),
                Some(snapshot.job.project_id.clone()),
                None,
                snapshot.delivery_attempts.max(1),
                JobDeadLetterReason::AttemptsExhausted,
                Utc::now(),
            )
            .expect("database attempt and job ID form a valid dead letter");
            let publish_outcome = self
                .dlq
                .ensure_published(&dead_letter, BTreeMap::new())
                .await?;
            if publish_outcome == DlqPublishOutcome::NewlyPublished {
                self.metrics.record_dlq(DlqMetricReason::AttemptsExhausted);
            }
            let lease = self.pending_exhausted.lock().await.remove(&job_id);
            if let Some(lease) = lease {
                if let Ok(binding) = self
                    .ack_registry
                    .take(&job_id, &lease.worker_id, &lease.lease_id)
                    .await
                {
                    self.dlq.retire_delivery(&self.job_stream, &binding).await?;
                }
            }
        }
        Ok(finalized)
    }

    async fn retire_published_exhausted(&self) -> Result<(), ReconcilerError> {
        let pending = self.pending_exhausted.lock().await.clone();
        for (job_id, lease) in pending {
            let Some(snapshot) = self.repository.get_job_scheduling_snapshot(&job_id).await? else {
                continue;
            };
            if snapshot.job.state != JobState::InfraFailed {
                continue;
            }
            let event_key = exhaustion_event_key(&job_id, snapshot.delivery_attempts);
            let Some(marker) = self.repository.get_outbox_message(&event_key).await? else {
                continue;
            };
            if !is_matching_exhaustion_marker(&marker, &job_id, snapshot.delivery_attempts) {
                return Err(ReconcilerError::InvalidExhaustionMarker(event_key));
            }
            if marker.published_at.is_none() {
                continue;
            }
            if let Ok(binding) = self
                .ack_registry
                .take(&job_id, &lease.worker_id, &lease.lease_id)
                .await
            {
                self.dlq.retire_delivery(&self.job_stream, &binding).await?;
            }
            self.pending_exhausted.lock().await.remove(&job_id);
        }
        Ok(())
    }
}

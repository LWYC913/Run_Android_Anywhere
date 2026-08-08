//! Leader-only pull consumption, canonical validation, fair matching, and dispatch.

use std::{collections::BTreeMap, time::Instant};

use async_nats::{Client, HeaderMap, jetstream};
use chrono::{Duration as ChronoDuration, Utc};
use futures_util::StreamExt as _;
use run_anywhere_contracts::{
    ControlResponse, JobDeadLetter, JobDeadLetterReason, JobDispatch, JobId, JobOutcome, JobState,
    LeaseId, TransitionEvidence,
};
use run_anywhere_repository::{
    ClaimJobOptions, JobSchedulingSnapshot, ProjectQuota, Repository, RepositoryError,
};
use serde_json::json;
use thiserror::Error;
use tokio::sync::watch;
use uuid::Uuid;

use crate::{
    AckBinding, AckRegistry, CanonicalDeliveryDecision, Config, DlqPublishOutcome, DlqPublisher,
    FairEntry, FairQueue, FairQueueConfig, PriorityLane, QueuePayloadError, SchedulerMetrics,
    delivery::decode_queue_payload,
    dlq::{exhaustion_event_key, is_matching_exhaustion_marker},
    metrics::DlqMetricReason,
    subjects::{
        JOB_DISPATCH_CONSUMER, JOB_QUEUE_STREAM, JOBS_QUEUED_SUBJECT, extract_trace_headers,
        inject_trace_headers, worker_dispatch_subject,
    },
};

const NATS_MESSAGE_ID_HEADER: &str = "Nats-Msg-Id";

#[derive(Clone)]
struct PendingDelivery {
    queued: run_anywhere_contracts::JobQueued,
    trace_context: BTreeMap<String, String>,
    message: jetstream::Message,
    stream_sequence: u64,
    delivered: u32,
    last_progress_at: chrono::DateTime<Utc>,
}

impl std::fmt::Debug for PendingDelivery {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PendingDelivery")
            .field("job_id", &self.queued.job_id)
            .field("stream_sequence", &self.stream_sequence)
            .field("delivered", &self.delivered)
            .field("message", &"[redacted]")
            .finish()
    }
}

#[derive(Clone)]
pub struct Dispatcher {
    client: Client,
    repository: Repository,
    consumer: jetstream::consumer::PullConsumer,
    job_stream: jetstream::stream::Stream,
    acknowledgements: AckRegistry,
    dlq: DlqPublisher,
    config: Config,
    metrics: SchedulerMetrics,
}

impl std::fmt::Debug for Dispatcher {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Dispatcher")
            .field("consumer", &crate::subjects::JOB_DISPATCH_CONSUMER)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Error)]
pub enum DispatcherError {
    #[error(transparent)]
    Repository(#[from] RepositoryError),
    #[error(transparent)]
    Dlq(#[from] crate::DlqError),
    #[error("could not start or read the JetStream pull consumer: {0}")]
    Consumer(String),
    #[error("could not acknowledge a queue message: {0}")]
    Acknowledge(String),
    #[error("could not construct the fairness buffer: {0}")]
    Fairness(String),
    #[error("worker dispatch serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("trace propagation failed: {0}")]
    Trace(String),
    #[error("canonical queue redrive failed: {0}")]
    Redrive(String),
    #[error("invalid durable exhaustion marker `{0}`")]
    InvalidExhaustionMarker(String),
}

impl Dispatcher {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: Client,
        repository: Repository,
        consumer: jetstream::consumer::PullConsumer,
        job_stream: jetstream::stream::Stream,
        acknowledgements: AckRegistry,
        dlq: DlqPublisher,
        config: Config,
        metrics: SchedulerMetrics,
    ) -> Self {
        Self {
            client,
            repository,
            consumer,
            job_stream,
            acknowledgements,
            dlq,
            config,
            metrics,
        }
    }

    pub async fn run(self, mut shutdown: watch::Receiver<bool>) -> Result<(), DispatcherError> {
        let mut fair = FairQueue::new(FairQueueConfig {
            capacity: self.config.fairness.buffer_size,
            interactive_weight: self.config.fairness.interactive_weight,
            batch_weight: self.config.fairness.batch_weight,
            batch_aging: self.config.fairness.batch_aging,
        })
        .map_err(|error| DispatcherError::Fairness(error.to_string()))?;
        let mut messages = self
            .consumer
            .stream()
            .max_messages_per_batch(self.config.topology.pull_batch_size)
            .messages()
            .await
            .map_err(|error| DispatcherError::Consumer(error.to_string()))?;
        let mut dispatch_tick = tokio::time::interval(std::time::Duration::from_secs(1));
        dispatch_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut metrics_tick = tokio::time::interval(self.config.timing.reconciliation_interval);
        metrics_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    let _ = changed;
                    return Ok(());
                }
                item = messages.next() => {
                    let Some(item) = item else {
                        return Err(DispatcherError::Consumer("message stream closed".to_owned()));
                    };
                    match item {
                        Ok(message) => {
                            let retry = message.clone();
                            if let Err(error) = self.accept_delivery(&mut fair, message).await {
                                let _ = retry.ack_with(jetstream::AckKind::Progress).await;
                                tracing::warn!(error = %error, "queue delivery was left pending for retry");
                            }
                        }
                        Err(error) => {
                            tracing::warn!(error = %error, "JetStream pull returned a transient error");
                        }
                    }
                }
                _ = dispatch_tick.tick() => {
                    self.progress_buffered(&mut fair).await;
                    // Drain at most one configured pull batch. Capturing the
                    // initial length prevents a capacity-blocked entry that is
                    // requeued during this pass from creating an unbounded
                    // busy loop.
                    let dispatch_budget = fair
                        .len()
                        .min(self.config.topology.pull_batch_size);
                    for _ in 0..dispatch_budget {
                        if let Err(error) = self.dispatch_one(&mut fair).await {
                            tracing::warn!(error = %error, "fair dispatch attempt failed");
                        }
                        // A worker acceptance timeout may consume one heartbeat
                        // interval; keep every other buffered delivery alive
                        // while this bounded batch is drained.
                        self.progress_buffered(&mut fair).await;
                    }
                    self.update_pending_metrics(&fair);
                }
                _ = metrics_tick.tick() => {
                    self.refresh_consumer_metrics().await;
                }
            }
        }
    }

    async fn accept_delivery(
        &self,
        fair: &mut FairQueue<PendingDelivery>,
        message: jetstream::Message,
    ) -> Result<(), DispatcherError> {
        let info = message
            .info()
            .map_err(|error| DispatcherError::Consumer(error.to_string()))?;
        let stream_sequence = info.stream_sequence;
        let delivered = u32::try_from(info.delivered.max(1)).unwrap_or(u32::MAX);
        if delivered > 1 {
            self.metrics.record_redelivery();
        }
        let trace_context = message
            .headers
            .as_ref()
            .map(extract_trace_headers)
            .unwrap_or_default();
        let queued = match decode_queue_payload(message.payload.as_ref()) {
            Ok(queued) => queued,
            Err(QueuePayloadError::TooLarge | QueuePayloadError::Malformed) => {
                return self
                    .dead_letter_delivery(
                        &message,
                        stream_sequence,
                        1,
                        None,
                        None,
                        JobDeadLetterReason::MalformedMessage,
                        trace_context,
                    )
                    .await;
            }
        };
        let snapshot = self
            .repository
            .get_job_scheduling_snapshot(&queued.job_id)
            .await?;
        let Some(snapshot) = snapshot else {
            return self
                .dead_letter_delivery(
                    &message,
                    stream_sequence,
                    1,
                    Some(queued.job_id),
                    Some(queued.project_id),
                    JobDeadLetterReason::CanonicalJobInconsistent,
                    trace_context,
                )
                .await;
        };

        match crate::classify_canonical_delivery(&queued, &snapshot, Utc::now()) {
            CanonicalDeliveryDecision::AckTerminal => {
                self.finish_terminal_delivery(&snapshot, &message, stream_sequence, trace_context)
                    .await
            }
            CanonicalDeliveryDecision::FinalizeCancellation => {
                self.finalize_queued_cancellation(&snapshot).await?;
                confirm_ack(&message).await
            }
            CanonicalDeliveryDecision::Rebind(lease) => {
                self.bind_or_ack_duplicate(
                    queued.job_id,
                    lease.worker_id,
                    lease.lease_id,
                    stream_sequence,
                    message,
                )
                .await
            }
            CanonicalDeliveryDecision::HoldForReconciliation => {
                if let Some(lease) = snapshot.lease {
                    return self
                        .bind_or_ack_duplicate(
                            queued.job_id,
                            lease.worker_id,
                            lease.lease_id,
                            stream_sequence,
                            message,
                        )
                        .await;
                }
                progress(&message).await
            }
            CanonicalDeliveryDecision::DeadLetter(reason) => {
                if reason == JobDeadLetterReason::CanonicalJobInconsistent {
                    self.redrive_canonical(&snapshot.queued, &trace_context, stream_sequence)
                        .await?;
                }
                self.dead_letter_delivery(
                    &message,
                    stream_sequence,
                    snapshot.delivery_attempts.max(1),
                    Some(snapshot.job.id),
                    Some(snapshot.job.project_id),
                    reason,
                    trace_context,
                )
                .await
            }
            CanonicalDeliveryDecision::Buffer(lane) => {
                let job_id = queued.job_id.clone();
                if let Some(existing) = fair.get_mut(&job_id) {
                    if existing.payload.stream_sequence == stream_sequence {
                        existing.payload.message = message;
                        existing.payload.delivered = delivered;
                        existing.payload.trace_context = trace_context;
                        progress(&existing.payload.message).await?;
                        existing.payload.last_progress_at = Utc::now();
                    } else if existing.payload.stream_sequence < stream_sequence {
                        let _ = existing.payload.message.double_ack().await;
                        existing.payload.message = message;
                        existing.payload.stream_sequence = stream_sequence;
                        existing.payload.delivered = delivered;
                        existing.payload.trace_context = trace_context;
                        progress(&existing.payload.message).await?;
                        existing.payload.last_progress_at = Utc::now();
                    } else {
                        // Never let an older, possibly exhausted source
                        // sequence displace a newer canonical delivery.
                        let _ = message.double_ack().await;
                    }
                    return Ok(());
                }
                let entry = FairEntry::new(
                    job_id.clone(),
                    queued.project_id.clone(),
                    lane,
                    snapshot.job.created_at,
                    Utc::now(),
                    PendingDelivery {
                        queued,
                        trace_context,
                        message,
                        stream_sequence,
                        delivered,
                        last_progress_at: Utc::now(),
                    },
                );
                fair.push(entry)
                    .map_err(|_| DispatcherError::Fairness("buffer is full".to_owned()))
            }
        }
    }

    async fn dispatch_one(
        &self,
        fair: &mut FairQueue<PendingDelivery>,
    ) -> Result<(), DispatcherError> {
        let Some(mut entry) = fair.pop(Utc::now()) else {
            return Ok(());
        };
        let Some(snapshot) = self
            .repository
            .get_job_scheduling_snapshot(&entry.job_id)
            .await?
        else {
            return self
                .dead_letter_entry(entry, JobDeadLetterReason::CanonicalJobInconsistent, 1)
                .await;
        };
        match crate::classify_canonical_delivery(&entry.payload.queued, &snapshot, Utc::now()) {
            CanonicalDeliveryDecision::Buffer(_) => {}
            CanonicalDeliveryDecision::AckTerminal => {
                return self
                    .finish_terminal_delivery(
                        &snapshot,
                        &entry.payload.message,
                        entry.payload.stream_sequence,
                        entry.payload.trace_context,
                    )
                    .await;
            }
            CanonicalDeliveryDecision::FinalizeCancellation => {
                self.finalize_queued_cancellation(&snapshot).await?;
                return confirm_ack(&entry.payload.message).await;
            }
            CanonicalDeliveryDecision::Rebind(lease) => {
                return self
                    .bind_or_ack_duplicate(
                        entry.job_id,
                        lease.worker_id,
                        lease.lease_id,
                        entry.payload.stream_sequence,
                        entry.payload.message,
                    )
                    .await;
            }
            CanonicalDeliveryDecision::HoldForReconciliation => {
                entry
                    .payload
                    .progress_if_due(self.config.timing.worker_heartbeat)
                    .await?;
                return self.requeue(fair, entry);
            }
            CanonicalDeliveryDecision::DeadLetter(reason) => {
                let attempt = snapshot.delivery_attempts.max(1);
                if reason == JobDeadLetterReason::CanonicalJobInconsistent {
                    self.redrive_canonical(
                        &snapshot.queued,
                        &entry.payload.trace_context,
                        entry.payload.stream_sequence,
                    )
                    .await?;
                }
                return self.dead_letter_entry(entry, reason, attempt).await;
            }
        }

        let cutoff = Utc::now()
            - ChronoDuration::from_std(self.config.timing.stale_worker_threshold)
                .expect("validated duration fits chrono");
        let workers = self
            .repository
            .find_workers_matching(
                &snapshot.runtime_profile,
                entry.payload.queued.min_isolation,
                cutoff,
                200,
            )
            .await?;
        if workers.is_empty() {
            entry
                .payload
                .progress_if_due(self.config.timing.worker_heartbeat)
                .await?;
            return self.requeue(fair, entry);
        }

        for worker in workers {
            let lease_id = LeaseId::new(format!("lease_{}", Uuid::new_v4().simple()))
                .expect("UUID lease ID is valid");
            let lease_expires_at = Utc::now()
                + ChronoDuration::from_std(self.config.timing.database_lease_ttl)
                    .expect("validated duration fits chrono");
            let started = Instant::now();
            let claim = match self
                .repository
                .claim_job_with_options(
                    &entry.job_id,
                    &worker.worker_id,
                    &lease_id,
                    lease_expires_at,
                    ClaimJobOptions {
                        worker_stale_after: ChronoDuration::from_std(
                            self.config.timing.stale_worker_threshold,
                        )
                        .expect("validated duration fits chrono"),
                    },
                )
                .await
            {
                Ok(claim) => claim,
                Err(RepositoryError::QuotaExceeded {
                    quota: ProjectQuota::ConcurrentJobs,
                    ..
                }) => {
                    self.metrics.record_quota_blocked();
                    entry
                        .payload
                        .progress_if_due(self.config.timing.worker_heartbeat)
                        .await?;
                    return self.requeue(fair, entry);
                }
                Err(
                    RepositoryError::CompareAndSwapLost { .. }
                    | RepositoryError::Conflict(_)
                    | RepositoryError::NotFound {
                        entity: "worker", ..
                    },
                ) => continue,
                Err(error) => return Err(error.into()),
            };
            self.metrics.observe_claim_latency(started.elapsed());
            self.acknowledgements
                .bind(
                    claim.job_id.clone(),
                    AckBinding {
                        worker_id: claim.worker_id.clone(),
                        lease_id: claim.lease_id.clone(),
                        stream_sequence: entry.payload.stream_sequence,
                        message: entry.payload.message.clone(),
                    },
                )
                .await;
            progress(&entry.payload.message).await?;

            let dispatch = JobDispatch {
                claim: claim.clone(),
                trace_context: entry.payload.trace_context.clone(),
            };
            let mut headers = HeaderMap::new();
            inject_trace_headers(&mut headers, &entry.payload.trace_context)
                .map_err(|error| DispatcherError::Trace(error.to_string()))?;
            let request = self.client.request_with_headers(
                worker_dispatch_subject(&claim.worker_id),
                headers,
                serde_json::to_vec(&dispatch)?.into(),
            );
            let accepted = tokio::time::timeout(self.config.timing.worker_heartbeat, request)
                .await
                .ok()
                .and_then(Result::ok)
                .and_then(|reply| serde_json::from_slice::<ControlResponse>(&reply.payload).ok())
                == Some(ControlResponse::Accepted);
            if accepted {
                tracing::info!(job_id = %claim.job_id, worker_id = %claim.worker_id, lease_id = %claim.lease_id, "worker accepted fenced dispatch");
            } else {
                // The durable lease remains the sole owner. The reconciler will
                // release it after expiry/staleness; no second worker is started.
                tracing::warn!(job_id = %claim.job_id, worker_id = %claim.worker_id, lease_id = %claim.lease_id, "worker did not accept dispatch before the control deadline");
            }
            return Ok(());
        }

        entry
            .payload
            .progress_if_due(self.config.timing.worker_heartbeat)
            .await?;
        self.requeue(fair, entry)
    }

    fn requeue(
        &self,
        fair: &mut FairQueue<PendingDelivery>,
        entry: FairEntry<PendingDelivery>,
    ) -> Result<(), DispatcherError> {
        fair.push(entry)
            .map_err(|_| DispatcherError::Fairness("buffer is full".to_owned()))
    }

    async fn bind_or_ack_duplicate(
        &self,
        job_id: JobId,
        worker_id: run_anywhere_contracts::WorkerId,
        lease_id: LeaseId,
        stream_sequence: u64,
        message: jetstream::Message,
    ) -> Result<(), DispatcherError> {
        if let Ok(existing) = self
            .acknowledgements
            .binding(&job_id, &worker_id, &lease_id)
            .await
        {
            if existing.stream_sequence > stream_sequence {
                let _ = message.double_ack().await;
                return Ok(());
            }
            if existing.stream_sequence < stream_sequence {
                let _ = existing.message.double_ack().await;
            }
        }
        self.acknowledgements
            .bind(
                job_id,
                AckBinding {
                    worker_id,
                    lease_id,
                    stream_sequence,
                    message: message.clone(),
                },
            )
            .await;
        progress(&message).await
    }

    async fn finalize_queued_cancellation(
        &self,
        snapshot: &JobSchedulingSnapshot,
    ) -> Result<(), DispatcherError> {
        if snapshot.job.state == JobState::CollectingArtifacts {
            self.repository
                .transition_job_state(
                    &snapshot.job.id,
                    JobState::CollectingArtifacts,
                    JobState::CleaningUp,
                    TransitionEvidence {
                        pending_outcome: Some(JobOutcome::Cancelled),
                        artifacts_finalized: true,
                        cleanup_completed: false,
                    },
                    None,
                    BTreeMap::from([("finalizer".to_owned(), json!("empty_queue_job"))]),
                )
                .await?;
        }
        let refreshed = self
            .repository
            .get_job_scheduling_snapshot(&snapshot.job.id)
            .await?;
        if refreshed.is_some_and(|job| job.job.state == JobState::CleaningUp) {
            self.repository
                .transition_job_state(
                    &snapshot.job.id,
                    JobState::CleaningUp,
                    JobState::Cancelled,
                    TransitionEvidence {
                        pending_outcome: Some(JobOutcome::Cancelled),
                        artifacts_finalized: true,
                        cleanup_completed: true,
                    },
                    None,
                    BTreeMap::from([("cleanup".to_owned(), json!("not_started"))]),
                )
                .await?;
        }
        Ok(())
    }

    /// A stale delivery for an ordinary terminal job can be acknowledged
    /// immediately. Exhaustion is different: stale recovery inserts the DLQ
    /// outbox row in the same transaction that starts finalization, so a
    /// terminal `infra_failed` record carrying that marker must not retire the
    /// source message until the marker has been publish-confirmed.
    async fn finish_terminal_delivery(
        &self,
        snapshot: &JobSchedulingSnapshot,
        message: &jetstream::Message,
        stream_sequence: u64,
        trace_context: BTreeMap<String, String>,
    ) -> Result<(), DispatcherError> {
        if snapshot.job.state == JobState::InfraFailed {
            let attempt = snapshot.delivery_attempts.max(1);
            let event_key = exhaustion_event_key(&snapshot.job.id, attempt);
            if let Some(outbox) = self.repository.get_outbox_message(&event_key).await? {
                if !is_matching_exhaustion_marker(&outbox, &snapshot.job.id, attempt) {
                    return Err(DispatcherError::InvalidExhaustionMarker(event_key));
                }
                let dead_letter = JobDeadLetter::new(
                    Some(snapshot.job.id.clone()),
                    Some(snapshot.job.project_id.clone()),
                    Some(stream_sequence),
                    attempt,
                    JobDeadLetterReason::AttemptsExhausted,
                    Utc::now(),
                )
                .expect("terminal exhaustion metadata forms a valid dead letter");
                let publish_outcome = self
                    .dlq
                    .ensure_published(&dead_letter, trace_context)
                    .await?;
                if publish_outcome == DlqPublishOutcome::NewlyPublished {
                    self.metrics.record_dlq(DlqMetricReason::AttemptsExhausted);
                }
                message
                    .ack_with(jetstream::AckKind::Term)
                    .await
                    .map_err(|error| DispatcherError::Acknowledge(error.to_string()))?;
                self.dlq
                    .delete_source(&self.job_stream, stream_sequence)
                    .await?;
                return Ok(());
            }
        }
        confirm_ack(message).await
    }

    /// Confirm a replacement canonical delivery before retiring a known-job
    /// poison message. The immutable source sequence makes retries and the
    /// advisory recovery path converge on one JetStream deduplication key.
    async fn redrive_canonical(
        &self,
        queued: &run_anywhere_contracts::JobQueued,
        trace_context: &BTreeMap<String, String>,
        source_stream_sequence: u64,
    ) -> Result<(), DispatcherError> {
        let mut headers = HeaderMap::new();
        inject_trace_headers(&mut headers, trace_context)
            .map_err(|error| DispatcherError::Trace(error.to_string()))?;
        headers.insert(
            NATS_MESSAGE_ID_HEADER,
            format!("redrive:{JOB_QUEUE_STREAM}:{JOB_DISPATCH_CONSUMER}:{source_stream_sequence}"),
        );
        let acknowledgement = jetstream::new(self.client.clone())
            .publish_with_headers(
                JOBS_QUEUED_SUBJECT,
                headers,
                serde_json::to_vec(queued)?.into(),
            )
            .await
            .map_err(|error| DispatcherError::Redrive(error.to_string()))?;
        acknowledgement
            .await
            .map_err(|error| DispatcherError::Redrive(error.to_string()))?;
        Ok(())
    }

    async fn dead_letter_entry(
        &self,
        entry: FairEntry<PendingDelivery>,
        reason: JobDeadLetterReason,
        attempt: u32,
    ) -> Result<(), DispatcherError> {
        self.dead_letter_delivery(
            &entry.payload.message,
            entry.payload.stream_sequence,
            attempt,
            Some(entry.payload.queued.job_id),
            Some(entry.payload.queued.project_id),
            reason,
            entry.payload.trace_context,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn dead_letter_delivery(
        &self,
        message: &jetstream::Message,
        stream_sequence: u64,
        attempt: u32,
        job_id: Option<JobId>,
        project_id: Option<run_anywhere_contracts::ProjectId>,
        reason: JobDeadLetterReason,
        trace_context: BTreeMap<String, String>,
    ) -> Result<(), DispatcherError> {
        let dead_letter = JobDeadLetter::new(
            job_id,
            project_id,
            Some(stream_sequence),
            attempt.max(1),
            reason,
            Utc::now(),
        )
        .expect("delivery metadata forms a valid sanitized dead letter");
        let publish_outcome = self
            .dlq
            .ensure_published(&dead_letter, trace_context)
            .await?;
        if publish_outcome == DlqPublishOutcome::NewlyPublished {
            self.metrics.record_dlq(metric_reason(reason));
        }
        message
            .ack_with(jetstream::AckKind::Term)
            .await
            .map_err(|error| DispatcherError::Acknowledge(error.to_string()))?;
        self.dlq
            .delete_source(&self.job_stream, stream_sequence)
            .await?;
        Ok(())
    }

    fn update_pending_metrics(&self, fair: &FairQueue<PendingDelivery>) {
        self.metrics.set_pending_dispatches(
            PriorityLane::Interactive,
            u64::try_from(fair.lane_len(PriorityLane::Interactive)).unwrap_or(u64::MAX),
        );
        self.metrics.set_pending_dispatches(
            PriorityLane::Batch,
            u64::try_from(fair.lane_len(PriorityLane::Batch)).unwrap_or(u64::MAX),
        );
    }

    async fn progress_buffered(&self, fair: &mut FairQueue<PendingDelivery>) {
        for entry in fair.iter_mut() {
            if let Err(error) = entry
                .payload
                .progress_if_due(self.config.timing.worker_heartbeat)
                .await
            {
                tracing::warn!(
                    job_id = %entry.job_id,
                    stream_sequence = entry.payload.stream_sequence,
                    error = %error,
                    "could not renew a buffered queue delivery"
                );
            }
        }
    }

    async fn refresh_consumer_metrics(&self) {
        let mut consumer = self.consumer.clone();
        match consumer.info().await {
            Ok(info) => self.metrics.set_queue_depth(
                info.num_pending,
                u64::try_from(info.num_ack_pending).unwrap_or(u64::MAX),
            ),
            Err(error) => tracing::warn!(error = %error, "could not refresh queue-depth metrics"),
        }
    }
}

impl PendingDelivery {
    async fn progress_if_due(
        &mut self,
        interval: std::time::Duration,
    ) -> Result<(), DispatcherError> {
        let elapsed = Utc::now().signed_duration_since(self.last_progress_at);
        let interval = ChronoDuration::from_std(interval).expect("validated duration fits chrono");
        if elapsed >= interval {
            progress(&self.message).await?;
            self.last_progress_at = Utc::now();
        }
        Ok(())
    }
}

async fn progress(message: &jetstream::Message) -> Result<(), DispatcherError> {
    message
        .ack_with(jetstream::AckKind::Progress)
        .await
        .map_err(|error| DispatcherError::Acknowledge(error.to_string()))
}

async fn confirm_ack(message: &jetstream::Message) -> Result<(), DispatcherError> {
    message
        .double_ack()
        .await
        .map_err(|error| DispatcherError::Acknowledge(error.to_string()))
}

const fn metric_reason(reason: JobDeadLetterReason) -> DlqMetricReason {
    match reason {
        JobDeadLetterReason::MalformedMessage => DlqMetricReason::Malformed,
        JobDeadLetterReason::CanonicalJobInconsistent => DlqMetricReason::DatabaseInconsistent,
        JobDeadLetterReason::AttemptsExhausted => DlqMetricReason::AttemptsExhausted,
    }
}

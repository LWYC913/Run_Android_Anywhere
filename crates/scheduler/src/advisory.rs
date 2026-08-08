//! Durable recovery for JetStream max-delivery and termination advisories.

use std::collections::BTreeMap;

use async_nats::{HeaderMap, jetstream};
use futures_util::StreamExt as _;
use run_anywhere_contracts::{JobDeadLetter, JobDeadLetterReason, JobOutcome};
use run_anywhere_repository::Repository;
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::watch;

use crate::{
    DlqPublishOutcome, DlqPublisher, SchedulerMetrics,
    delivery::decode_queue_payload,
    dlq::{exhaustion_event_key, is_matching_exhaustion_marker},
    metrics::DlqMetricReason,
    subjects::{
        JOB_DISPATCH_CONSUMER, JOB_QUEUE_STREAM, JOBS_QUEUED_SUBJECT, extract_trace_headers,
        inject_trace_headers,
    },
};

const NATS_MESSAGE_ID_HEADER: &str = "Nats-Msg-Id";
const MAX_ADVISORY_PAYLOAD_BYTES: usize = 64 * 1024;

#[derive(Clone)]
pub struct AdvisoryRecovery {
    jetstream: jetstream::Context,
    repository: Repository,
    consumer: jetstream::consumer::PullConsumer,
    job_stream: jetstream::stream::Stream,
    dlq: DlqPublisher,
    max_run_attempts: u32,
    pull_batch_size: usize,
    metrics: SchedulerMetrics,
}

impl std::fmt::Debug for AdvisoryRecovery {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AdvisoryRecovery")
            .field("max_run_attempts", &self.max_run_attempts)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Error)]
pub enum AdvisoryError {
    #[error(transparent)]
    Repository(#[from] run_anywhere_repository::RepositoryError),
    #[error(transparent)]
    Dlq(#[from] crate::DlqError),
    #[error("advisory pull consumer failed: {0}")]
    Consumer(String),
    #[error("source-stream lookup failed: {0}")]
    Source(String),
    #[error("advisory acknowledgement failed: {0}")]
    Acknowledge(String),
    #[error("queue redrive failed: {0}")]
    Redrive(String),
    #[error("trace propagation failed: {0}")]
    Trace(String),
    #[error("invalid durable exhaustion marker `{0}`")]
    InvalidExhaustionMarker(String),
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
struct ConsumerAdvisory {
    stream: String,
    consumer: String,
    stream_seq: u64,
}

impl AdvisoryRecovery {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        jetstream: jetstream::Context,
        repository: Repository,
        consumer: jetstream::consumer::PullConsumer,
        job_stream: jetstream::stream::Stream,
        dlq: DlqPublisher,
        max_run_attempts: u32,
        pull_batch_size: usize,
        metrics: SchedulerMetrics,
    ) -> Self {
        Self {
            jetstream,
            repository,
            consumer,
            job_stream,
            dlq,
            max_run_attempts,
            pull_batch_size,
            metrics,
        }
    }

    pub async fn run(self, mut shutdown: watch::Receiver<bool>) -> Result<(), AdvisoryError> {
        let mut messages = self
            .consumer
            .stream()
            .max_messages_per_batch(self.pull_batch_size)
            .messages()
            .await
            .map_err(|error| AdvisoryError::Consumer(error.to_string()))?;
        loop {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    let _ = changed;
                    return Ok(());
                }
                item = messages.next() => {
                    let Some(item) = item else {
                        return Err(AdvisoryError::Consumer("advisory stream closed".to_owned()));
                    };
                    match item {
                        Ok(message) => {
                            let retry = message.clone();
                            if let Err(error) = self.handle(message).await {
                                let _ = retry.ack_with(jetstream::AckKind::Progress).await;
                                tracing::warn!(error = %error, "advisory recovery remains pending");
                            }
                        }
                        Err(error) => tracing::warn!(error = %error, "advisory pull returned a transient error"),
                    }
                }
            }
        }
    }

    async fn handle(&self, message: jetstream::Message) -> Result<(), AdvisoryError> {
        if message.payload.len() > MAX_ADVISORY_PAYLOAD_BYTES {
            return confirm_ack(&message).await;
        }
        let advisory: ConsumerAdvisory = match serde_json::from_slice(&message.payload) {
            Ok(advisory) => advisory,
            Err(_) => return confirm_ack(&message).await,
        };
        if advisory.stream != JOB_QUEUE_STREAM
            || advisory.consumer != JOB_DISPATCH_CONSUMER
            || advisory.stream_seq == 0
        {
            return confirm_ack(&message).await;
        }
        let raw = match self.job_stream.get_raw_message(advisory.stream_seq).await {
            Ok(raw) => raw,
            Err(error)
                if error.kind() == jetstream::stream::RawMessageErrorKind::NoMessageFound =>
            {
                return confirm_ack(&message).await;
            }
            Err(error) => return Err(AdvisoryError::Source(error.to_string())),
        };
        let trace_context = extract_trace_headers(&raw.headers);
        let queued = match decode_queue_payload(&raw.payload) {
            Ok(queued) => queued,
            Err(_) => {
                let dead_letter = JobDeadLetter::new(
                    None,
                    None,
                    Some(advisory.stream_seq),
                    1,
                    JobDeadLetterReason::MalformedMessage,
                    chrono::Utc::now(),
                )
                .expect("advisory sequence forms a stable dead letter");
                let publish_outcome = self
                    .dlq
                    .ensure_published(&dead_letter, trace_context)
                    .await?;
                if publish_outcome == DlqPublishOutcome::NewlyPublished {
                    self.metrics.record_dlq(DlqMetricReason::Malformed);
                }
                self.dlq
                    .delete_source(&self.job_stream, advisory.stream_seq)
                    .await?;
                return confirm_ack(&message).await;
            }
        };
        let snapshot = self
            .repository
            .get_job_scheduling_snapshot(&queued.job_id)
            .await?;
        let Some(snapshot) = snapshot else {
            return self
                .dead_letter_and_finish(
                    &message,
                    advisory.stream_seq,
                    1,
                    Some(queued.job_id),
                    Some(queued.project_id),
                    JobDeadLetterReason::CanonicalJobInconsistent,
                    trace_context,
                )
                .await;
        };
        let attempt = snapshot.delivery_attempts.max(1);
        let exhausted_event_key = exhaustion_event_key(&snapshot.job.id, attempt);
        let exhaustion_marker = self
            .repository
            .get_outbox_message(&exhausted_event_key)
            .await?;
        let recovery_marked_exhausted = match exhaustion_marker {
            Some(marker) if is_matching_exhaustion_marker(&marker, &snapshot.job.id, attempt) => {
                snapshot.delivery_attempts >= self.max_run_attempts
            }
            Some(_) => return Err(AdvisoryError::InvalidExhaustionMarker(exhausted_event_key)),
            None => false,
        };
        if snapshot.job.state.is_terminal() {
            if recovery_marked_exhausted {
                return self
                    .dead_letter_and_finish(
                        &message,
                        advisory.stream_seq,
                        snapshot.delivery_attempts.max(1),
                        Some(snapshot.job.id),
                        Some(snapshot.job.project_id),
                        JobDeadLetterReason::AttemptsExhausted,
                        trace_context,
                    )
                    .await;
            }
            self.dlq
                .delete_source(&self.job_stream, advisory.stream_seq)
                .await?;
            return confirm_ack(&message).await;
        }
        if queued != snapshot.queued {
            self.redrive_canonical(&snapshot.queued, &trace_context, advisory.stream_seq)
                .await?;
            return self
                .dead_letter_and_finish(
                    &message,
                    advisory.stream_seq,
                    attempt,
                    Some(snapshot.job.id),
                    Some(snapshot.job.project_id),
                    JobDeadLetterReason::CanonicalJobInconsistent,
                    trace_context,
                )
                .await;
        }
        if snapshot.pending_outcome == Some(JobOutcome::InfraFailed)
            && snapshot.lease.is_none()
            && recovery_marked_exhausted
        {
            return self
                .dead_letter_and_finish(
                    &message,
                    advisory.stream_seq,
                    snapshot.delivery_attempts.max(1),
                    Some(snapshot.job.id),
                    Some(snapshot.job.project_id),
                    JobDeadLetterReason::AttemptsExhausted,
                    trace_context,
                )
                .await;
        }

        // MaxDeliver can be consumed by scheduler crashes, capacity waits, or
        // a lost Progress acknowledgement. Redrive canonical work instead of
        // charging a run attempt. A still-live database lease is preserved and
        // the dispatcher rebinds the replacement delivery to that exact lease.
        self.redrive_canonical(&snapshot.queued, &trace_context, advisory.stream_seq)
            .await?;
        self.dlq
            .delete_source(&self.job_stream, advisory.stream_seq)
            .await?;
        confirm_ack(&message).await
    }

    async fn redrive_canonical(
        &self,
        queued: &run_anywhere_contracts::JobQueued,
        trace_context: &BTreeMap<String, String>,
        source_stream_sequence: u64,
    ) -> Result<(), AdvisoryError> {
        let mut headers = HeaderMap::new();
        inject_trace_headers(&mut headers, trace_context)
            .map_err(|error| AdvisoryError::Trace(error.to_string()))?;
        headers.insert(
            NATS_MESSAGE_ID_HEADER,
            format!("redrive:{JOB_QUEUE_STREAM}:{JOB_DISPATCH_CONSUMER}:{source_stream_sequence}"),
        );
        let acknowledgement = self
            .jetstream
            .publish_with_headers(
                JOBS_QUEUED_SUBJECT,
                headers,
                serde_json::to_vec(queued)
                    .map_err(|error| AdvisoryError::Redrive(error.to_string()))?
                    .into(),
            )
            .await
            .map_err(|error| AdvisoryError::Redrive(error.to_string()))?;
        acknowledgement
            .await
            .map_err(|error| AdvisoryError::Redrive(error.to_string()))?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn dead_letter_and_finish(
        &self,
        advisory_message: &jetstream::Message,
        stream_sequence: u64,
        attempt: u32,
        job_id: Option<run_anywhere_contracts::JobId>,
        project_id: Option<run_anywhere_contracts::ProjectId>,
        reason: JobDeadLetterReason,
        trace_context: BTreeMap<String, String>,
    ) -> Result<(), AdvisoryError> {
        let dead_letter = JobDeadLetter::new(
            job_id,
            project_id,
            Some(stream_sequence),
            attempt.max(1),
            reason,
            chrono::Utc::now(),
        )
        .expect("advisory metadata forms a valid dead letter");
        let publish_outcome = self
            .dlq
            .ensure_published(&dead_letter, trace_context)
            .await?;
        if publish_outcome == DlqPublishOutcome::NewlyPublished {
            self.metrics.record_dlq(match reason {
                JobDeadLetterReason::MalformedMessage => DlqMetricReason::Malformed,
                JobDeadLetterReason::CanonicalJobInconsistent => {
                    DlqMetricReason::DatabaseInconsistent
                }
                JobDeadLetterReason::AttemptsExhausted => DlqMetricReason::AttemptsExhausted,
            });
        }
        self.dlq
            .delete_source(&self.job_stream, stream_sequence)
            .await?;
        confirm_ack(advisory_message).await
    }
}

async fn confirm_ack(message: &jetstream::Message) -> Result<(), AdvisoryError> {
    message
        .double_ack()
        .await
        .map_err(|error| AdvisoryError::Acknowledge(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_both_supported_advisory_shapes_without_retaining_extra_fields() {
        let advisory: ConsumerAdvisory = serde_json::from_value(serde_json::json!({
            "type": "io.nats.jetstream.advisory.v1.max_deliver",
            "stream": "JOB_QUEUE",
            "consumer": "job-dispatch-v1",
            "stream_seq": 42,
            "deliveries": 4
        }))
        .unwrap();
        assert_eq!(advisory.stream_seq, 42);
    }
}

//! Transactional-outbox publication and source-message retirement for DLQ work.

use std::{collections::BTreeMap, time::Duration};

use async_nats::{HeaderMap, jetstream};
use chrono::{Duration as ChronoDuration, Utc};
use run_anywhere_contracts::{JobDeadLetter, JobDeadLetterReason, JobId};
use run_anywhere_repository::{OutboxMessage, Repository};
use thiserror::Error;

use crate::{
    acknowledgements::AckBinding,
    subjects::{JOBS_DEAD_SUBJECT, inject_trace_headers},
};

const DLQ_LEASE_BATCH: u32 = 64;
const NATS_MESSAGE_ID_HEADER: &str = "Nats-Msg-Id";

#[must_use]
pub fn exhaustion_event_key(job_id: &JobId, attempt: u32) -> String {
    format!("dlq:{job_id}:{}", attempt.max(1))
}

/// Reject a same-key poison row or malformed outbox payload when deciding
/// whether a job was durably marked as execution-attempt exhaustion.
#[must_use]
pub fn is_matching_exhaustion_marker(
    message: &OutboxMessage,
    job_id: &JobId,
    attempt: u32,
) -> bool {
    if message.subject != JOBS_DEAD_SUBJECT
        || message.event_key != exhaustion_event_key(job_id, attempt)
    {
        return false;
    }
    serde_json::from_value::<JobDeadLetter>(message.payload.clone()).is_ok_and(|dead_letter| {
        dead_letter.reason == JobDeadLetterReason::AttemptsExhausted
            && dead_letter.job_id.as_ref() == Some(job_id)
            && dead_letter.attempt == attempt.max(1)
    })
}

#[derive(Debug, Error)]
pub enum DlqError {
    #[error(transparent)]
    Repository(#[from] run_anywhere_repository::RepositoryError),
    #[error("dead-letter trace context is invalid: {0}")]
    Trace(String),
    #[error("JetStream did not confirm dead-letter publication: {0}")]
    Publish(String),
    #[error("dead-letter outbox entry `{0}` is not yet publishable")]
    Pending(String),
    #[error("could not terminate the exhausted queue delivery: {0}")]
    Term(String),
    #[error("could not delete source-stream sequence {sequence}: {message}")]
    Delete { sequence: u64, message: String },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DlqPublishOutcome {
    NewlyPublished,
    AlreadyPublished,
}

#[derive(Clone, Debug)]
pub struct DlqPublisher {
    repository: Repository,
    jetstream: jetstream::Context,
    dispatcher_id: String,
    outbox_lease_timeout: ChronoDuration,
}

impl DlqPublisher {
    pub fn new(
        repository: Repository,
        jetstream: jetstream::Context,
        dispatcher_id: impl Into<String>,
    ) -> Self {
        Self {
            repository,
            jetstream,
            dispatcher_id: dispatcher_id.into(),
            outbox_lease_timeout: ChronoDuration::seconds(30),
        }
    }

    /// Durably insert the sanitized record, publish all ready DLQ outbox rows,
    /// and return only after this exact logical record is confirmed published.
    pub async fn ensure_published(
        &self,
        dead_letter: &JobDeadLetter,
        trace_context: BTreeMap<String, String>,
    ) -> Result<DlqPublishOutcome, DlqError> {
        let event_key = dead_letter
            .idempotency_key()
            .map_err(|error| DlqError::Trace(error.to_string()))?;
        let message = self
            .repository
            .enqueue_job_dead_letter(dead_letter, trace_context)
            .await?;
        let already_published = message.published_at.is_some();
        if message.published_at.is_none() {
            self.publish_ready().await?;
        }
        let published = self.repository.get_outbox_message(&event_key).await?;
        if published.and_then(|entry| entry.published_at).is_some() {
            Ok(if already_published {
                DlqPublishOutcome::AlreadyPublished
            } else {
                DlqPublishOutcome::NewlyPublished
            })
        } else {
            Err(DlqError::Pending(event_key))
        }
    }

    /// Drain the scheduler-owned DLQ outbox channel. The stable NATS message ID
    /// makes a crash after publish-confirm but before the PostgreSQL update safe.
    pub async fn publish_ready(&self) -> Result<u64, DlqError> {
        let messages = self
            .repository
            .lease_outbox_messages_for_subject(
                &self.dispatcher_id,
                JOBS_DEAD_SUBJECT,
                self.outbox_lease_timeout,
                DLQ_LEASE_BATCH,
            )
            .await?;
        let mut published = 0_u64;
        for message in messages {
            if let Err(error) = self.publish_one(&message).await {
                let retry_at = Utc::now() + ChronoDuration::seconds(5);
                let _ = self
                    .repository
                    .retry_outbox_message(
                        message.id,
                        &self.dispatcher_id,
                        retry_at,
                        "JetStream did not confirm DLQ publication",
                    )
                    .await;
                return Err(error);
            }
            self.repository
                .mark_outbox_published(message.id, &self.dispatcher_id)
                .await?;
            published = published.saturating_add(1);
        }
        Ok(published)
    }

    async fn publish_one(&self, message: &OutboxMessage) -> Result<(), DlqError> {
        let mut headers = HeaderMap::new();
        headers.insert(NATS_MESSAGE_ID_HEADER, message.event_key.as_str());
        inject_trace_headers(&mut headers, &message.trace_headers)
            .map_err(|error| DlqError::Trace(error.to_string()))?;
        let acknowledgement = self
            .jetstream
            .publish_with_headers(JOBS_DEAD_SUBJECT, headers, message.payload_bytes()?.into())
            .await
            .map_err(|error| DlqError::Publish(error.to_string()))?;
        acknowledgement
            .await
            .map_err(|error| DlqError::Publish(error.to_string()))?;
        Ok(())
    }

    /// Stop further delivery and remove the source message only after the DLQ
    /// publish has been confirmed by the server.
    pub async fn retire_delivery(
        &self,
        job_stream: &jetstream::stream::Stream,
        binding: &AckBinding,
    ) -> Result<(), DlqError> {
        binding
            .message
            .ack_with(jetstream::AckKind::Term)
            .await
            .map_err(|error| DlqError::Term(error.to_string()))?;
        self.delete_source(job_stream, binding.stream_sequence)
            .await
    }

    /// Advisory recovery has no live ack subject, but it can still delete the
    /// exhausted source record after durable, confirmed DLQ publication.
    pub async fn delete_source(
        &self,
        job_stream: &jetstream::stream::Stream,
        stream_sequence: u64,
    ) -> Result<(), DlqError> {
        let deletion = job_stream.delete_message(stream_sequence).await;
        match deletion {
            Ok(true) => Ok(()),
            Ok(false) => {
                self.accept_if_source_is_absent(
                    job_stream,
                    stream_sequence,
                    "JetStream reported an unsuccessful source-message deletion".to_owned(),
                )
                .await
            }
            Err(error) => {
                self.accept_if_source_is_absent(job_stream, stream_sequence, error.to_string())
                    .await
            }
        }
    }

    async fn accept_if_source_is_absent(
        &self,
        job_stream: &jetstream::stream::Stream,
        stream_sequence: u64,
        delete_error: String,
    ) -> Result<(), DlqError> {
        match job_stream.get_raw_message(stream_sequence).await {
            Err(error)
                if error.kind() == jetstream::stream::RawMessageErrorKind::NoMessageFound =>
            {
                Ok(())
            }
            Ok(_) | Err(_) => Err(DlqError::Delete {
                sequence: stream_sequence,
                message: delete_error,
            }),
        }
    }

    pub const fn retry_delay() -> Duration {
        Duration::from_secs(5)
    }
}

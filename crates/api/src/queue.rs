use std::{collections::BTreeMap, fmt, str::FromStr, sync::Arc, time::Duration};

use async_nats::{HeaderMap, HeaderName, HeaderValue, jetstream};
use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures_util::{StreamExt as _, stream};
use run_anywhere_contracts::JobQueued;
use run_anywhere_repository::{OutboxMessage, Repository, RepositoryError};
use thiserror::Error;
use tokio::{sync::watch, time::timeout};

use crate::observability::ApiMetrics;

pub const JOBS_QUEUED_SUBJECT: &str = "jobs.queued";
pub const NATS_MESSAGE_ID_HEADER: &str = "Nats-Msg-Id";

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum QueuePublishError {
    #[error("queue subject must be a concrete non-empty NATS subject")]
    InvalidSubject,
    #[error("queue message ID must not be blank or contain control characters")]
    InvalidMessageId,
    #[error("invalid NATS trace header name `{0}`")]
    InvalidHeaderName(String),
    #[error("invalid value for NATS trace header `{0}`")]
    InvalidHeaderValue(String),
    #[error("failed to serialize JobQueued: {0}")]
    Serialize(String),
    #[error("failed to publish JobQueued: {0}")]
    Publish(String),
    #[error("JetStream did not acknowledge JobQueued: {0}")]
    Acknowledgement(String),
}

/// Object-safe producer boundary. The API/outbox can use a fake publisher in
/// tests and the scheduler continues to consume the shared `JobQueued` contract.
#[async_trait]
pub trait JobQueuePublisher: Send + Sync {
    async fn publish_job_queued(
        &self,
        message: &JobQueued,
        trace_headers: &BTreeMap<String, String>,
    ) -> Result<(), QueuePublishError>;
}

#[derive(Clone)]
pub struct JetStreamPublisher {
    context: jetstream::Context,
    subject: String,
}

impl fmt::Debug for JetStreamPublisher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JetStreamPublisher")
            .field("subject", &self.subject)
            .finish_non_exhaustive()
    }
}

impl JetStreamPublisher {
    pub fn new(context: jetstream::Context) -> Self {
        Self {
            context,
            subject: JOBS_QUEUED_SUBJECT.to_owned(),
        }
    }

    pub fn with_subject(
        context: jetstream::Context,
        subject: impl Into<String>,
    ) -> Result<Self, QueuePublishError> {
        let subject = subject.into();
        validate_subject(&subject)?;
        Ok(Self { context, subject })
    }

    pub fn subject(&self) -> &str {
        &self.subject
    }
}

#[async_trait]
impl JobQueuePublisher for JetStreamPublisher {
    async fn publish_job_queued(
        &self,
        message: &JobQueued,
        trace_headers: &BTreeMap<String, String>,
    ) -> Result<(), QueuePublishError> {
        validate_subject(&self.subject)?;
        let message_id = message.job_id.as_str();
        validate_message_id(message_id)?;
        let payload = serde_json::to_vec(message)
            .map_err(|error| QueuePublishError::Serialize(error.to_string()))?;
        let headers = build_headers(message_id, trace_headers)?;
        let acknowledgement = self
            .context
            .publish_with_headers(self.subject.clone(), headers, payload.into())
            .await
            .map_err(|error| QueuePublishError::Publish(error.to_string()))?;
        acknowledgement
            .await
            .map_err(|error| QueuePublishError::Acknowledgement(error.to_string()))?;
        Ok(())
    }
}

fn build_headers(
    message_id: &str,
    trace_headers: &BTreeMap<String, String>,
) -> Result<HeaderMap, QueuePublishError> {
    let mut headers = HeaderMap::new();
    for (name, value) in trace_headers {
        if name.eq_ignore_ascii_case(NATS_MESSAGE_ID_HEADER) {
            continue;
        }
        let parsed_name = HeaderName::from_str(name)
            .map_err(|_| QueuePublishError::InvalidHeaderName(name.clone()))?;
        let parsed_value = HeaderValue::from_str(value)
            .map_err(|_| QueuePublishError::InvalidHeaderValue(name.clone()))?;
        headers.insert(parsed_name, parsed_value);
    }
    headers.insert(NATS_MESSAGE_ID_HEADER, message_id);
    Ok(headers)
}

fn validate_subject(subject: &str) -> Result<(), QueuePublishError> {
    if subject.trim().is_empty()
        || subject
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
        || subject.contains(['*', '>'])
    {
        return Err(QueuePublishError::InvalidSubject);
    }
    Ok(())
}

fn validate_message_id(message_id: &str) -> Result<(), QueuePublishError> {
    if message_id.trim().is_empty() || message_id.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(QueuePublishError::InvalidMessageId);
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct OutboxDispatcherConfig {
    pub dispatcher_id: String,
    pub lease_timeout: ChronoDuration,
    pub batch_size: u32,
    pub idle_poll_interval: Duration,
    pub publish_timeout: Duration,
    pub base_backoff: Duration,
    pub max_backoff: Duration,
}

impl OutboxDispatcherConfig {
    pub fn new(dispatcher_id: impl Into<String>) -> Self {
        Self {
            dispatcher_id: dispatcher_id.into(),
            lease_timeout: ChronoDuration::seconds(90),
            batch_size: 50,
            idle_poll_interval: Duration::from_secs(1),
            publish_timeout: Duration::from_secs(30),
            base_backoff: Duration::from_millis(250),
            max_backoff: Duration::from_secs(60),
        }
    }

    fn validate(&self) -> Result<(), OutboxDispatchError> {
        if self.dispatcher_id.trim().is_empty()
            || self.batch_size == 0
            || self.lease_timeout <= ChronoDuration::zero()
            || self.idle_poll_interval.is_zero()
            || self.publish_timeout.is_zero()
            || ChronoDuration::from_std(self.publish_timeout).map_or(true, |publish_timeout| {
                publish_timeout >= self.lease_timeout
            })
            || self.base_backoff.is_zero()
            || self.max_backoff < self.base_backoff
        {
            return Err(OutboxDispatchError::InvalidConfig);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DispatchBatch {
    pub leased: u32,
    pub published: u32,
    pub retried: u32,
}

#[derive(Debug, Error)]
pub enum OutboxDispatchError {
    #[error("invalid outbox dispatcher configuration")]
    InvalidConfig,
    #[error(transparent)]
    Repository(#[from] RepositoryError),
}

/// Multi-replica-safe PostgreSQL outbox dispatcher. Leasing is committed by the
/// repository before this type performs network I/O.
#[derive(Clone)]
pub struct OutboxDispatcher {
    repository: Repository,
    publisher: Arc<dyn JobQueuePublisher>,
    config: OutboxDispatcherConfig,
    metrics: ApiMetrics,
}

impl fmt::Debug for OutboxDispatcher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OutboxDispatcher")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl OutboxDispatcher {
    pub fn new(
        repository: Repository,
        publisher: Arc<dyn JobQueuePublisher>,
        config: OutboxDispatcherConfig,
    ) -> Result<Self, OutboxDispatchError> {
        config.validate()?;
        Ok(Self {
            repository,
            publisher,
            config,
            metrics: ApiMetrics::default(),
        })
    }

    pub fn with_metrics(mut self, metrics: ApiMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    pub async fn dispatch_once(&self) -> Result<DispatchBatch, OutboxDispatchError> {
        let messages = self
            .repository
            .lease_outbox_messages_for_subject(
                &self.config.dispatcher_id,
                JOBS_QUEUED_SUBJECT,
                self.config.lease_timeout,
                self.config.batch_size,
            )
            .await?;
        let mut batch = DispatchBatch {
            leased: u32::try_from(messages.len()).unwrap_or(u32::MAX),
            ..DispatchBatch::default()
        };

        // All leases begin at roughly the same time, so publish the bounded
        // batch concurrently and cap each acknowledgement below the lease
        // timeout. A failure for one row never prevents the rest from being
        // finalized or released for retry.
        let results = stream::iter(messages)
            .map(|message| self.dispatch_message(message))
            .buffer_unordered(self.config.batch_size as usize)
            .collect::<Vec<_>>()
            .await;
        let mut first_repository_error = None;
        for result in results {
            match result {
                Ok(MessageDispatch::Published) => batch.published += 1,
                Ok(MessageDispatch::Retried) => batch.retried += 1,
                Err(error) => {
                    self.metrics.record_outbox_finalization_failure();
                    tracing::error!(error = %error, "failed to finalize an outbox row");
                    if first_repository_error.is_none() {
                        first_repository_error = Some(error);
                    }
                }
            }
        }
        self.refresh_backlog().await;
        if let Some(error) = first_repository_error {
            return Err(error.into());
        }
        Ok(batch)
    }

    /// Run until the watch value becomes true or all senders are dropped.
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) {
        loop {
            if *shutdown.borrow() {
                return;
            }
            let should_poll_immediately = match self.dispatch_once().await {
                Ok(batch) => batch.leased >= self.config.batch_size,
                Err(error) => {
                    tracing::error!(error = %error, "outbox dispatch iteration failed");
                    false
                }
            };
            if should_poll_immediately {
                continue;
            }
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
                () = tokio::time::sleep(self.config.idle_poll_interval) => {}
            }
        }
    }

    async fn publish_outbox_message(
        &self,
        outbox: &OutboxMessage,
    ) -> Result<(), QueuePublishError> {
        if outbox.subject != JOBS_QUEUED_SUBJECT {
            return Err(QueuePublishError::InvalidSubject);
        }
        let message: JobQueued = serde_json::from_value(outbox.payload.clone())
            .map_err(|error| QueuePublishError::Serialize(error.to_string()))?;
        if outbox.event_key != message.job_id.as_str() {
            return Err(QueuePublishError::InvalidMessageId);
        }
        self.publisher
            .publish_job_queued(&message, &outbox.trace_headers)
            .await
    }

    async fn dispatch_message(
        &self,
        message: OutboxMessage,
    ) -> Result<MessageDispatch, RepositoryError> {
        let publish_error = match timeout(
            self.config.publish_timeout,
            self.publish_outbox_message(&message),
        )
        .await
        {
            Ok(Ok(())) => {
                self.repository
                    .mark_outbox_published(message.id, &self.config.dispatcher_id)
                    .await?;
                return Ok(MessageDispatch::Published);
            }
            Ok(Err(error)) => error.to_string(),
            Err(_) => format!(
                "queue acknowledgement timed out after {} ms",
                self.config.publish_timeout.as_millis()
            ),
        };

        self.metrics.record_outbox_publish_failure();
        let available_at = retry_at(
            Utc::now(),
            backoff_for(
                message.attempts,
                message.id,
                self.config.base_backoff,
                self.config.max_backoff,
            ),
        );
        self.repository
            .retry_outbox_message(
                message.id,
                &self.config.dispatcher_id,
                available_at,
                truncate_error(&publish_error),
            )
            .await?;
        Ok(MessageDispatch::Retried)
    }

    async fn refresh_backlog(&self) {
        match self.repository.pending_outbox_count().await {
            Ok(backlog) => self
                .metrics
                .set_outbox_backlog(i64::try_from(backlog).unwrap_or(i64::MAX)),
            Err(error) => {
                tracing::warn!(error = %error, "failed to refresh outbox backlog metric");
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MessageDispatch {
    Published,
    Retried,
}

fn retry_at(now: DateTime<Utc>, delay: Duration) -> DateTime<Utc> {
    let delay = ChronoDuration::from_std(delay).unwrap_or_else(|_| ChronoDuration::seconds(60));
    now.checked_add_signed(delay).unwrap_or(now)
}

fn backoff_for(attempt: u32, message_id: i64, base: Duration, max: Duration) -> Duration {
    let exponent = attempt.saturating_sub(1).min(31);
    let multiplier = 1_u128 << exponent;
    let uncapped_ms = base.as_millis().saturating_mul(multiplier);
    let capped_ms = uncapped_ms.min(max.as_millis());

    // Stable per-row/attempt jitter in [75%, 125%]. It avoids an additional RNG
    // dependency and still prevents replicas from retrying in lockstep.
    let mixed = (message_id as u64)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .rotate_left(attempt % 64);
    let jitter_percent = 75_u128 + u128::from(mixed % 51);
    let jittered_ms = capped_ms
        .saturating_mul(jitter_percent)
        .saturating_div(100)
        .min(max.as_millis())
        .max(1);
    Duration::from_millis(u64::try_from(jittered_ms).unwrap_or(u64::MAX))
}

fn truncate_error(error: &str) -> String {
    const MAX_BYTES: usize = 1_024;
    if error.len() <= MAX_BYTES {
        return error.to_owned();
    }
    let mut end = MAX_BYTES;
    while !error.is_char_boundary(end) {
        end -= 1;
    }
    error[..end].to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_backoff_is_bounded_and_jittered() {
        let base = Duration::from_millis(250);
        let max = Duration::from_secs(60);
        assert!(backoff_for(1, 1, base, max) >= Duration::from_millis(187));
        assert!(backoff_for(30, 1, base, max) <= max);
        assert_ne!(backoff_for(4, 1, base, max), backoff_for(4, 2, base, max));
    }

    #[test]
    fn reserved_message_id_cannot_be_overridden_by_trace_context() {
        let mut trace = BTreeMap::new();
        trace.insert(
            NATS_MESSAGE_ID_HEADER.to_ascii_lowercase(),
            "wrong".to_owned(),
        );
        trace.insert("traceparent".to_owned(), "00-abc-def-01".to_owned());
        let headers = build_headers("job_right", &trace).unwrap();
        let entries = headers
            .iter()
            .map(|(name, values)| {
                (
                    String::from_utf8_lossy(name.as_ref()).into_owned(),
                    values.iter().map(HeaderValue::as_str).collect::<Vec<_>>(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(entries[NATS_MESSAGE_ID_HEADER], ["job_right"]);
        assert_eq!(entries["traceparent"], ["00-abc-def-01"]);
    }

    #[test]
    fn wildcards_are_not_valid_publish_subjects() {
        assert!(validate_subject(JOBS_QUEUED_SUBJECT).is_ok());
        assert!(validate_subject("jobs.*").is_err());
        assert!(validate_subject("jobs.>").is_err());
    }
}

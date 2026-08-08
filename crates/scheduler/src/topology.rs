//! Idempotent JetStream topology provisioning with explicit drift policy.

use std::time::Duration;

use async_nats::jetstream::{
    self,
    consumer::{self, AckPolicy, DeliverPolicy, IntoConsumerConfig as _, pull},
    stream::{self, DiscardPolicy, RetentionPolicy, StorageType},
};
use thiserror::Error;

use crate::{
    config::Config,
    subjects::{
        JOB_ADVISORY_CONSUMER, JOB_ADVISORY_STREAM, JOB_DISPATCH_CONSUMER, JOB_DLQ_STREAM,
        JOB_QUEUE_STREAM, JOBS_DEAD_SUBJECT, JOBS_QUEUED_SUBJECT, MAX_DELIVERIES_ADVISORY_SUBJECT,
        TERMINATED_ADVISORY_SUBJECT,
    },
};

pub const MAX_JOB_MESSAGE_BYTES: i32 = 256 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopologyConfig {
    pub job_stream_max_bytes: i64,
    pub dlq_stream_max_bytes: i64,
    pub advisory_stream_max_bytes: i64,
    pub stream_replicas: usize,
    pub dlq_retention: Duration,
    pub advisory_retention: Duration,
    pub ack_wait: Duration,
    /// Includes one final scheduler delivery used to perform durable DLQ work.
    pub max_deliver: i64,
    pub max_ack_pending: i64,
    pub pull_batch_size: i64,
}

impl Default for TopologyConfig {
    fn default() -> Self {
        Self {
            job_stream_max_bytes: 10 * 1024 * 1024 * 1024,
            dlq_stream_max_bytes: 2 * 1024 * 1024 * 1024,
            advisory_stream_max_bytes: 256 * 1024 * 1024,
            stream_replicas: 1,
            dlq_retention: Duration::from_secs(30 * 24 * 60 * 60),
            advisory_retention: Duration::from_secs(30 * 24 * 60 * 60),
            ack_wait: Duration::from_secs(60),
            max_deliver: 4,
            max_ack_pending: 256,
            pull_batch_size: 64,
        }
    }
}

impl TryFrom<&Config> for TopologyConfig {
    type Error = TopologyError;

    fn try_from(config: &Config) -> Result<Self, Self::Error> {
        let pull_batch_size = i64::try_from(config.topology.pull_batch_size)
            .map_err(|_| TopologyError::InvalidConfig("pull batch size exceeds i64".to_owned()))?;
        Ok(Self {
            job_stream_max_bytes: config.topology.job_stream_max_bytes,
            dlq_stream_max_bytes: config.topology.dlq_stream_max_bytes,
            advisory_stream_max_bytes: config.topology.advisory_stream_max_bytes,
            stream_replicas: config.topology.stream_replicas,
            dlq_retention: config.topology.dlq_retention,
            advisory_retention: config.topology.advisory_retention,
            ack_wait: config.timing.ack_wait,
            max_deliver: i64::from(config.max_run_attempts) + 1,
            max_ack_pending: config.topology.max_ack_pending,
            pull_batch_size,
        })
    }
}

#[derive(Debug, Error)]
pub enum TopologyError {
    #[error("invalid JetStream topology configuration: {0}")]
    InvalidConfig(String),
    #[error(
        "incompatible JetStream `{resource}` field `{field}`: expected {expected}, found {actual}"
    )]
    Incompatible {
        resource: String,
        field: &'static str,
        expected: String,
        actual: String,
    },
    #[error("could not reconcile JetStream `{resource}`: {message}")]
    Operation { resource: String, message: String },
}

#[derive(Clone, Debug)]
pub struct ProvisionedTopology {
    pub job_stream: stream::Stream,
    pub job_consumer: consumer::PullConsumer,
    pub dlq_stream: stream::Stream,
    pub advisory_stream: stream::Stream,
    pub advisory_consumer: consumer::PullConsumer,
}

pub fn desired_job_stream(config: &TopologyConfig) -> stream::Config {
    stream::Config {
        name: JOB_QUEUE_STREAM.to_owned(),
        description: Some("Run Anywhere durable job work queue".to_owned()),
        subjects: vec![JOBS_QUEUED_SUBJECT.to_owned()],
        retention: RetentionPolicy::WorkQueue,
        storage: StorageType::File,
        discard: DiscardPolicy::New,
        max_bytes: config.job_stream_max_bytes,
        max_message_size: MAX_JOB_MESSAGE_BYTES,
        duplicate_window: config.advisory_retention,
        num_replicas: config.stream_replicas,
        no_ack: false,
        deny_delete: false,
        ..Default::default()
    }
}

pub fn desired_dlq_stream(config: &TopologyConfig) -> stream::Config {
    stream::Config {
        name: JOB_DLQ_STREAM.to_owned(),
        description: Some("Run Anywhere sanitized job dead letters".to_owned()),
        subjects: vec![JOBS_DEAD_SUBJECT.to_owned()],
        retention: RetentionPolicy::Limits,
        storage: StorageType::File,
        discard: DiscardPolicy::Old,
        max_bytes: config.dlq_stream_max_bytes,
        max_age: config.dlq_retention,
        duplicate_window: config.dlq_retention,
        max_message_size: MAX_JOB_MESSAGE_BYTES,
        num_replicas: config.stream_replicas,
        no_ack: false,
        ..Default::default()
    }
}

pub fn desired_advisory_stream(config: &TopologyConfig) -> stream::Config {
    stream::Config {
        name: JOB_ADVISORY_STREAM.to_owned(),
        description: Some("Durable max-delivery and termination advisories".to_owned()),
        subjects: vec![
            MAX_DELIVERIES_ADVISORY_SUBJECT.to_owned(),
            TERMINATED_ADVISORY_SUBJECT.to_owned(),
        ],
        retention: RetentionPolicy::Limits,
        storage: StorageType::File,
        discard: DiscardPolicy::Old,
        max_bytes: config.advisory_stream_max_bytes,
        max_age: config.advisory_retention,
        max_message_size: MAX_JOB_MESSAGE_BYTES,
        num_replicas: config.stream_replicas,
        no_ack: false,
        ..Default::default()
    }
}

pub fn desired_job_consumer(config: &TopologyConfig) -> pull::Config {
    pull::Config {
        durable_name: Some(JOB_DISPATCH_CONSUMER.to_owned()),
        name: Some(JOB_DISPATCH_CONSUMER.to_owned()),
        description: Some("Fenced scheduler dispatch and DLQ handling".to_owned()),
        deliver_policy: DeliverPolicy::All,
        ack_policy: AckPolicy::Explicit,
        ack_wait: config.ack_wait,
        max_deliver: config.max_deliver,
        filter_subject: JOBS_QUEUED_SUBJECT.to_owned(),
        max_ack_pending: config.max_ack_pending,
        max_batch: config.pull_batch_size,
        num_replicas: config.stream_replicas,
        memory_storage: false,
        ..Default::default()
    }
}

pub fn desired_advisory_consumer(config: &TopologyConfig) -> pull::Config {
    pull::Config {
        durable_name: Some(JOB_ADVISORY_CONSUMER.to_owned()),
        name: Some(JOB_ADVISORY_CONSUMER.to_owned()),
        description: Some("Durable recovery of exhausted and terminated job messages".to_owned()),
        deliver_policy: DeliverPolicy::All,
        ack_policy: AckPolicy::Explicit,
        ack_wait: config.ack_wait,
        max_deliver: -1,
        max_ack_pending: config.max_ack_pending.min(256),
        max_batch: config.pull_batch_size,
        num_replicas: config.stream_replicas,
        memory_storage: false,
        ..Default::default()
    }
}

pub async fn provision_topology(
    context: &jetstream::Context,
    config: &TopologyConfig,
) -> Result<ProvisionedTopology, TopologyError> {
    validate_config(config)?;
    let job_stream = ensure_stream(context, desired_job_stream(config)).await?;
    let dlq_stream = ensure_stream(context, desired_dlq_stream(config)).await?;
    let advisory_stream = ensure_stream(context, desired_advisory_stream(config)).await?;

    let job_consumer = ensure_pull_consumer(
        &job_stream,
        JOB_DISPATCH_CONSUMER,
        desired_job_consumer(config),
    )
    .await?;
    let advisory_consumer = ensure_pull_consumer(
        &advisory_stream,
        JOB_ADVISORY_CONSUMER,
        desired_advisory_consumer(config),
    )
    .await?;

    Ok(ProvisionedTopology {
        job_stream,
        job_consumer,
        dlq_stream,
        advisory_stream,
        advisory_consumer,
    })
}

async fn ensure_pull_consumer(
    stream: &stream::Stream,
    name: &str,
    desired_pull: pull::Config,
) -> Result<consumer::PullConsumer, TopologyError> {
    let desired_generic = desired_pull.clone().into_consumer_config();
    let existing = stream
        .get_or_create_consumer::<consumer::Config>(name, desired_generic.clone())
        .await
        .map_err(|error| operation(name, error))?;
    let needs_update =
        consumer_needs_update(&existing.cached_info().config, &desired_generic, name)?;
    if needs_update {
        stream
            .update_consumer(desired_pull)
            .await
            .map_err(|error| operation(name, error))
    } else {
        stream
            .get_consumer::<pull::Config>(name)
            .await
            .map_err(|error| operation(name, error))
    }
}

fn validate_config(config: &TopologyConfig) -> Result<(), TopologyError> {
    if config.job_stream_max_bytes <= 0
        || config.dlq_stream_max_bytes <= 0
        || config.advisory_stream_max_bytes <= 0
        || !(1..=5).contains(&config.stream_replicas)
        || config.dlq_retention.is_zero()
        || config.advisory_retention.is_zero()
        || config.ack_wait.is_zero()
        || config.max_deliver < 2
        || config.max_ack_pending <= 0
        || config.pull_batch_size <= 0
    {
        return Err(TopologyError::InvalidConfig(
            "sizes, retention, timing, delivery, replicas, and batch limits must be positive"
                .to_owned(),
        ));
    }
    Ok(())
}

async fn ensure_stream(
    context: &jetstream::Context,
    desired: stream::Config,
) -> Result<stream::Stream, TopologyError> {
    let stream_name = desired.name.clone();
    let existing = context
        .get_or_create_stream(desired.clone())
        .await
        .map_err(|error| operation(&stream_name, error))?;
    if let Some(updated) =
        reconciled_stream_config(&existing.cached_info().config, &desired, &stream_name)?
    {
        context
            .update_stream(updated)
            .await
            .map_err(|error| operation(&stream_name, error))?;
        context
            .get_stream(&stream_name)
            .await
            .map_err(|error| operation(&stream_name, error))
    } else {
        Ok(existing)
    }
}

fn reconciled_stream_config(
    actual: &stream::Config,
    desired: &stream::Config,
    resource: &str,
) -> Result<Option<stream::Config>, TopologyError> {
    incompatible_if(resource, "retention", desired.retention, actual.retention)?;
    incompatible_if(resource, "storage", desired.storage, actual.storage)?;
    incompatible_if(resource, "no_ack", false, actual.no_ack)?;
    incompatible_if(resource, "deny_delete", false, actual.deny_delete)?;
    incompatible_if(resource, "sealed", false, actual.sealed)?;
    let mut actual_subjects = actual.subjects.clone();
    let mut desired_subjects = desired.subjects.clone();
    actual_subjects.sort();
    desired_subjects.sort();
    incompatible_if(resource, "subjects", desired_subjects, actual_subjects)?;

    let mut updated = actual.clone();
    updated.description.clone_from(&desired.description);
    updated.max_bytes = desired.max_bytes;
    updated.discard = desired.discard;
    updated.max_age = desired.max_age;
    updated.duplicate_window = desired.duplicate_window;
    updated.max_message_size = desired.max_message_size;
    updated.num_replicas = desired.num_replicas;

    let managed_equal = actual.description == updated.description
        && actual.max_bytes == updated.max_bytes
        && actual.discard == updated.discard
        && actual.max_age == updated.max_age
        && actual.duplicate_window == updated.duplicate_window
        && actual.max_message_size == updated.max_message_size
        && actual.num_replicas == updated.num_replicas;
    Ok((!managed_equal).then_some(updated))
}

fn consumer_needs_update(
    actual: &consumer::Config,
    desired: &consumer::Config,
    resource: &str,
) -> Result<bool, TopologyError> {
    incompatible_if(
        resource,
        "durable_name",
        desired.durable_name.clone(),
        actual.durable_name.clone(),
    )?;
    incompatible_if(
        resource,
        "pull_consumer",
        None::<String>,
        actual.deliver_subject.clone(),
    )?;
    incompatible_if(
        resource,
        "deliver_group",
        None::<String>,
        actual.deliver_group.clone(),
    )?;
    incompatible_if(
        resource,
        "ack_policy",
        desired.ack_policy,
        actual.ack_policy,
    )?;
    incompatible_if(
        resource,
        "deliver_policy",
        desired.deliver_policy,
        actual.deliver_policy,
    )?;
    incompatible_if(resource, "headers_only", false, actual.headers_only)?;

    let expected_filter = &desired.filter_subject;
    let filter_matches = if expected_filter.is_empty() && desired.filter_subjects.is_empty() {
        actual.filter_subject.is_empty() && actual.filter_subjects.is_empty()
    } else {
        actual.filter_subject == *expected_filter && actual.filter_subjects.is_empty()
            || actual.filter_subject.is_empty()
                && actual.filter_subjects.as_slice() == [expected_filter.as_str()]
    };
    if !filter_matches {
        return Err(TopologyError::Incompatible {
            resource: resource.to_owned(),
            field: "filter_subject",
            expected: expected_filter.clone(),
            actual: format!("{} / {:?}", actual.filter_subject, actual.filter_subjects),
        });
    }

    Ok(actual.description != desired.description
        || actual.ack_wait != desired.ack_wait
        || actual.max_deliver != desired.max_deliver
        || actual.max_ack_pending != desired.max_ack_pending
        || actual.max_batch != desired.max_batch
        || actual.num_replicas != desired.num_replicas
        || actual.memory_storage != desired.memory_storage
        || !actual.backoff.is_empty())
}

fn incompatible_if<T>(
    resource: &str,
    field: &'static str,
    expected: T,
    actual: T,
) -> Result<(), TopologyError>
where
    T: std::fmt::Debug + PartialEq,
{
    if expected == actual {
        Ok(())
    } else {
        Err(TopologyError::Incompatible {
            resource: resource.to_owned(),
            field,
            expected: format!("{expected:?}"),
            actual: format!("{actual:?}"),
        })
    }
}

fn operation(resource: &str, error: impl std::fmt::Display) -> TopologyError {
    TopologyError::Operation {
        resource: resource.to_owned(),
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn desired_topology_has_part_four_invariants() {
        let config = TopologyConfig::default();
        let queue = desired_job_stream(&config);
        assert_eq!(queue.retention, RetentionPolicy::WorkQueue);
        assert_eq!(queue.storage, StorageType::File);
        assert_eq!(queue.discard, DiscardPolicy::New);
        assert_eq!(queue.max_bytes, 10 * 1024 * 1024 * 1024);
        assert_eq!(queue.max_message_size, 256 * 1024);
        assert_eq!(queue.duplicate_window, config.advisory_retention);
        assert_eq!(queue.subjects, [JOBS_QUEUED_SUBJECT]);

        let consumer = desired_job_consumer(&config);
        assert_eq!(
            consumer.durable_name.as_deref(),
            Some(JOB_DISPATCH_CONSUMER)
        );
        assert_eq!(consumer.ack_policy, AckPolicy::Explicit);
        assert_eq!(consumer.ack_wait, Duration::from_secs(60));
        assert_eq!(consumer.max_deliver, 4);
        assert_eq!(consumer.max_ack_pending, 256);

        let dlq = desired_dlq_stream(&config);
        assert_eq!(dlq.retention, RetentionPolicy::Limits);
        assert_eq!(dlq.max_age, Duration::from_secs(30 * 24 * 60 * 60));
        assert_eq!(dlq.duplicate_window, dlq.max_age);
        let advisories = desired_advisory_stream(&config);
        assert_eq!(advisories.subjects.len(), 2);
        assert!(
            advisories
                .subjects
                .contains(&MAX_DELIVERIES_ADVISORY_SUBJECT.to_owned())
        );
        let advisory_consumer = desired_advisory_consumer(&config);
        assert_eq!(
            advisory_consumer.durable_name.as_deref(),
            Some(JOB_ADVISORY_CONSUMER)
        );
        assert_eq!(advisory_consumer.ack_policy, AckPolicy::Explicit);
        assert!(
            advisories
                .subjects
                .contains(&TERMINATED_ADVISORY_SUBJECT.to_owned())
        );
    }

    #[test]
    fn safely_reconciles_stream_capacity_but_rejects_semantic_drift() {
        let desired = desired_job_stream(&TopologyConfig::default());
        let mut actual = desired.clone();
        actual.max_bytes = 123;
        let updated = reconciled_stream_config(&actual, &desired, JOB_QUEUE_STREAM)
            .unwrap()
            .unwrap();
        assert_eq!(updated.max_bytes, desired.max_bytes);

        actual.retention = RetentionPolicy::Limits;
        assert!(matches!(
            reconciled_stream_config(&actual, &desired, JOB_QUEUE_STREAM),
            Err(TopologyError::Incompatible {
                field: "retention",
                ..
            })
        ));
    }

    #[test]
    fn updates_consumer_timing_but_rejects_filter_drift() {
        let desired_pull = desired_job_consumer(&TopologyConfig::default());
        let desired = desired_pull.into_consumer_config();
        let mut actual = desired.clone();
        actual.ack_wait = Duration::from_secs(30);
        assert!(consumer_needs_update(&actual, &desired, JOB_DISPATCH_CONSUMER).unwrap());

        actual.filter_subject = "jobs.somewhere_else".to_owned();
        assert!(matches!(
            consumer_needs_update(&actual, &desired, JOB_DISPATCH_CONSUMER),
            Err(TopologyError::Incompatible {
                field: "filter_subject",
                ..
            })
        ));
    }
}

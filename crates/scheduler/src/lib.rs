//! Queue, scheduling, and reconciliation building blocks.
//!
//! The binary composes these modules into the active/passive scheduler.  The
//! modules remain independently testable so queue policy, fairness, and
//! destructive cleanup decisions do not depend on a running service.

#![forbid(unsafe_code)]

pub mod acknowledgements;
pub mod advisory;
pub mod config;
pub mod control;
pub mod delivery;
pub mod dispatcher;
pub mod dlq;
pub mod fairness;
pub mod leader;
pub mod metrics;
pub mod nats_connection;
pub mod reaper;
pub mod reconciler;
pub mod security;
mod service;
pub mod subjects;
pub mod topology;

pub use acknowledgements::{AckBinding, AckRegistry, AckRegistryError};
pub use advisory::{AdvisoryError, AdvisoryRecovery};
pub use config::{Config, ConfigError};
pub use control::{ControlPlane, ControlPlaneError, ControlPlaneHandle};
pub use delivery::{
    CanonicalDeliveryDecision, QueuePayloadError, classify_canonical_delivery, decode_queue_payload,
};
pub use dispatcher::{Dispatcher, DispatcherError};
pub use dlq::{DlqError, DlqPublishOutcome, DlqPublisher};
pub use fairness::{
    FairEntry, FairQueue, FairQueueConfig, FairQueueConfigError, FairQueueError, PriorityLane,
};
pub use leader::{LeaderGuard, LeaderLockError, SCHEDULER_ADVISORY_LOCK_KEY};
pub use metrics::{
    DlqMetricReason, ReaperMetricResult, RecoveryMetricResult, SchedulerMetrics, metrics_router,
};
pub use nats_connection::{NatsConnectionError, connect_nats};
pub use reaper::{
    DockerEngineReaper, MemoryRuntimeReaper, NoopRuntimeReaper, ReapDecision, ReapOutcome,
    ReaperError, RuntimeOwnership, RuntimeReaper, RuntimeResource, evaluate_reap,
};
pub use reconciler::{ReconcileReport, Reconciler, ReconcilerError};
pub use security::{
    API_INBOX_PREFIX, JETSTREAM_ACK_WILDCARD, JETSTREAM_API_WILDCARD, JOB_ADVISORY_ACK_SUBJECTS,
    JOB_QUEUE_ACK_SUBJECTS, NatsPermissionPolicy, NatsPrincipal, SCHEDULER_INBOX_PREFIX,
    SCHEDULER_JETSTREAM_API_SUBJECTS, worker_inbox_prefix,
};
pub use service::run;
pub use topology::{
    ProvisionedTopology, TopologyConfig, TopologyError, desired_advisory_consumer,
    desired_advisory_stream, desired_dlq_stream, desired_job_consumer, desired_job_stream,
    provision_topology,
};

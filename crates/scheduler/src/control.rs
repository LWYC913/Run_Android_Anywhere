//! Scheduler-owned worker control plane.
//!
//! Workers communicate over narrowly scoped request/reply subjects and never
//! receive a JetStream acknowledgement subject. Every job mutation is fenced
//! by both the worker identifier carried in the NATS subject and the exact
//! PostgreSQL lease. Queue acknowledgements stay in [`AckRegistry`].

use std::{collections::BTreeMap, time::Duration};

use async_nats::{Client, Message, Subscriber};
use futures_util::StreamExt;
use run_anywhere_contracts::{
    ControlResponse, DurationSeconds, JobId, JobOutcome, JobResult, JobState,
    JobStateTransitionRequest, TransitionEvidence, WorkerHeartbeat, WorkerId, WorkerRegistration,
};
use run_anywhere_repository::{LeaseGuard, Repository, RepositoryError};
use serde::de::DeserializeOwned;
use thiserror::Error;
use tokio::{
    sync::watch,
    task::{JoinHandle, JoinSet},
};

use crate::{AckRegistry, AckRegistryError, Config, security::worker_inbox_prefix};

pub const WORKER_REGISTER_WILDCARD: &str = "control.workers.*.register";
pub const WORKER_HEARTBEAT_WILDCARD: &str = "control.workers.*.heartbeat";
pub const JOB_TRANSITION_WILDCARD: &str = "control.jobs.*.transition";
pub const JOB_RESULT_WILDCARD: &str = "control.jobs.*.result";

/// Core NATS control messages are bounded independently of the JetStream
/// queue. This also prevents a credential with publish permission on a control
/// subject from forcing unbounded JSON allocations.
pub const MAX_CONTROL_PAYLOAD_BYTES: usize = 256 * 1024;
const MAX_CONCURRENT_CONTROL_REQUESTS: usize = 128;

#[derive(Clone)]
pub struct ControlPlane {
    client: Client,
    repository: Repository,
    config: Config,
    acknowledgements: AckRegistry,
}

impl ControlPlane {
    pub const fn new(
        client: Client,
        repository: Repository,
        config: Config,
        acknowledgements: AckRegistry,
    ) -> Self {
        Self {
            client,
            repository,
            config,
            acknowledgements,
        }
    }

    /// Subscribe to all four stable worker-control endpoints and start one
    /// cancellable task. The caller should only start this while holding the
    /// scheduler leadership lock.
    pub async fn start(self) -> Result<ControlPlaneHandle, ControlPlaneError> {
        let registrations = self
            .client
            .subscribe(WORKER_REGISTER_WILDCARD)
            .await
            .map_err(nats_error)?;
        let heartbeats = self
            .client
            .subscribe(WORKER_HEARTBEAT_WILDCARD)
            .await
            .map_err(nats_error)?;
        let transitions = self
            .client
            .subscribe(JOB_TRANSITION_WILDCARD)
            .await
            .map_err(nats_error)?;
        let results = self
            .client
            .subscribe(JOB_RESULT_WILDCARD)
            .await
            .map_err(nats_error)?;
        // A flush makes the four subscriptions visible to the server before
        // the leader advertises the control plane as ready.
        self.client.flush().await.map_err(nats_error)?;

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let task =
            tokio::spawn(self.run(registrations, heartbeats, transitions, results, shutdown_rx));
        Ok(ControlPlaneHandle {
            shutdown: shutdown_tx,
            task: Some(task),
        })
    }

    async fn run(
        self,
        mut registrations: Subscriber,
        mut heartbeats: Subscriber,
        mut transitions: Subscriber,
        mut results: Subscriber,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), ControlPlaneError> {
        let mut handlers = JoinSet::new();
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    let _ = changed;
                    return Ok(());
                }
                completed = handlers.join_next(), if !handlers.is_empty() => {
                    match completed {
                        Some(Ok(Ok(()))) => {}
                        Some(Ok(Err(error))) => {
                            tracing::warn!(error = %error, "worker control request failed");
                        }
                        Some(Err(error)) => {
                            tracing::warn!(error = %error, "worker control request task failed");
                        }
                        None => {}
                    }
                }
                message = registrations.next(), if handlers.len() < MAX_CONCURRENT_CONTROL_REQUESTS => {
                    spawn_handler(&mut handlers, self.clone(), message)?;
                }
                message = heartbeats.next(), if handlers.len() < MAX_CONCURRENT_CONTROL_REQUESTS => {
                    spawn_handler(&mut handlers, self.clone(), message)?;
                }
                message = transitions.next(), if handlers.len() < MAX_CONCURRENT_CONTROL_REQUESTS => {
                    spawn_handler(&mut handlers, self.clone(), message)?;
                }
                message = results.next(), if handlers.len() < MAX_CONCURRENT_CONTROL_REQUESTS => {
                    spawn_handler(&mut handlers, self.clone(), message)?;
                }
            }
        }
    }

    /// Process one request and publish exactly one typed response when a reply
    /// inbox is present. A fire-and-forget message is deliberately ignored:
    /// control mutations require request/reply semantics.
    pub async fn handle_message(&self, message: Message) -> Result<(), ControlPlaneError> {
        let Some(reply) = message.reply.clone() else {
            return Ok(());
        };
        let Ok(route) = parse_control_subject(message.subject.as_str()) else {
            return Ok(());
        };
        if !reply_is_scoped_to_worker(reply.as_str(), route.worker_id()) {
            tracing::warn!(
                worker_id = %route.worker_id(),
                "ignored a control request with an out-of-scope reply subject"
            );
            return Ok(());
        }
        let response = if message.payload.len() > MAX_CONTROL_PAYLOAD_BYTES {
            self.response(ResponseClass::Retry)
        } else {
            self.process(message.subject.as_str(), message.payload.as_ref())
                .await
        };
        let payload = serde_json::to_vec(&response)?;
        self.client
            .publish(reply, payload.into())
            .await
            .map_err(nats_error)
    }

    async fn process(&self, subject: &str, payload: &[u8]) -> ControlResponse {
        let Ok(route) = parse_control_subject(subject) else {
            return self.response(ResponseClass::Retry);
        };
        match route {
            ControlRoute::Register(subject_worker) => {
                self.register_worker(subject_worker, payload).await
            }
            ControlRoute::Heartbeat(subject_worker) => {
                self.record_heartbeat(subject_worker, payload).await
            }
            ControlRoute::Transition(subject_worker) => {
                self.transition_job(subject_worker, payload).await
            }
            ControlRoute::Result(subject_worker) => {
                self.record_result(subject_worker, payload).await
            }
        }
    }

    async fn register_worker(&self, subject_worker: WorkerId, payload: &[u8]) -> ControlResponse {
        let Some(registration) = decode_payload::<WorkerRegistration>(payload) else {
            return self.response(ResponseClass::Retry);
        };
        if !worker_matches_subject(&subject_worker, &registration.worker_id) {
            return ControlResponse::StaleLease;
        }
        match self.repository.upsert_worker(registration).await {
            Ok(_) => ControlResponse::Accepted,
            Err(error) => self.response(classify_repository_error(&error)),
        }
    }

    async fn record_heartbeat(&self, subject_worker: WorkerId, payload: &[u8]) -> ControlResponse {
        let Some(heartbeat) = decode_payload::<WorkerHeartbeat>(payload) else {
            return self.response(ResponseClass::Retry);
        };
        if !worker_matches_subject(&subject_worker, &heartbeat.worker_id) {
            return ControlResponse::StaleLease;
        }
        let Ok(lease_ttl) = chrono::Duration::from_std(self.config.timing.database_lease_ttl)
        else {
            return self.response(ResponseClass::Retry);
        };
        let receipt = match self.repository.record_heartbeat(heartbeat, lease_ttl).await {
            Ok(receipt) => receipt,
            Err(error) => return self.response(classify_repository_error(&error)),
        };

        let extended = receipt.extended;
        for extension in &extended {
            if let Err(error) = self
                .acknowledgements
                .progress(&extension.job_id, &subject_worker, &extension.lease_id)
                .await
            {
                return self.response(classify_ack_error(&error));
            }
        }
        ControlResponse::Heartbeat {
            extended,
            cancel_requested: receipt.cancel_requested,
            stale: receipt.rejected,
        }
    }

    async fn transition_job(&self, subject_worker: WorkerId, payload: &[u8]) -> ControlResponse {
        let Some(request) = decode_payload::<JobStateTransitionRequest>(payload) else {
            return self.response(ResponseClass::Retry);
        };
        if !worker_matches_subject(&subject_worker, &request.worker_id) {
            return ControlResponse::StaleLease;
        }
        if request.validate().is_err() {
            return self.response(ResponseClass::Retry);
        }
        let lease = LeaseGuard {
            worker_id: request.worker_id.clone(),
            lease_id: request.lease_id.clone(),
        };
        match self
            .repository
            .transition_job_state(
                &request.job_id,
                request.from,
                request.to,
                request.evidence,
                Some(&lease),
                BTreeMap::new(),
            )
            .await
        {
            Ok(_) if request.to.is_terminal() => self.confirm_ack(&request.job_id, &lease).await,
            Ok(_) => ControlResponse::Accepted,
            Err(error) if is_cas_or_conflict(&error) => {
                self.classify_replayed_transition(&request, &lease).await
            }
            Err(error) => self.response(classify_repository_error(&error)),
        }
    }

    async fn classify_replayed_transition(
        &self,
        request: &JobStateTransitionRequest,
        lease: &LeaseGuard,
    ) -> ControlResponse {
        let snapshot = match self
            .repository
            .get_job_scheduling_snapshot(&request.job_id)
            .await
        {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => return ControlResponse::StaleLease,
            Err(error) => return self.response(classify_repository_error(&error)),
        };
        if snapshot.job.state != request.to {
            return ControlResponse::StaleLease;
        }
        if request.to.is_terminal() {
            return self.confirm_ack(&request.job_id, lease).await;
        }
        if snapshot.lease.as_ref() == Some(lease) {
            ControlResponse::Accepted
        } else {
            ControlResponse::StaleLease
        }
    }

    async fn record_result(&self, subject_worker: WorkerId, payload: &[u8]) -> ControlResponse {
        let Some(result) = decode_payload::<JobResult>(payload) else {
            return self.response(ResponseClass::Retry);
        };
        if !worker_matches_subject(&subject_worker, &result.worker_id) {
            return ControlResponse::StaleLease;
        }
        let lease = LeaseGuard {
            worker_id: result.worker_id.clone(),
            lease_id: result.lease_id.clone(),
        };
        let state = match self.repository.record_job_result(result.clone()).await {
            Ok(job) => job.state,
            Err(error) if is_cas_or_conflict(&error) => {
                return self.classify_replayed_result(&result, &lease).await;
            }
            Err(error) => return self.response(classify_repository_error(&error)),
        };
        self.drive_result_finalizer(&result, &lease, state).await
    }

    async fn classify_replayed_result(
        &self,
        result: &JobResult,
        lease: &LeaseGuard,
    ) -> ControlResponse {
        let snapshot = match self
            .repository
            .get_job_scheduling_snapshot(&result.job_id)
            .await
        {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => return ControlResponse::StaleLease,
            Err(error) => return self.response(classify_repository_error(&error)),
        };
        if snapshot.job.state.is_terminal()
            && snapshot.job.outcome == Some(result.outcome)
            && snapshot.job.worker_id.as_ref() == Some(&result.worker_id)
        {
            return self.confirm_ack(&result.job_id, lease).await;
        }
        if snapshot.lease.as_ref() == Some(lease)
            && snapshot.pending_outcome == Some(result.outcome)
        {
            return self
                .drive_result_finalizer(result, lease, snapshot.job.state)
                .await;
        }
        ControlResponse::StaleLease
    }

    async fn drive_result_finalizer(
        &self,
        result: &JobResult,
        lease: &LeaseGuard,
        mut state: JobState,
    ) -> ControlResponse {
        let evidence = TransitionEvidence {
            pending_outcome: Some(result.outcome),
            artifacts_finalized: result.artifacts_finalized,
            cleanup_completed: result.cleanup_completed,
        };

        // At most three state changes are needed: enter artifact collection,
        // enter cleanup, and commit the terminal outcome. Extra iterations let
        // a same-lease concurrent request win one edge without making this
        // request fail spuriously.
        for _ in 0..6 {
            if state.is_terminal() {
                return self.confirm_ack(&result.job_id, lease).await;
            }
            let Some(target) = next_result_state(state, result) else {
                return ControlResponse::Accepted;
            };
            match self
                .repository
                .transition_job_state(
                    &result.job_id,
                    state,
                    target,
                    evidence,
                    Some(lease),
                    BTreeMap::new(),
                )
                .await
            {
                Ok(job) => state = job.state,
                Err(error) if is_cas_or_conflict(&error) => {
                    let snapshot = match self
                        .repository
                        .get_job_scheduling_snapshot(&result.job_id)
                        .await
                    {
                        Ok(Some(snapshot)) => snapshot,
                        Ok(None) => return ControlResponse::StaleLease,
                        Err(error) => {
                            return self.response(classify_repository_error(&error));
                        }
                    };
                    if snapshot.job.state.is_terminal() {
                        if snapshot.job.outcome == Some(result.outcome)
                            && snapshot.job.worker_id.as_ref() == Some(&result.worker_id)
                        {
                            return self.confirm_ack(&result.job_id, lease).await;
                        }
                        return ControlResponse::StaleLease;
                    }
                    if snapshot.lease.as_ref() != Some(lease)
                        || snapshot.pending_outcome != Some(result.outcome)
                    {
                        return ControlResponse::StaleLease;
                    }
                    state = snapshot.job.state;
                }
                Err(error) => return self.response(classify_repository_error(&error)),
            }
        }
        self.response(ResponseClass::Retry)
    }

    async fn confirm_ack(&self, job_id: &JobId, lease: &LeaseGuard) -> ControlResponse {
        match self
            .acknowledgements
            .confirm_ack(job_id, &lease.worker_id, &lease.lease_id)
            .await
        {
            Ok(()) => ControlResponse::Accepted,
            Err(AckRegistryError::Missing(_)) => ControlResponse::Accepted,
            Err(error) => self.response(classify_ack_error(&error)),
        }
    }

    fn response(&self, class: ResponseClass) -> ControlResponse {
        response_for_class(class, self.config.timing.reconciliation_interval)
    }
}

fn spawn_handler(
    handlers: &mut JoinSet<Result<(), ControlPlaneError>>,
    control: ControlPlane,
    message: Option<Message>,
) -> Result<(), ControlPlaneError> {
    let message = message.ok_or(ControlPlaneError::SubscriptionClosed)?;
    handlers.spawn(async move { control.handle_message(message).await });
    Ok(())
}

/// Handle returned by [`ControlPlane::start`]. Calling `shutdown` lets the
/// subscription loop exit before the leadership connection is released.
pub struct ControlPlaneHandle {
    shutdown: watch::Sender<bool>,
    task: Option<JoinHandle<Result<(), ControlPlaneError>>>,
}

impl ControlPlaneHandle {
    /// Whether the leader-owned subscription task exited unexpectedly.
    pub fn is_finished(&self) -> bool {
        self.task.as_ref().is_none_or(JoinHandle::is_finished)
    }

    pub async fn shutdown(mut self) -> Result<(), ControlPlaneError> {
        let _ = self.shutdown.send(true);
        let Some(task) = self.task.take() else {
            return Ok(());
        };
        task.await
            .map_err(|error| ControlPlaneError::Task(error.to_string()))?
    }
}

impl Drop for ControlPlaneHandle {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[derive(Debug, Error)]
pub enum ControlPlaneError {
    #[error("NATS control-plane operation failed: {0}")]
    Nats(String),
    #[error("a control-plane subscription closed unexpectedly")]
    SubscriptionClosed,
    #[error("control-plane response serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("control-plane task failed: {0}")]
    Task(String),
}

fn nats_error(error: impl ToString) -> ControlPlaneError {
    ControlPlaneError::Nats(error.to_string())
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ControlRoute {
    Register(WorkerId),
    Heartbeat(WorkerId),
    Transition(WorkerId),
    Result(WorkerId),
}

impl ControlRoute {
    const fn worker_id(&self) -> &WorkerId {
        match self {
            Self::Register(worker_id)
            | Self::Heartbeat(worker_id)
            | Self::Transition(worker_id)
            | Self::Result(worker_id) => worker_id,
        }
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
enum SubjectParseError {
    #[error("unsupported control subject shape")]
    UnsupportedShape,
    #[error("control subject contains an invalid worker identifier")]
    InvalidWorkerId,
}

fn parse_control_subject(subject: &str) -> Result<ControlRoute, SubjectParseError> {
    let tokens = subject.split('.').collect::<Vec<_>>();
    let [control, namespace, worker, operation] = tokens.as_slice() else {
        return Err(SubjectParseError::UnsupportedShape);
    };
    if *control != "control" {
        return Err(SubjectParseError::UnsupportedShape);
    }
    let worker =
        WorkerId::new((*worker).to_owned()).map_err(|_| SubjectParseError::InvalidWorkerId)?;
    match (*namespace, *operation) {
        ("workers", "register") => Ok(ControlRoute::Register(worker)),
        ("workers", "heartbeat") => Ok(ControlRoute::Heartbeat(worker)),
        ("jobs", "transition") => Ok(ControlRoute::Transition(worker)),
        ("jobs", "result") => Ok(ControlRoute::Result(worker)),
        _ => Err(SubjectParseError::UnsupportedShape),
    }
}

fn decode_payload<T: DeserializeOwned>(payload: &[u8]) -> Option<T> {
    serde_json::from_slice(payload).ok()
}

fn worker_matches_subject(subject_worker: &WorkerId, payload_worker: &WorkerId) -> bool {
    subject_worker == payload_worker
}

/// A scheduler response may only return to the private inbox namespace of the
/// worker identity embedded in the authenticated control subject. This check
/// must happen before processing the payload because NATS response permissions
/// can otherwise make an attacker-selected reply subject temporarily writable.
fn reply_is_scoped_to_worker(reply: &str, worker_id: &WorkerId) -> bool {
    let prefix = worker_inbox_prefix(worker_id);
    let Some(suffix) = reply.strip_prefix(&prefix) else {
        return false;
    };
    let Some(tokens) = suffix.strip_prefix('.') else {
        return false;
    };
    !tokens.is_empty()
        && tokens
            .split('.')
            .all(|token| !token.is_empty() && !matches!(token, "*" | ">"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ResponseClass {
    StaleLease,
    QuotaExceeded,
    Retry,
}

const fn classify_repository_error(error: &RepositoryError) -> ResponseClass {
    match error {
        RepositoryError::QuotaExceeded { .. } => ResponseClass::QuotaExceeded,
        RepositoryError::NotFound { .. }
        | RepositoryError::CompareAndSwapLost { .. }
        | RepositoryError::Conflict(_)
        | RepositoryError::InvalidTransition(_) => ResponseClass::StaleLease,
        RepositoryError::Validation(_)
        | RepositoryError::Decode { .. }
        | RepositoryError::Sqlx(_)
        | RepositoryError::Migration(_) => ResponseClass::Retry,
    }
}

const fn classify_ack_error(error: &AckRegistryError) -> ResponseClass {
    match error {
        AckRegistryError::StaleLease(_) => ResponseClass::StaleLease,
        AckRegistryError::Missing(_) | AckRegistryError::JetStream(_) => ResponseClass::Retry,
    }
}

const fn is_cas_or_conflict(error: &RepositoryError) -> bool {
    matches!(
        error,
        RepositoryError::CompareAndSwapLost { .. } | RepositoryError::Conflict(_)
    )
}

fn response_for_class(class: ResponseClass, retry_after: Duration) -> ControlResponse {
    let retry_after_seconds = DurationSeconds::new(retry_after.as_secs().max(1))
        .expect("the retry interval is clamped to at least one second");
    match class {
        ResponseClass::StaleLease => ControlResponse::StaleLease,
        ResponseClass::QuotaExceeded => ControlResponse::QuotaExceeded {
            retry_after_seconds,
        },
        ResponseClass::Retry => ControlResponse::Retry {
            retry_after_seconds,
        },
    }
}

fn next_result_state(state: JobState, result: &JobResult) -> Option<JobState> {
    match state {
        JobState::Queued
        | JobState::Claimed
        | JobState::ProvisioningRuntime
        | JobState::Booting
        | JobState::InstallingApk
        | JobState::RunningTests
        | JobState::DebugAvailable => Some(JobState::CollectingArtifacts),
        JobState::CollectingArtifacts if result.artifacts_finalized => Some(JobState::CleaningUp),
        JobState::CleaningUp if result.cleanup_completed => Some(terminal_state(result.outcome)),
        JobState::CollectingArtifacts | JobState::CleaningUp => None,
        JobState::Passed
        | JobState::Failed
        | JobState::Cancelled
        | JobState::TimedOut
        | JobState::InfraFailed => None,
    }
}

const fn terminal_state(outcome: JobOutcome) -> JobState {
    match outcome {
        JobOutcome::Passed => JobState::Passed,
        JobOutcome::Failed => JobState::Failed,
        JobOutcome::Cancelled => JobState::Cancelled,
        JobOutcome::TimedOut => JobState::TimedOut,
        JobOutcome::InfraFailed => JobState::InfraFailed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use run_anywhere_contracts::LeaseId;
    use run_anywhere_repository::ProjectQuota;

    #[test]
    fn parses_only_the_four_scoped_control_subjects() {
        let worker = WorkerId::new("wrk_alpha").unwrap();
        assert_eq!(
            parse_control_subject("control.workers.wrk_alpha.register"),
            Ok(ControlRoute::Register(worker.clone()))
        );
        assert_eq!(
            parse_control_subject("control.workers.wrk_alpha.heartbeat"),
            Ok(ControlRoute::Heartbeat(worker.clone()))
        );
        assert_eq!(
            parse_control_subject("control.jobs.wrk_alpha.transition"),
            Ok(ControlRoute::Transition(worker.clone()))
        );
        assert_eq!(
            parse_control_subject("control.jobs.wrk_alpha.result"),
            Ok(ControlRoute::Result(worker))
        );

        assert_eq!(
            parse_control_subject("control.jobs.wrk_alpha.result.extra"),
            Err(SubjectParseError::UnsupportedShape)
        );
        assert_eq!(
            parse_control_subject("control.jobs.not-a-worker.result"),
            Err(SubjectParseError::InvalidWorkerId)
        );
        assert_eq!(
            parse_control_subject("workers.wrk_alpha.dispatch"),
            Err(SubjectParseError::UnsupportedShape)
        );
    }

    #[test]
    fn repository_errors_map_to_stable_typed_classes() {
        assert_eq!(
            classify_repository_error(&RepositoryError::QuotaExceeded {
                project_id: "proj_a".to_owned(),
                quota: ProjectQuota::ConcurrentJobs,
                current: 2,
                limit: 2,
            }),
            ResponseClass::QuotaExceeded
        );
        assert_eq!(
            classify_repository_error(&RepositoryError::CompareAndSwapLost {
                entity: "job lease",
                id: "job_a".to_owned(),
            }),
            ResponseClass::StaleLease
        );
        assert_eq!(
            classify_repository_error(&RepositoryError::NotFound {
                entity: "worker",
                id: "wrk_a".to_owned(),
            }),
            ResponseClass::StaleLease
        );
        assert_eq!(
            classify_repository_error(&RepositoryError::Validation("bad input".to_owned())),
            ResponseClass::Retry
        );
        assert_eq!(
            classify_repository_error(&RepositoryError::Sqlx(sqlx::Error::RowNotFound)),
            ResponseClass::Retry
        );
    }

    #[test]
    fn payload_worker_must_match_the_authenticated_subject_scope() {
        let subject_worker = WorkerId::new("wrk_subject").unwrap();
        let matching_payload = WorkerId::new("wrk_subject").unwrap();
        let other_payload = WorkerId::new("wrk_other").unwrap();
        assert!(worker_matches_subject(&subject_worker, &matching_payload));
        assert!(!worker_matches_subject(&subject_worker, &other_payload));
    }

    #[test]
    fn reply_subject_must_stay_inside_the_subject_workers_private_inbox() {
        let worker = WorkerId::new("wrk_alpha").unwrap();

        assert!(reply_is_scoped_to_worker(
            "_INBOX.workers.wrk_alpha.request.1",
            &worker
        ));
        assert!(!reply_is_scoped_to_worker("jobs.queued", &worker));
        assert!(!reply_is_scoped_to_worker("jobs.dead", &worker));
        assert!(!reply_is_scoped_to_worker(
            "workers.wrk_beta.dispatch",
            &worker
        ));
        assert!(!reply_is_scoped_to_worker(
            "_INBOX.workers.wrk_beta.request.1",
            &worker
        ));
        assert!(!reply_is_scoped_to_worker(
            "_INBOX.workers.wrk_alpha",
            &worker
        ));
        assert!(!reply_is_scoped_to_worker(
            "_INBOX.workers.wrk_alpha.>",
            &worker
        ));
    }

    #[test]
    fn acknowledgement_errors_distinguish_wrong_lease_from_retryable_loss() {
        let job_id = JobId::new("job_alpha").unwrap();
        assert_eq!(
            classify_ack_error(&AckRegistryError::StaleLease(job_id.clone())),
            ResponseClass::StaleLease
        );
        assert_eq!(
            classify_ack_error(&AckRegistryError::Missing(job_id)),
            ResponseClass::Retry
        );
        assert_eq!(
            classify_ack_error(&AckRegistryError::JetStream("unavailable".to_owned())),
            ResponseClass::Retry
        );
    }

    #[test]
    fn response_classification_uses_a_positive_bounded_retry_hint() {
        let response = response_for_class(ResponseClass::Retry, Duration::ZERO);
        assert_eq!(
            response,
            ControlResponse::Retry {
                retry_after_seconds: DurationSeconds::new(1).unwrap(),
            }
        );
        assert_eq!(
            response_for_class(ResponseClass::QuotaExceeded, Duration::from_secs(10)),
            ControlResponse::QuotaExceeded {
                retry_after_seconds: DurationSeconds::new(10).unwrap(),
            }
        );
    }

    #[test]
    fn every_outcome_has_the_matching_terminal_state() {
        assert_eq!(terminal_state(JobOutcome::Passed), JobState::Passed);
        assert_eq!(terminal_state(JobOutcome::Failed), JobState::Failed);
        assert_eq!(terminal_state(JobOutcome::Cancelled), JobState::Cancelled);
        assert_eq!(terminal_state(JobOutcome::TimedOut), JobState::TimedOut);
        assert_eq!(
            terminal_state(JobOutcome::InfraFailed),
            JobState::InfraFailed
        );
    }

    #[test]
    fn finalizer_waits_for_artifact_and_cleanup_evidence() {
        let base = JobResult {
            job_id: JobId::new("job_alpha").unwrap(),
            worker_id: WorkerId::new("wrk_alpha").unwrap(),
            lease_id: LeaseId::new("lease_alpha").unwrap(),
            outcome: JobOutcome::Passed,
            artifact_ids: Vec::new(),
            artifacts_finalized: false,
            cleanup_completed: false,
            error: None,
            completed_at: "2026-07-22T00:00:00Z".parse().unwrap(),
        };
        assert_eq!(
            next_result_state(JobState::RunningTests, &base),
            Some(JobState::CollectingArtifacts)
        );
        assert_eq!(
            next_result_state(JobState::CollectingArtifacts, &base),
            None
        );

        let artifacts_done = JobResult {
            artifacts_finalized: true,
            ..base.clone()
        };
        assert_eq!(
            next_result_state(JobState::CollectingArtifacts, &artifacts_done),
            Some(JobState::CleaningUp)
        );
        assert_eq!(
            next_result_state(JobState::CleaningUp, &artifacts_done),
            None
        );

        let cleanup_done = JobResult {
            cleanup_completed: true,
            ..artifacts_done
        };
        assert_eq!(
            next_result_state(JobState::CleaningUp, &cleanup_done),
            Some(JobState::Passed)
        );
    }
}

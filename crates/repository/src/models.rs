use std::{collections::BTreeMap, fmt};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use run_anywhere_contracts::{
    Artifact, AuthScope, DebugSessionId, DebugSessionMode, Job, JobEvent, JobId, JobLeaseExtension,
    JobState, LeaseId, Project, ProjectId, Sha256, UploadId, UploadKind, Uri, WebhookId, WorkerId,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{RepositoryError, RepositoryResult};

/// SHA-256 digest used to look up an API key. The plaintext key is never persisted.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ApiKeyHash([u8; 32]);

impl ApiKeyHash {
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for ApiKeyHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ApiKeyHash([redacted])")
    }
}

/// A newly generated API key. Debug output is intentionally redacted.
#[derive(Clone, PartialEq, Eq)]
pub struct ApiKeySecret(String);

impl ApiKeySecret {
    pub(crate) fn new(value: String) -> Self {
        Self(value)
    }

    pub fn expose_secret(&self) -> &str {
        &self.0
    }

    pub fn into_secret(self) -> String {
        self.0
    }
}

impl fmt::Debug for ApiKeySecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ApiKeySecret([redacted])")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApiKeyRecord {
    pub id: String,
    pub project_id: ProjectId,
    pub scopes: Vec<AuthScope>,
    pub created_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreatedApiKey {
    pub key: ApiKeySecret,
    pub record: ApiKeyRecord,
}

/// A project and its first API key, committed as one database transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreatedProject {
    pub project: Project,
    pub api_key: CreatedApiKey,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredUpload {
    pub id: UploadId,
    pub project_id: ProjectId,
    pub kind: UploadKind,
    pub s3_key: String,
    pub sha256: Sha256,
    pub size_bytes: u64,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredArtifact {
    pub artifact: Artifact,
    pub s3_key: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredArtifactPage {
    pub items: Vec<StoredArtifact>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredDebugSession {
    pub id: DebugSessionId,
    pub job_id: JobId,
    pub jti: String,
    pub created_by: String,
    pub mode: DebugSessionMode,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuditEntry {
    pub id: i64,
    pub actor: String,
    pub action: String,
    pub subject: String,
    pub timestamp: DateTime<Utc>,
    pub payload: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreatedDebugSession {
    pub session: StoredDebugSession,
    pub audit: AuditEntry,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobCursor {
    pub created_at: DateTime<Utc>,
    pub job_id: JobId,
}

impl JobCursor {
    pub fn encode(&self) -> RepositoryResult<String> {
        encode_route_cursor("jobs", self.clone())
    }

    pub fn decode(value: &str) -> RepositoryResult<Self> {
        decode_route_cursor("jobs", value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobListQuery {
    pub project_id: ProjectId,
    pub state: Option<JobState>,
    pub cursor: Option<String>,
    pub limit: u32,
}

impl JobListQuery {
    pub const DEFAULT_LIMIT: u32 = 50;
    pub const MAX_LIMIT: u32 = 200;

    pub fn new(project_id: ProjectId) -> Self {
        Self {
            project_id,
            state: None,
            cursor: None,
            limit: Self::DEFAULT_LIMIT,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreatedJob {
    pub job: Job,
    pub was_created: bool,
    /// The exact persisted event for a winning create. Replays do not emit an event.
    pub queued_event: Option<JobEvent>,
}

/// Result of an idempotent state mutation such as cancellation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobMutation {
    pub job: Job,
    pub was_changed: bool,
    /// The event committed with the mutation, absent on an idempotent replay.
    pub event: Option<JobEvent>,
}

/// A durably persisted message awaiting external publication or delivery.
#[derive(Clone, Debug, PartialEq)]
pub struct OutboxMessage {
    pub id: i64,
    /// Stable delivery ID. For `jobs.queued`, this is the job ID.
    pub event_key: String,
    pub subject: String,
    pub payload: Value,
    pub trace_headers: BTreeMap<String, String>,
    pub available_at: DateTime<Utc>,
    pub attempts: u32,
    pub locked_by: Option<String>,
    pub locked_at: Option<DateTime<Utc>>,
    pub published_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl OutboxMessage {
    pub fn payload_bytes(&self) -> RepositoryResult<Vec<u8>> {
        serde_json::to_vec(&self.payload)
            .map_err(|error| RepositoryError::decode("outbox_messages.payload", error))
    }
}

/// Internal payload for one durable webhook recipient. The outbox key is
/// stable across retries so receivers can deduplicate at-least-once delivery.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookOutboxPayload {
    pub webhook_id: WebhookId,
    pub url: Uri,
    pub event: JobEvent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaseGuard {
    pub worker_id: WorkerId,
    pub lease_id: LeaseId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeartbeatReceipt {
    pub recorded_at: DateTime<Utc>,
    pub extended: Vec<JobLeaseExtension>,
    pub rejected: Vec<JobLeaseExtension>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StaleJobCriteria {
    pub lease_expired_before: DateTime<Utc>,
    pub worker_heartbeat_before: DateTime<Utc>,
    pub limit: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StaleJob {
    pub job_id: JobId,
    pub worker_id: WorkerId,
    pub lease_id: LeaseId,
    pub state: JobState,
    pub delivery_attempts: u32,
    pub lease_expires_at: DateTime<Utc>,
    pub worker_last_heartbeat_at: DateTime<Utc>,
    pub lease_expired_before: DateTime<Utc>,
    pub worker_heartbeat_before: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecoveryDisposition {
    Requeued(Job),
    Finalizing(Job),
    LostRace,
}

pub(crate) fn checked_limit(limit: u32) -> RepositoryResult<i64> {
    if limit == 0 || limit > JobListQuery::MAX_LIMIT {
        return Err(RepositoryError::Validation(format!(
            "limit must be between 1 and {}",
            JobListQuery::MAX_LIMIT
        )));
    }
    Ok(i64::from(limit))
}

pub(crate) const CONTROL_PLANE_PAGE_SIZE: i64 = 50;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ArtifactCursor {
    pub job_id: String,
    pub created_at: DateTime<Utc>,
    pub artifact_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkerCursor {
    pub registered_at: DateTime<Utc>,
    pub worker_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RuntimeProfileCursor {
    pub profile_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct RouteCursor<T> {
    route: String,
    key: T,
}

pub(crate) fn encode_route_cursor<T: Serialize>(
    route: &'static str,
    key: T,
) -> RepositoryResult<String> {
    let bytes = serde_json::to_vec(&RouteCursor {
        route: route.to_owned(),
        key,
    })
    .map_err(|error| RepositoryError::decode("cursor", error))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

pub(crate) fn decode_route_cursor<T: for<'de> Deserialize<'de>>(
    expected_route: &'static str,
    value: &str,
) -> RepositoryResult<T> {
    let bytes = URL_SAFE_NO_PAD.decode(value).map_err(|error| {
        RepositoryError::Validation(format!("invalid {expected_route} cursor: {error}"))
    })?;
    let cursor: RouteCursor<Value> = serde_json::from_slice(&bytes).map_err(|error| {
        RepositoryError::Validation(format!("invalid {expected_route} cursor: {error}"))
    })?;
    if cursor.route != expected_route {
        return Err(RepositoryError::Validation(format!(
            "cursor is for route `{}`, not `{expected_route}`",
            cursor.route
        )));
    }
    serde_json::from_value(cursor.key).map_err(|error| {
        RepositoryError::Validation(format!("invalid {expected_route} cursor: {error}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursors_round_trip_and_are_route_tagged() {
        let job_cursor = JobCursor {
            created_at: Utc::now(),
            job_id: JobId::new("job_cursor_test").expect("valid test ID"),
        };
        assert_eq!(
            JobCursor::decode(&job_cursor.encode().expect("encode job cursor"))
                .expect("decode job cursor"),
            job_cursor
        );

        let worker_cursor = encode_route_cursor(
            "workers",
            WorkerCursor {
                registered_at: Utc::now(),
                worker_id: "wrk_cursor_test".to_owned(),
            },
        )
        .expect("encode worker cursor");
        assert!(matches!(
            JobCursor::decode(&worker_cursor),
            Err(RepositoryError::Validation(message)) if message.contains("not `jobs`")
        ));
    }
}

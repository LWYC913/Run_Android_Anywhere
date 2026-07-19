use chrono::{DateTime, Utc};
use run_anywhere_contracts::{
    Artifact, ArtifactId, DebugSessionId, DurationSeconds, Job, JobEvent, JobEventId, JobId,
    JobSummary, Project, ProjectId, RuntimeProfile, RuntimeProfileId, Sha256, UploadId, Uri,
    Webhook, WebhookId, WorkerId, WorkerStatus,
};
use serde_json::Value;
use sqlx::FromRow;

use crate::{
    ApiKeyHash, ApiKeyRecord, AuditEntry, RepositoryError, RepositoryResult, StoredArtifact,
    StoredDebugSession, StoredUpload,
    codec::{
        artifacts_from_value, automation_from_value, decode_enum, failure_from_value,
        payload_from_value, to_u16, to_u32, to_u64,
    },
};

#[derive(Debug, FromRow)]
pub(crate) struct ProjectRow {
    pub id: String,
    pub name: String,
    pub owner: String,
    pub created_at: DateTime<Utc>,
}

impl TryFrom<ProjectRow> for Project {
    type Error = RepositoryError;

    fn try_from(row: ProjectRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: ProjectId::new(row.id)
                .map_err(|error| RepositoryError::decode("projects.id", error))?,
            name: row.name,
            owner: row.owner,
            created_at: row.created_at,
        })
    }
}

#[derive(Debug, FromRow)]
pub(crate) struct ApiKeyRow {
    pub id: String,
    pub project_id: String,
    pub key_hash: Vec<u8>,
    pub scopes: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
}

impl ApiKeyRow {
    pub(crate) fn into_record(self) -> RepositoryResult<ApiKeyRecord> {
        let hash: [u8; 32] = self
            .key_hash
            .try_into()
            .map_err(|_| RepositoryError::decode("api_keys.key_hash", "expected 32 bytes"))?;
        let _validated_hash = ApiKeyHash::new(hash);
        Ok(ApiKeyRecord {
            id: self.id,
            project_id: ProjectId::new(self.project_id)
                .map_err(|error| RepositoryError::decode("api_keys.project_id", error))?,
            scopes: self
                .scopes
                .into_iter()
                .map(|scope| decode_enum("api_keys.scopes", scope))
                .collect::<RepositoryResult<_>>()?,
            created_at: self.created_at,
            last_used_at: self.last_used_at,
            revoked_at: self.revoked_at,
        })
    }
}

#[derive(Debug, FromRow)]
pub(crate) struct UploadRow {
    pub id: String,
    pub project_id: String,
    pub kind: String,
    pub s3_key: String,
    pub sha256: String,
    pub size_bytes: i64,
    pub created_at: DateTime<Utc>,
}

impl TryFrom<UploadRow> for StoredUpload {
    type Error = RepositoryError;

    fn try_from(row: UploadRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: UploadId::new(row.id)
                .map_err(|error| RepositoryError::decode("uploads.id", error))?,
            project_id: ProjectId::new(row.project_id)
                .map_err(|error| RepositoryError::decode("uploads.project_id", error))?,
            kind: decode_enum("uploads.kind", row.kind)?,
            s3_key: row.s3_key,
            sha256: Sha256::new(row.sha256)
                .map_err(|error| RepositoryError::decode("uploads.sha256", error))?,
            size_bytes: to_u64("uploads.size_bytes", row.size_bytes)?,
            created_at: row.created_at,
        })
    }
}

#[derive(Clone, Debug, FromRow)]
pub(crate) struct RuntimeProfileRow {
    pub id: String,
    pub android_api: i32,
    pub device_profile: String,
    pub abi: String,
    pub host_arch: String,
    pub runtime_kind: String,
    pub image_ref: String,
    pub isolation_tier: String,
}

impl TryFrom<RuntimeProfileRow> for RuntimeProfile {
    type Error = RepositoryError;

    fn try_from(row: RuntimeProfileRow) -> Result<Self, Self::Error> {
        let profile = Self {
            id: RuntimeProfileId::new(row.id)
                .map_err(|error| RepositoryError::decode("runtime_profiles.id", error))?,
            android_api: to_u16("runtime_profiles.android_api", row.android_api)?,
            device_profile: row.device_profile,
            abi: decode_enum("runtime_profiles.abi", row.abi)?,
            host_arch: decode_enum("runtime_profiles.host_arch", row.host_arch)?,
            runtime_kind: decode_enum("runtime_profiles.runtime_kind", row.runtime_kind)?,
            image_ref: row.image_ref,
            isolation_tier: decode_enum("runtime_profiles.isolation_tier", row.isolation_tier)?,
        };
        profile
            .validate()
            .map_err(|error| RepositoryError::decode("runtime_profiles", error))?;
        Ok(profile)
    }
}

#[derive(Clone, Debug, FromRow)]
pub(crate) struct JobRow {
    pub id: String,
    pub project_id: String,
    pub apk_upload_id: String,
    pub test_upload_id: Option<String>,
    pub runtime_profile_id: String,
    pub worker_id: Option<String>,
    pub mode: String,
    pub min_isolation: String,
    pub automation: Value,
    pub requested_artifacts: Value,
    pub timeout_seconds: i64,
    pub state: String,
    pub pending_outcome: Option<String>,
    pub outcome: Option<String>,
    pub failure: Option<Value>,
    pub artifacts_finalized: bool,
    pub lease_id: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub delivery_attempts: i64,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

impl JobRow {
    pub(crate) fn into_job(self) -> RepositoryResult<Job> {
        Ok(Job {
            id: JobId::new(self.id).map_err(|error| RepositoryError::decode("jobs.id", error))?,
            project_id: ProjectId::new(self.project_id)
                .map_err(|error| RepositoryError::decode("jobs.project_id", error))?,
            apk_upload_id: UploadId::new(self.apk_upload_id)
                .map_err(|error| RepositoryError::decode("jobs.apk_upload_id", error))?,
            test_upload_id: self
                .test_upload_id
                .map(UploadId::new)
                .transpose()
                .map_err(|error| RepositoryError::decode("jobs.test_upload_id", error))?,
            runtime_profile: RuntimeProfileId::new(self.runtime_profile_id)
                .map_err(|error| RepositoryError::decode("jobs.runtime_profile_id", error))?,
            mode: decode_enum("jobs.mode", self.mode)?,
            min_isolation: decode_enum("jobs.min_isolation", self.min_isolation)?,
            automation: automation_from_value(self.automation)?,
            artifacts: artifacts_from_value(self.requested_artifacts)?,
            timeout_seconds: DurationSeconds::new(to_u64(
                "jobs.timeout_seconds",
                self.timeout_seconds,
            )?)
            .map_err(|error| RepositoryError::decode("jobs.timeout_seconds", error))?,
            state: decode_enum("jobs.state", self.state)?,
            outcome: self
                .outcome
                .map(|value| decode_enum("jobs.outcome", value))
                .transpose()?,
            failure: failure_from_value(self.failure)?,
            worker_id: self
                .worker_id
                .map(WorkerId::new)
                .transpose()
                .map_err(|error| RepositoryError::decode("jobs.worker_id", error))?,
            created_at: self.created_at,
            started_at: self.started_at,
            finished_at: self.finished_at,
        })
    }

    pub(crate) fn to_summary(&self) -> RepositoryResult<JobSummary> {
        Ok(JobSummary {
            id: JobId::new(self.id.clone())
                .map_err(|error| RepositoryError::decode("jobs.id", error))?,
            project_id: ProjectId::new(self.project_id.clone())
                .map_err(|error| RepositoryError::decode("jobs.project_id", error))?,
            runtime_profile: RuntimeProfileId::new(self.runtime_profile_id.clone())
                .map_err(|error| RepositoryError::decode("jobs.runtime_profile_id", error))?,
            mode: decode_enum("jobs.mode", self.mode.clone())?,
            state: decode_enum("jobs.state", self.state.clone())?,
            outcome: self
                .outcome
                .clone()
                .map(|value| decode_enum("jobs.outcome", value))
                .transpose()?,
            created_at: self.created_at,
            started_at: self.started_at,
            finished_at: self.finished_at,
        })
    }
}

#[derive(Debug, FromRow)]
pub(crate) struct JobEventRow {
    pub id: String,
    pub sequence: i64,
    pub job_id: String,
    pub timestamp: DateTime<Utc>,
    pub event_type: String,
    pub state: Option<String>,
    pub payload: Value,
}

impl TryFrom<JobEventRow> for JobEvent {
    type Error = RepositoryError;

    fn try_from(row: JobEventRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: JobEventId::new(row.id)
                .map_err(|error| RepositoryError::decode("job_events.id", error))?,
            job_id: JobId::new(row.job_id)
                .map_err(|error| RepositoryError::decode("job_events.job_id", error))?,
            sequence: to_u64("job_events.sequence", row.sequence)?,
            timestamp: row.timestamp,
            event_type: row.event_type,
            state: row
                .state
                .map(|state| decode_enum("job_events.state", state))
                .transpose()?,
            payload: payload_from_value("job_events.payload", row.payload)?,
        })
    }
}

#[derive(Debug, FromRow)]
pub(crate) struct ArtifactRow {
    pub id: String,
    pub job_id: String,
    pub kind: String,
    pub s3_key: String,
    pub file_name: Option<String>,
    pub size_bytes: i64,
    pub sha256: String,
    pub created_at: DateTime<Utc>,
}

impl TryFrom<ArtifactRow> for StoredArtifact {
    type Error = RepositoryError;

    fn try_from(row: ArtifactRow) -> Result<Self, Self::Error> {
        Ok(Self {
            artifact: Artifact {
                id: ArtifactId::new(row.id)
                    .map_err(|error| RepositoryError::decode("artifacts.id", error))?,
                job_id: JobId::new(row.job_id)
                    .map_err(|error| RepositoryError::decode("artifacts.job_id", error))?,
                kind: decode_enum("artifacts.kind", row.kind)?,
                file_name: row.file_name,
                size_bytes: to_u64("artifacts.size_bytes", row.size_bytes)?,
                sha256: Sha256::new(row.sha256)
                    .map_err(|error| RepositoryError::decode("artifacts.sha256", error))?,
                created_at: row.created_at,
                download_url: None,
                download_expires_at: None,
            },
            s3_key: row.s3_key,
        })
    }
}

#[derive(Debug, FromRow)]
pub(crate) struct WorkerRow {
    pub id: String,
    pub runtimes: Vec<String>,
    pub kvm: bool,
    pub gpu: bool,
    pub arch: String,
    pub capacity: i64,
    pub active_jobs: i64,
    pub state: String,
    pub last_heartbeat_at: DateTime<Utc>,
}

impl WorkerRow {
    pub(crate) fn into_status(self) -> RepositoryResult<WorkerStatus> {
        Ok(WorkerStatus {
            worker_id: WorkerId::new(self.id)
                .map_err(|error| RepositoryError::decode("workers.id", error))?,
            runtimes: self
                .runtimes
                .into_iter()
                .map(|runtime| decode_enum("workers.runtimes", runtime))
                .collect::<RepositoryResult<_>>()?,
            kvm: self.kvm,
            gpu: self.gpu,
            arch: decode_enum("workers.arch", self.arch)?,
            capacity: to_u32("workers.capacity", self.capacity)?,
            active_jobs: to_u32("workers.active_jobs", self.active_jobs)?,
            state: decode_enum("workers.state", self.state)?,
            last_seen: self.last_heartbeat_at,
        })
    }
}

#[derive(Debug, FromRow)]
pub(crate) struct DebugSessionRow {
    pub id: String,
    pub job_id: String,
    pub jti: String,
    pub created_by: String,
    pub mode: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
}

impl TryFrom<DebugSessionRow> for StoredDebugSession {
    type Error = RepositoryError;

    fn try_from(row: DebugSessionRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: DebugSessionId::new(row.id)
                .map_err(|error| RepositoryError::decode("debug_sessions.id", error))?,
            job_id: JobId::new(row.job_id)
                .map_err(|error| RepositoryError::decode("debug_sessions.job_id", error))?,
            jti: row.jti,
            created_by: row.created_by,
            mode: decode_enum("debug_sessions.mode", row.mode)?,
            created_at: row.created_at,
            expires_at: row.expires_at,
            ended_at: row.ended_at,
        })
    }
}

#[derive(Debug, FromRow)]
pub(crate) struct AuditRow {
    pub id: i64,
    pub actor: String,
    pub action: String,
    pub subject: String,
    pub timestamp: DateTime<Utc>,
    pub payload: Value,
}

impl TryFrom<AuditRow> for AuditEntry {
    type Error = RepositoryError;

    fn try_from(row: AuditRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.id,
            actor: row.actor,
            action: row.action,
            subject: row.subject,
            timestamp: row.timestamp,
            payload: payload_from_value("audit_log.payload", row.payload)?,
        })
    }
}

#[derive(Debug, FromRow)]
pub(crate) struct WebhookRow {
    pub id: String,
    pub project_id: String,
    pub url: String,
    pub events: Vec<String>,
    pub active: bool,
    pub created_at: DateTime<Utc>,
}

impl TryFrom<WebhookRow> for Webhook {
    type Error = RepositoryError;

    fn try_from(row: WebhookRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: WebhookId::new(row.id)
                .map_err(|error| RepositoryError::decode("webhooks.id", error))?,
            project_id: ProjectId::new(row.project_id)
                .map_err(|error| RepositoryError::decode("webhooks.project_id", error))?,
            url: Uri::new(row.url)
                .map_err(|error| RepositoryError::decode("webhooks.url", error))?,
            events: row
                .events
                .into_iter()
                .map(|event| decode_enum("webhooks.events", event))
                .collect::<RepositoryResult<_>>()?,
            active: row.active,
            created_at: row.created_at,
        })
    }
}

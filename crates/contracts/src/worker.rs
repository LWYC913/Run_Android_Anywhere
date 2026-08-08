use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

use crate::{
    ArtifactId, ArtifactSelection, AutomationSpec, DurationSeconds, FailureDetail, HostArch,
    IsolationTier, JobId, JobMode, JobOutcome, JobState, LeaseId, ProjectId, RuntimeKind,
    RuntimeProfile, RuntimeProfileId, TransitionError, TransitionEvidence, UploadId, WorkerId,
    validate_transition,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct WorkerRegistration {
    pub worker_id: WorkerId,
    pub runtimes: Vec<RuntimeKind>,
    pub kvm: bool,
    pub gpu: bool,
    pub arch: HostArch,
    pub capacity: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct WorkerHeartbeat {
    pub worker_id: WorkerId,
    pub active_jobs: u32,
    pub capacity: u32,
    pub runtimes: Vec<RuntimeKind>,
    pub kvm: bool,
    pub gpu: bool,
    pub arch: HostArch,
    pub lease_extends: Vec<JobLeaseExtension>,
    pub last_seen: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct JobQueued {
    pub job_id: JobId,
    pub project_id: ProjectId,
    /// Canonical profile data is resolved from PostgreSQL by the scheduler.
    /// Keeping only the identifier prevents image references or registry
    /// credentials from entering the durable work queue.
    pub runtime_profile_id: RuntimeProfileId,
    pub min_isolation: IsolationTier,
    pub timeout_seconds: DurationSeconds,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CurrentJobQueuedWire {
    job_id: JobId,
    project_id: ProjectId,
    runtime_profile_id: RuntimeProfileId,
    min_isolation: IsolationTier,
    timeout_seconds: DurationSeconds,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyJobQueuedWire {
    job_id: JobId,
    project_id: ProjectId,
    runtime_profile: RuntimeProfile,
    min_isolation: IsolationTier,
    timeout_seconds: DurationSeconds,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum JobQueuedWire {
    Current(CurrentJobQueuedWire),
    Legacy(LegacyJobQueuedWire),
}

impl<'de> Deserialize<'de> for JobQueued {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(match JobQueuedWire::deserialize(deserializer)? {
            JobQueuedWire::Current(current) => Self {
                job_id: current.job_id,
                project_id: current.project_id,
                runtime_profile_id: current.runtime_profile_id,
                min_isolation: current.min_isolation,
                timeout_seconds: current.timeout_seconds,
            },
            JobQueuedWire::Legacy(legacy) => Self {
                job_id: legacy.job_id,
                project_id: legacy.project_id,
                runtime_profile_id: legacy.runtime_profile.id,
                min_isolation: legacy.min_isolation,
                timeout_seconds: legacy.timeout_seconds,
            },
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct JobClaim {
    pub job_id: JobId,
    pub project_id: ProjectId,
    pub worker_id: WorkerId,
    pub lease_id: LeaseId,
    pub apk_upload_id: UploadId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub test_upload_id: Option<UploadId>,
    pub runtime_profile: RuntimeProfile,
    pub mode: JobMode,
    pub min_isolation: IsolationTier,
    pub automation: AutomationSpec,
    pub artifacts: ArtifactSelection,
    pub timeout_seconds: DurationSeconds,
    pub claimed_at: DateTime<Utc>,
    pub lease_expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct JobLeaseExtension {
    pub job_id: JobId,
    pub lease_id: LeaseId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct JobResult {
    pub job_id: JobId,
    pub worker_id: WorkerId,
    pub lease_id: LeaseId,
    pub outcome: JobOutcome,
    pub artifact_ids: Vec<ArtifactId>,
    pub artifacts_finalized: bool,
    pub cleanup_completed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<FailureDetail>,
    pub completed_at: DateTime<Utc>,
}

/// A scheduler-owned dispatch request sent to one specifically addressed worker.
///
/// JetStream acknowledgement subjects are intentionally absent: only the scheduler
/// holds and operates the queue acknowledgement. `trace_context` is limited by the
/// sender to tracing propagation headers and must never contain credentials.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct JobDispatch {
    pub claim: JobClaim,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub trace_context: BTreeMap<String, String>,
}

/// A worker-requested state transition fenced by the exact worker lease.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct JobStateTransitionRequest {
    pub job_id: JobId,
    pub worker_id: WorkerId,
    pub lease_id: LeaseId,
    pub from: JobState,
    pub to: JobState,
    pub evidence: TransitionEvidence,
}

impl JobStateTransitionRequest {
    /// Validate the requested lifecycle edge before it is sent or persisted.
    ///
    /// Lease ownership must still be checked atomically by the repository.
    pub fn validate(&self) -> Result<(), TransitionError> {
        validate_transition(self.from, self.to, &self.evidence)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ControlDisposition {
    Accepted,
    Heartbeat,
    StaleLease,
    QuotaExceeded,
    Retry,
}

/// Typed reply used by the scheduler's internal request/reply control subjects.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "disposition", rename_all = "snake_case", deny_unknown_fields)]
pub enum ControlResponse {
    Accepted,
    Heartbeat {
        extended: Vec<JobLeaseExtension>,
        cancel_requested: Vec<JobLeaseExtension>,
        stale: Vec<JobLeaseExtension>,
    },
    StaleLease,
    QuotaExceeded {
        retry_after_seconds: DurationSeconds,
    },
    Retry {
        retry_after_seconds: DurationSeconds,
    },
}

impl ControlResponse {
    pub const fn disposition(&self) -> ControlDisposition {
        match self {
            Self::Accepted => ControlDisposition::Accepted,
            Self::Heartbeat { .. } => ControlDisposition::Heartbeat,
            Self::StaleLease => ControlDisposition::StaleLease,
            Self::QuotaExceeded { .. } => ControlDisposition::QuotaExceeded,
            Self::Retry { .. } => ControlDisposition::Retry,
        }
    }
}

/// Stable, bounded reason labels safe to publish and use as metric dimensions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum JobDeadLetterReason {
    MalformedMessage,
    CanonicalJobInconsistent,
    AttemptsExhausted,
}

/// Sanitized record published to `jobs.dead` after durable outbox insertion.
///
/// This intentionally has no raw source payload, arbitrary metadata, or free-form
/// error message, preventing credentials and signed URLs from leaking into the DLQ.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct JobDeadLetter {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_id: Option<JobId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_id: Option<ProjectId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(minimum = 1)]
    pub source_stream_sequence: Option<u64>,
    #[schema(minimum = 1)]
    pub attempt: u32,
    pub reason: JobDeadLetterReason,
    pub dead_lettered_at: DateTime<Utc>,
}

impl JobDeadLetter {
    pub fn new(
        job_id: Option<JobId>,
        project_id: Option<ProjectId>,
        source_stream_sequence: Option<u64>,
        attempt: u32,
        reason: JobDeadLetterReason,
        dead_lettered_at: DateTime<Utc>,
    ) -> Result<Self, JobDeadLetterValidationError> {
        let dead_letter = Self {
            job_id,
            project_id,
            source_stream_sequence,
            attempt,
            reason,
            dead_lettered_at,
        };
        dead_letter.validate()?;
        Ok(dead_letter)
    }

    pub const fn validate(&self) -> Result<(), JobDeadLetterValidationError> {
        if self.attempt == 0 {
            Err(JobDeadLetterValidationError::ZeroAttempt)
        } else if matches!(self.source_stream_sequence, Some(0)) {
            Err(JobDeadLetterValidationError::ZeroSourceStreamSequence)
        } else if match self.reason {
            JobDeadLetterReason::AttemptsExhausted => self.job_id.is_none(),
            JobDeadLetterReason::MalformedMessage
            | JobDeadLetterReason::CanonicalJobInconsistent => {
                self.source_stream_sequence.is_none()
            }
        } {
            Err(JobDeadLetterValidationError::MissingStableIdentity)
        } else {
            Ok(())
        }
    }

    /// Idempotency key used for the durable DLQ outbox entry.
    pub fn idempotency_key(&self) -> Result<String, JobDeadLetterValidationError> {
        self.validate()?;
        match self.reason {
            JobDeadLetterReason::AttemptsExhausted => self
                .job_id
                .as_ref()
                .map(|job_id| format!("dlq:{job_id}:{}", self.attempt))
                .ok_or(JobDeadLetterValidationError::MissingStableIdentity),
            JobDeadLetterReason::MalformedMessage => self
                .source_stream_sequence
                .map(|sequence| format!("dlq:JOB_QUEUE:{sequence}:malformed_message"))
                .ok_or(JobDeadLetterValidationError::MissingStableIdentity),
            JobDeadLetterReason::CanonicalJobInconsistent => self
                .source_stream_sequence
                .map(|sequence| format!("dlq:JOB_QUEUE:{sequence}:canonical_job_inconsistent"))
                .ok_or(JobDeadLetterValidationError::MissingStableIdentity),
        }
    }
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum JobDeadLetterValidationError {
    #[error("dead-letter attempt must be at least one")]
    ZeroAttempt,
    #[error("dead-letter source stream sequence must be at least one")]
    ZeroSourceStreamSequence,
    #[error("dead letter must identify either a job or a source stream sequence")]
    MissingStableIdentity,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn claim() -> JobClaim {
        JobClaim {
            job_id: JobId::new("job_123").unwrap(),
            project_id: ProjectId::new("proj_123").unwrap(),
            worker_id: WorkerId::new("wrk_123").unwrap(),
            lease_id: LeaseId::new("lease_123").unwrap(),
            apk_upload_id: UploadId::new("upl_apk").unwrap(),
            test_upload_id: None,
            runtime_profile: RuntimeProfile {
                id: crate::RuntimeProfileId::new("rtp_android_35").unwrap(),
                android_api: 35,
                device_profile: "pixel_6".to_owned(),
                abi: crate::AndroidAbi::X86_64,
                host_arch: HostArch::X86_64,
                runtime_kind: RuntimeKind::AndroidEmulatorContainer,
                image_ref: "registry.example/android:35".to_owned(),
                isolation_tier: IsolationTier::VmIsolated,
            },
            mode: JobMode::HeadlessCi,
            min_isolation: IsolationTier::VmIsolated,
            automation: AutomationSpec::BuiltInSmoke,
            artifacts: ArtifactSelection {
                screenshots: true,
                video: false,
                logcat: true,
                junit: true,
            },
            timeout_seconds: DurationSeconds::new(900).unwrap(),
            claimed_at: "2026-07-13T00:00:00Z".parse().unwrap(),
            lease_expires_at: "2026-07-13T00:01:00Z".parse().unwrap(),
        }
    }

    #[test]
    fn optional_test_upload_is_omitted_from_claim_wire_shape() {
        let value = serde_json::to_value(claim()).unwrap();
        assert!(value.get("test_upload_id").is_none());
        assert_eq!(value["mode"], "headless_ci");
    }

    #[test]
    fn queued_work_contains_only_a_runtime_profile_identifier() {
        let queued = JobQueued {
            job_id: JobId::new("job_123").unwrap(),
            project_id: ProjectId::new("proj_123").unwrap(),
            runtime_profile_id: RuntimeProfileId::new("rtp_android_35").unwrap(),
            min_isolation: IsolationTier::VmIsolated,
            timeout_seconds: DurationSeconds::new(900).unwrap(),
        };

        let value = serde_json::to_value(queued).unwrap();
        assert_eq!(value["runtime_profile_id"], "rtp_android_35");
        assert!(value.get("runtime_profile").is_none());
        assert!(!value.to_string().contains("image_ref"));
    }

    #[test]
    fn legacy_queued_profiles_are_drained_but_never_reserialized() {
        let legacy = serde_json::json!({
            "job_id": "job_123",
            "project_id": "proj_123",
            "runtime_profile": {
                "id": "rtp_android_35",
                "android_api": 35,
                "device_profile": "pixel_6",
                "abi": "x86_64",
                "host_arch": "x86_64",
                "runtime_kind": "android_emulator_container",
                "image_ref": "https://legacy.invalid/image?signature=must-not-survive",
                "isolation_tier": "vm_isolated"
            },
            "min_isolation": "vm_isolated",
            "timeout_seconds": 900
        });

        let queued: JobQueued = serde_json::from_value(legacy).unwrap();
        assert_eq!(queued.runtime_profile_id.as_str(), "rtp_android_35");
        let current = serde_json::to_value(queued).unwrap();
        assert!(current.get("runtime_profile").is_none());
        assert!(!current.to_string().contains("signature"));
    }

    #[test]
    fn dispatch_round_trips_without_exposing_queue_ack_data() {
        let dispatch = JobDispatch {
            claim: claim(),
            trace_context: BTreeMap::from([(
                "traceparent".to_owned(),
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_owned(),
            )]),
        };

        let value = serde_json::to_value(&dispatch).unwrap();
        assert!(value.get("ack_subject").is_none());
        assert!(value.get("queue_message").is_none());
        assert_eq!(
            serde_json::from_value::<JobDispatch>(value).unwrap(),
            dispatch
        );
    }

    #[test]
    fn state_transition_request_validates_the_lifecycle_edge() {
        let request = JobStateTransitionRequest {
            job_id: JobId::new("job_123").unwrap(),
            worker_id: WorkerId::new("wrk_123").unwrap(),
            lease_id: LeaseId::new("lease_123").unwrap(),
            from: JobState::Claimed,
            to: JobState::ProvisioningRuntime,
            evidence: TransitionEvidence::default(),
        };
        assert!(request.validate().is_ok());

        let invalid = JobStateTransitionRequest {
            to: JobState::Passed,
            ..request
        };
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn control_responses_have_stable_tagged_wire_shapes() {
        let retry_after_seconds = DurationSeconds::new(30).unwrap();
        let responses = [
            ControlResponse::Accepted,
            ControlResponse::Heartbeat {
                extended: Vec::new(),
                cancel_requested: Vec::new(),
                stale: Vec::new(),
            },
            ControlResponse::StaleLease,
            ControlResponse::QuotaExceeded {
                retry_after_seconds,
            },
            ControlResponse::Retry {
                retry_after_seconds,
            },
        ];

        for response in &responses {
            let value = serde_json::to_value(response).unwrap();
            assert_eq!(
                &serde_json::from_value::<ControlResponse>(value).unwrap(),
                response
            );
        }
        assert_eq!(
            serde_json::to_value(ControlResponse::Accepted).unwrap(),
            serde_json::json!({ "disposition": "accepted" })
        );
        assert_eq!(
            responses[3].disposition(),
            ControlDisposition::QuotaExceeded
        );
    }

    #[test]
    fn dead_letter_is_validated_stable_and_sanitized() {
        let dead_letter = JobDeadLetter::new(
            Some(JobId::new("job_123").unwrap()),
            Some(ProjectId::new("proj_123").unwrap()),
            Some(42),
            3,
            JobDeadLetterReason::AttemptsExhausted,
            "2026-07-13T00:03:00Z".parse().unwrap(),
        )
        .unwrap();
        assert_eq!(dead_letter.idempotency_key().unwrap(), "dlq:job_123:3");

        let value = serde_json::to_value(&dead_letter).unwrap();
        let properties = value.as_object().unwrap();
        assert_eq!(properties.len(), 6);
        assert!(properties.get("raw_payload").is_none());
        assert!(properties.get("error_message").is_none());
        assert_eq!(
            serde_json::from_value::<JobDeadLetter>(value).unwrap(),
            dead_letter
        );
        assert_eq!(
            JobDeadLetter::new(
                Some(JobId::new("job_123").unwrap()),
                Some(ProjectId::new("proj_123").unwrap()),
                Some(42),
                0,
                JobDeadLetterReason::AttemptsExhausted,
                "2026-07-13T00:03:00Z".parse().unwrap(),
            ),
            Err(JobDeadLetterValidationError::ZeroAttempt)
        );

        let malformed = JobDeadLetter::new(
            None,
            None,
            Some(88),
            4,
            JobDeadLetterReason::MalformedMessage,
            "2026-07-13T00:04:00Z".parse().unwrap(),
        )
        .unwrap();
        assert_eq!(
            malformed.idempotency_key().unwrap(),
            "dlq:JOB_QUEUE:88:malformed_message"
        );

        let canonical_poison = JobDeadLetter::new(
            Some(JobId::new("job_123").unwrap()),
            Some(ProjectId::new("proj_123").unwrap()),
            Some(88),
            99,
            JobDeadLetterReason::CanonicalJobInconsistent,
            "2026-07-13T00:04:00Z".parse().unwrap(),
        )
        .unwrap();
        assert_eq!(
            canonical_poison.idempotency_key().unwrap(),
            "dlq:JOB_QUEUE:88:canonical_job_inconsistent"
        );
    }

    #[test]
    fn new_internal_contracts_reject_unknown_fields() {
        let mut value = serde_json::to_value(JobDispatch {
            claim: claim(),
            trace_context: BTreeMap::new(),
        })
        .unwrap();
        value.as_object_mut().unwrap().insert(
            "ack_subject".to_owned(),
            serde_json::json!("$JS.ACK.secret"),
        );

        assert!(serde_json::from_value::<JobDispatch>(value).is_err());
    }
}

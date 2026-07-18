use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    ArtifactId, ArtifactSelection, AutomationSpec, DurationSeconds, FailureDetail, HostArch,
    IsolationTier, JobId, JobMode, JobOutcome, LeaseId, ProjectId, RuntimeKind, RuntimeProfile,
    UploadId, WorkerId,
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct JobQueued {
    pub job_id: JobId,
    pub project_id: ProjectId,
    pub runtime_profile: RuntimeProfile,
    pub min_isolation: IsolationTier,
    pub timeout_seconds: DurationSeconds,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn optional_test_upload_is_omitted_from_claim_wire_shape() {
        let claim = JobClaim {
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
        };

        let value = serde_json::to_value(claim).unwrap();
        assert!(value.get("test_upload_id").is_none());
        assert_eq!(value["mode"], "headless_ci");
    }
}

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

use crate::{
    AndroidAbi, ArtifactId, DebugSessionId, DurationSeconds, ErrorCode, HostArch, IsolationTier,
    JobEventId, JobId, JobOutcome, JobState, ProjectId, RuntimeKind, RuntimeProfile,
    RuntimeProfileId, ScriptRef, Sha256, UploadId, Uri, WebhookId, WorkerId,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Page<T> {
    pub items: Vec<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

macro_rules! page_type {
    ($name:ident, $item:ty) => {
        #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
        pub struct $name {
            pub items: Vec<$item>,
            #[serde(skip_serializing_if = "Option::is_none")]
            pub next_cursor: Option<String>,
        }

        impl From<Page<$item>> for $name {
            fn from(page: Page<$item>) -> Self {
                Self {
                    items: page.items,
                    next_cursor: page.next_cursor,
                }
            }
        }

        impl From<$name> for Page<$item> {
            fn from(page: $name) -> Self {
                Self {
                    items: page.items,
                    next_cursor: page.next_cursor,
                }
            }
        }
    };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
pub enum AuthScope {
    #[serde(rename = "project:read")]
    ProjectRead,
    #[serde(rename = "project:write")]
    ProjectWrite,
    #[serde(rename = "debug:create")]
    DebugCreate,
    #[serde(rename = "admin")]
    Admin,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CreateProjectRequest {
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Project {
    pub id: ProjectId,
    pub name: String,
    pub owner: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CreateProjectResponse {
    pub project: Project,
    pub api_key: String,
    pub scopes: Vec<AuthScope>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum UploadKind {
    Apk,
    Test,
    Script,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CreateUploadRequest {
    pub project_id: ProjectId,
    pub kind: UploadKind,
    pub file_name: String,
    pub content_type: String,
    pub size_bytes: u64,
    pub sha256: Sha256,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CreateUploadResponse {
    pub upload_id: UploadId,
    pub upload_url: Uri,
    pub required_headers: BTreeMap<String, String>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum JobMode {
    HeadlessCi,
    BrowserDebug,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AutomationSpec {
    #[default]
    BuiltInSmoke,
    Appium {
        script_ref: ScriptRef,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ArtifactSelection {
    pub screenshots: bool,
    pub video: bool,
    pub logcat: bool,
    pub junit: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CreateJobRequest {
    pub project_id: ProjectId,
    pub apk_upload_id: UploadId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub test_upload_id: Option<UploadId>,
    pub runtime_profile: RuntimeProfileId,
    pub mode: JobMode,
    pub min_isolation: IsolationTier,
    #[serde(default)]
    pub automation: AutomationSpec,
    pub artifacts: ArtifactSelection,
    pub timeout_seconds: DurationSeconds,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct FailureDetail {
    pub code: ErrorCode,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Job {
    pub id: JobId,
    pub project_id: ProjectId,
    pub apk_upload_id: UploadId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub test_upload_id: Option<UploadId>,
    pub runtime_profile: RuntimeProfileId,
    pub mode: JobMode,
    pub min_isolation: IsolationTier,
    pub automation: AutomationSpec,
    pub artifacts: ArtifactSelection,
    pub timeout_seconds: DurationSeconds,
    pub state: JobState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<JobOutcome>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure: Option<FailureDetail>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<WorkerId>,
    pub created_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct JobSummary {
    pub id: JobId,
    pub project_id: ProjectId,
    pub runtime_profile: RuntimeProfileId,
    pub mode: JobMode,
    pub state: JobState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub outcome: Option<JobOutcome>,
    pub created_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct JobEvent {
    pub id: JobEventId,
    pub job_id: JobId,
    pub sequence: u64,
    pub timestamp: DateTime<Utc>,
    pub event_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<JobState>,
    pub payload: BTreeMap<String, Value>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactKind {
    ApkMetadata,
    InstallLog,
    Logcat,
    Screenshot,
    Video,
    AppiumLog,
    Junit,
    CrashTrace,
    RuntimeMetrics,
    DebugSessionAudit,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Artifact {
    pub id: ArtifactId,
    pub job_id: JobId,
    pub kind: ArtifactKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_name: Option<String>,
    pub size_bytes: u64,
    pub sha256: Sha256,
    pub created_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub download_url: Option<Uri>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub download_expires_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum WorkerState {
    Online,
    Draining,
    Offline,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct WorkerStatus {
    pub worker_id: WorkerId,
    pub runtimes: Vec<RuntimeKind>,
    pub kvm: bool,
    pub gpu: bool,
    pub arch: HostArch,
    pub capacity: u32,
    pub active_jobs: u32,
    pub state: WorkerState,
    pub last_seen: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DebugSessionMode {
    Viewer,
    Controller,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DebugSessionRequest {
    pub mode: DebugSessionMode,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DebugSessionToken {
    pub session_id: DebugSessionId,
    pub job_id: JobId,
    pub mode: DebugSessionMode,
    pub token: String,
    pub connect_url: Uri,
    pub expires_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum WebhookEvent {
    JobStateChanged,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CreateWebhookRequest {
    pub project_id: ProjectId,
    pub url: Uri,
    pub events: Vec<WebhookEvent>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Webhook {
    pub id: WebhookId,
    pub project_id: ProjectId,
    pub url: Uri,
    pub events: Vec<WebhookEvent>,
    pub active: bool,
    pub created_at: DateTime<Utc>,
}

page_type!(JobPage, JobSummary);
page_type!(ArtifactPage, Artifact);
page_type!(WorkerPage, WorkerStatus);
page_type!(RuntimeProfilePage, RuntimeProfile);

#[allow(dead_code)]
fn _assert_runtime_enum_types_are_public(_: (AndroidAbi, HostArch)) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn automation_is_internally_tagged() {
        assert_eq!(
            serde_json::to_value(AutomationSpec::BuiltInSmoke).unwrap(),
            serde_json::json!({ "type": "built_in_smoke" })
        );
        assert_eq!(
            serde_json::to_value(AutomationSpec::Appium {
                script_ref: ScriptRef::new("s3://bucket/test.zip").unwrap()
            })
            .unwrap(),
            serde_json::json!({
                "type": "appium",
                "script_ref": "s3://bucket/test.zip"
            })
        );
    }

    #[test]
    fn omitted_automation_selects_built_in_smoke() {
        let request: CreateJobRequest = serde_json::from_value(serde_json::json!({
            "project_id": "proj_demo",
            "apk_upload_id": "upl_apk",
            "runtime_profile": "rtp_android_35",
            "mode": "headless_ci",
            "min_isolation": "vm_isolated",
            "artifacts": {
                "screenshots": true,
                "video": false,
                "logcat": true,
                "junit": true
            },
            "timeout_seconds": 900
        }))
        .unwrap();

        assert_eq!(request.automation, AutomationSpec::BuiltInSmoke);
    }

    #[test]
    fn wire_names_are_stable() {
        assert_eq!(
            serde_json::to_string(&AuthScope::ProjectWrite).unwrap(),
            r#""project:write""#
        );
        assert_eq!(
            serde_json::to_string(&ArtifactKind::DebugSessionAudit).unwrap(),
            r#""debug_session_audit""#
        );
        assert_eq!(
            serde_json::to_string(&WebhookEvent::JobStateChanged).unwrap(),
            r#""job_state_changed""#
        );
    }

    #[test]
    fn concrete_pages_convert_to_and_from_generic_pages() {
        let page = Page::<Artifact> {
            items: Vec::new(),
            next_cursor: Some("next".to_owned()),
        };
        let concrete = ArtifactPage::from(page);
        assert_eq!(concrete.next_cursor.as_deref(), Some("next"));
        let generic = Page::<Artifact>::from(concrete);
        assert!(generic.items.is_empty());
    }
}

//! Safe runtime cleanup abstraction and Docker Engine implementation.

use std::{
    collections::{BTreeMap, BTreeSet},
    net::IpAddr,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use reqwest::{Client, StatusCode};
use run_anywhere_contracts::{JobId, LeaseId, WorkerId};
use serde::Deserialize;
use thiserror::Error;
use url::{Host, Url};

pub const MANAGED_LABEL: &str = "io.run-anywhere.managed";
pub const JOB_ID_LABEL: &str = "job-id";
pub const WORKER_ID_LABEL: &str = "worker-id";
pub const LEASE_ID_LABEL: &str = "lease-id";
pub const RUNTIME_KIND_LABEL: &str = "runtime-kind";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeResource {
    pub resource_id: String,
    pub job_id: JobId,
    pub worker_id: WorkerId,
    pub lease_id: LeaseId,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeOwnership {
    /// The canonical job still has exactly this worker/lease fence.
    ActiveMatchingLease,
    JobMissing,
    JobTerminal,
    LeaseMismatch,
    /// Database state could not be established. Cleanup must fail closed.
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReapDecision {
    Reap,
    KeepActive,
    KeepInGrace,
    KeepUnknown,
}

/// Decide whether destructive cleanup is permitted. Time alone is never
/// sufficient: an active matching lease remains protected at any age.
pub fn evaluate_reap(
    _resource: &RuntimeResource,
    ownership: RuntimeOwnership,
    unsafe_since: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
    grace: Duration,
) -> ReapDecision {
    match ownership {
        RuntimeOwnership::ActiveMatchingLease => return ReapDecision::KeepActive,
        RuntimeOwnership::Unknown => return ReapDecision::KeepUnknown,
        RuntimeOwnership::JobMissing
        | RuntimeOwnership::JobTerminal
        | RuntimeOwnership::LeaseMismatch => {}
    }
    let Some(unsafe_since) = unsafe_since else {
        return ReapDecision::KeepInGrace;
    };
    let Ok(grace) = ChronoDuration::from_std(grace) else {
        return ReapDecision::KeepInGrace;
    };
    if now.signed_duration_since(unsafe_since) < grace {
        ReapDecision::KeepInGrace
    } else {
        ReapDecision::Reap
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReapOutcome {
    Reaped,
    AlreadyAbsent,
    Skipped,
}

#[derive(Debug, Error)]
pub enum ReaperError {
    #[error("unsupported Docker Engine endpoint `{scheme}` on this platform")]
    UnsupportedEndpoint { scheme: String },
    #[error("invalid Docker Engine endpoint: {0}")]
    InvalidEndpoint(String),
    #[error("plaintext Docker Engine TCP is allowed only on loopback")]
    InsecureEndpoint,
    #[error("could not construct Docker Engine client: {0}")]
    BuildClient(#[source] reqwest::Error),
    #[error("Docker Engine request failed: {0}")]
    Request(#[source] reqwest::Error),
    #[error("Docker Engine returned HTTP {0}")]
    EngineStatus(StatusCode),
    #[error("managed runtime `{resource_id}` is missing or has an invalid `{label}` label")]
    MalformedManagedResource {
        resource_id: String,
        label: &'static str,
    },
    #[error("runtime resource ID is not a safe Docker identifier")]
    InvalidResourceId,
    #[error("Docker runtime `{0}` no longer matches the requested job/worker/lease fence")]
    IdentityMismatch(String),
    #[error("runtime reaper state lock was poisoned")]
    StatePoisoned,
}

#[async_trait]
pub trait RuntimeReaper: Send + Sync {
    async fn inventory(&self) -> Result<Vec<RuntimeResource>, ReaperError>;
    async fn reap(&self, resource: &RuntimeResource) -> Result<ReapOutcome, ReaperError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NoopRuntimeReaper;

#[async_trait]
impl RuntimeReaper for NoopRuntimeReaper {
    async fn inventory(&self) -> Result<Vec<RuntimeResource>, ReaperError> {
        Ok(Vec::new())
    }

    async fn reap(&self, _resource: &RuntimeResource) -> Result<ReapOutcome, ReaperError> {
        Ok(ReapOutcome::Skipped)
    }
}

/// In-memory implementation for reconciler tests. It models idempotent
/// deletion and exposes reaped identifiers without permitting external I/O.
#[derive(Clone, Debug, Default)]
pub struct MemoryRuntimeReaper {
    state: Arc<Mutex<MemoryState>>,
}

#[derive(Debug, Default)]
struct MemoryState {
    resources: BTreeMap<String, RuntimeResource>,
    reaped: BTreeSet<String>,
}

impl MemoryRuntimeReaper {
    pub fn new(resources: impl IntoIterator<Item = RuntimeResource>) -> Self {
        let resources = resources
            .into_iter()
            .map(|resource| (resource.resource_id.clone(), resource))
            .collect();
        Self {
            state: Arc::new(Mutex::new(MemoryState {
                resources,
                reaped: BTreeSet::new(),
            })),
        }
    }

    pub fn reaped_ids(&self) -> Result<Vec<String>, ReaperError> {
        let state = self.state.lock().map_err(|_| ReaperError::StatePoisoned)?;
        Ok(state.reaped.iter().cloned().collect())
    }
}

#[async_trait]
impl RuntimeReaper for MemoryRuntimeReaper {
    async fn inventory(&self) -> Result<Vec<RuntimeResource>, ReaperError> {
        let state = self.state.lock().map_err(|_| ReaperError::StatePoisoned)?;
        Ok(state.resources.values().cloned().collect())
    }

    async fn reap(&self, resource: &RuntimeResource) -> Result<ReapOutcome, ReaperError> {
        let mut state = self.state.lock().map_err(|_| ReaperError::StatePoisoned)?;
        let Some(stored) = state.resources.get(&resource.resource_id) else {
            return Ok(ReapOutcome::AlreadyAbsent);
        };
        if stored != resource {
            return Err(ReaperError::IdentityMismatch(resource.resource_id.clone()));
        }
        state.resources.remove(&resource.resource_id);
        state.reaped.insert(resource.resource_id.clone());
        Ok(ReapOutcome::Reaped)
    }
}

#[derive(Clone)]
pub struct DockerEngineReaper {
    client: Client,
    base_url: Url,
}

impl std::fmt::Debug for DockerEngineReaper {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DockerEngineReaper")
            .field("transport", &self.base_url.scheme())
            .finish_non_exhaustive()
    }
}

impl DockerEngineReaper {
    pub fn new(endpoint: &str, request_timeout: Duration) -> Result<Self, ReaperError> {
        let parsed = Url::parse(endpoint)
            .map_err(|error| ReaperError::InvalidEndpoint(error.to_string()))?;
        let mut builder = Client::builder().timeout(request_timeout);
        let base_url = match parsed.scheme() {
            "http" if endpoint_is_loopback(&parsed) => parsed,
            "http" => return Err(ReaperError::InsecureEndpoint),
            "https" => parsed,
            "unix" => {
                #[cfg(unix)]
                {
                    let socket = PathBuf::from(parsed.path());
                    if socket.as_os_str().is_empty() {
                        return Err(ReaperError::InvalidEndpoint(
                            "unix endpoint has no socket path".to_owned(),
                        ));
                    }
                    builder = builder.no_proxy().unix_socket(socket);
                    Url::parse("http://localhost").expect("static URL is valid")
                }
                #[cfg(not(unix))]
                {
                    return Err(ReaperError::UnsupportedEndpoint {
                        scheme: "unix".to_owned(),
                    });
                }
            }
            "npipe" => {
                #[cfg(windows)]
                {
                    let suffix = parsed.path().trim_start_matches('/').replace('/', "\\");
                    if suffix.is_empty() {
                        return Err(ReaperError::InvalidEndpoint(
                            "npipe endpoint has no pipe path".to_owned(),
                        ));
                    }
                    let pipe = PathBuf::from(format!(r"\\{suffix}"));
                    builder = builder.no_proxy().windows_named_pipe(pipe);
                    Url::parse("http://localhost").expect("static URL is valid")
                }
                #[cfg(not(windows))]
                {
                    return Err(ReaperError::UnsupportedEndpoint {
                        scheme: "npipe".to_owned(),
                    });
                }
            }
            scheme => {
                return Err(ReaperError::UnsupportedEndpoint {
                    scheme: scheme.to_owned(),
                });
            }
        };
        let client = builder.build().map_err(ReaperError::BuildClient)?;
        Ok(Self { client, base_url })
    }

    fn url(&self, segments: &[&str]) -> Result<Url, ReaperError> {
        let mut url = self.base_url.clone();
        url.set_query(None);
        url.set_fragment(None);
        let mut path = url.path_segments_mut().map_err(|_| {
            ReaperError::InvalidEndpoint("Docker endpoint cannot be a base URL".to_owned())
        })?;
        path.clear();
        for segment in segments {
            path.push(segment);
        }
        drop(path);
        Ok(url)
    }

    async fn inspect_matches(
        &self,
        resource: &RuntimeResource,
    ) -> Result<Option<bool>, ReaperError> {
        let response = self
            .client
            .get(self.url(&["containers", &resource.resource_id, "json"])?)
            .send()
            .await
            .map_err(ReaperError::Request)?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(ReaperError::EngineStatus(response.status()));
        }
        let inspected: DockerInspect = response.json().await.map_err(ReaperError::Request)?;
        let labels = inspected.config.labels.unwrap_or_default();
        Ok(Some(labels_match_resource(&labels, resource)))
    }
}

#[async_trait]
impl RuntimeReaper for DockerEngineReaper {
    async fn inventory(&self) -> Result<Vec<RuntimeResource>, ReaperError> {
        let filters = serde_json::json!({"label": [format!("{MANAGED_LABEL}=true")]});
        let response = self
            .client
            .get(self.url(&["containers", "json"])?)
            .query(&[("all", "1"), ("filters", &filters.to_string())])
            .send()
            .await
            .map_err(ReaperError::Request)?;
        if !response.status().is_success() {
            return Err(ReaperError::EngineStatus(response.status()));
        }
        let containers: Vec<DockerContainer> =
            response.json().await.map_err(ReaperError::Request)?;
        containers.into_iter().map(runtime_from_container).collect()
    }

    async fn reap(&self, resource: &RuntimeResource) -> Result<ReapOutcome, ReaperError> {
        if !valid_resource_id(&resource.resource_id) {
            return Err(ReaperError::InvalidResourceId);
        }
        match self.inspect_matches(resource).await? {
            None => return Ok(ReapOutcome::AlreadyAbsent),
            Some(false) => {
                return Err(ReaperError::IdentityMismatch(resource.resource_id.clone()));
            }
            Some(true) => {}
        }
        let response = self
            .client
            .delete(self.url(&["containers", &resource.resource_id])?)
            .query(&[("force", "true"), ("v", "true")])
            .send()
            .await
            .map_err(ReaperError::Request)?;
        match response.status() {
            status if status.is_success() => Ok(ReapOutcome::Reaped),
            StatusCode::NOT_FOUND => Ok(ReapOutcome::AlreadyAbsent),
            status => Err(ReaperError::EngineStatus(status)),
        }
    }
}

#[derive(Debug, Deserialize)]
struct DockerContainer {
    #[serde(rename = "Id")]
    id: String,
    #[serde(rename = "Created")]
    created: i64,
    #[serde(rename = "Labels", default)]
    labels: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Deserialize)]
struct DockerInspect {
    #[serde(rename = "Config")]
    config: DockerInspectConfig,
}

#[derive(Debug, Deserialize)]
struct DockerInspectConfig {
    #[serde(rename = "Labels", default)]
    labels: Option<BTreeMap<String, String>>,
}

fn runtime_from_container(container: DockerContainer) -> Result<RuntimeResource, ReaperError> {
    if !valid_resource_id(&container.id) {
        return Err(ReaperError::InvalidResourceId);
    }
    let labels = container.labels.unwrap_or_default();
    if required_label(&labels, &container.id, MANAGED_LABEL)? != "true" {
        return Err(ReaperError::MalformedManagedResource {
            resource_id: container.id.clone(),
            label: MANAGED_LABEL,
        });
    }
    let job_id = required_label(&labels, &container.id, JOB_ID_LABEL)?
        .parse()
        .map_err(|_| malformed(&container.id, JOB_ID_LABEL))?;
    let worker_id = required_label(&labels, &container.id, WORKER_ID_LABEL)?
        .parse()
        .map_err(|_| malformed(&container.id, WORKER_ID_LABEL))?;
    let lease_id = required_label(&labels, &container.id, LEASE_ID_LABEL)?
        .parse()
        .map_err(|_| malformed(&container.id, LEASE_ID_LABEL))?;
    let runtime_kind = required_label(&labels, &container.id, RUNTIME_KIND_LABEL)?;
    if !valid_runtime_kind(runtime_kind) {
        return Err(malformed(&container.id, RUNTIME_KIND_LABEL));
    }
    let created_at = DateTime::from_timestamp(container.created, 0)
        .ok_or_else(|| malformed(&container.id, "created_at"))?;
    Ok(RuntimeResource {
        resource_id: container.id,
        job_id,
        worker_id,
        lease_id,
        created_at,
    })
}

fn required_label<'a>(
    labels: &'a BTreeMap<String, String>,
    resource_id: &str,
    label: &'static str,
) -> Result<&'a str, ReaperError> {
    labels
        .get(label)
        .map(String::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| malformed(resource_id, label))
}

fn malformed(resource_id: &str, label: &'static str) -> ReaperError {
    ReaperError::MalformedManagedResource {
        resource_id: resource_id.to_owned(),
        label,
    }
}

fn labels_match_resource(labels: &BTreeMap<String, String>, resource: &RuntimeResource) -> bool {
    labels.get(MANAGED_LABEL).map(String::as_str) == Some("true")
        && labels.get(JOB_ID_LABEL).map(String::as_str) == Some(resource.job_id.as_str())
        && labels.get(WORKER_ID_LABEL).map(String::as_str) == Some(resource.worker_id.as_str())
        && labels.get(LEASE_ID_LABEL).map(String::as_str) == Some(resource.lease_id.as_str())
        && labels
            .get(RUNTIME_KIND_LABEL)
            .is_some_and(|value| valid_runtime_kind(value))
}

fn valid_runtime_kind(value: &str) -> bool {
    matches!(
        value,
        "android_emulator_container" | "redroid" | "cuttlefish" | "browser_native_wasm"
    )
}

fn valid_resource_id(resource_id: &str) -> bool {
    !resource_id.is_empty()
        && resource_id.len() <= 128
        && resource_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn endpoint_is_loopback(url: &Url) -> bool {
    match url.host() {
        Some(Host::Domain(host)) => {
            host.eq_ignore_ascii_case("localhost")
                || host
                    .parse::<IpAddr>()
                    .is_ok_and(|address| address.is_loopback())
        }
        Some(Host::Ipv4(address)) => address.is_loopback(),
        Some(Host::Ipv6(address)) => address.is_loopback(),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

    fn resource(created_at: i64) -> RuntimeResource {
        RuntimeResource {
            resource_id: "container-123".to_owned(),
            job_id: JobId::new("job_123").unwrap(),
            worker_id: WorkerId::new("wrk_123").unwrap(),
            lease_id: LeaseId::new("lease_123").unwrap(),
            created_at: DateTime::from_timestamp(created_at, 0).unwrap(),
        }
    }

    #[test]
    fn active_matching_lease_is_never_reaped_regardless_of_age() {
        assert_eq!(
            evaluate_reap(
                &resource(0),
                RuntimeOwnership::ActiveMatchingLease,
                None,
                DateTime::from_timestamp(100_000, 0).unwrap(),
                Duration::from_secs(120),
            ),
            ReapDecision::KeepActive
        );
    }

    #[test]
    fn orphan_requires_the_full_grace_period() {
        let runtime = resource(0);
        let unsafe_since = DateTime::from_timestamp(100, 0).unwrap();
        assert_eq!(
            evaluate_reap(
                &runtime,
                RuntimeOwnership::JobMissing,
                Some(unsafe_since),
                DateTime::from_timestamp(219, 0).unwrap(),
                Duration::from_secs(120),
            ),
            ReapDecision::KeepInGrace
        );
        assert_eq!(
            evaluate_reap(
                &runtime,
                RuntimeOwnership::JobMissing,
                Some(unsafe_since),
                DateTime::from_timestamp(220, 0).unwrap(),
                Duration::from_secs(120),
            ),
            ReapDecision::Reap
        );
    }

    #[test]
    fn docker_inventory_requires_all_fencing_labels() {
        let mut labels = BTreeMap::from([
            (MANAGED_LABEL.to_owned(), "true".to_owned()),
            (JOB_ID_LABEL.to_owned(), "job_123".to_owned()),
            (WORKER_ID_LABEL.to_owned(), "wrk_123".to_owned()),
            (LEASE_ID_LABEL.to_owned(), "lease_123".to_owned()),
            (
                RUNTIME_KIND_LABEL.to_owned(),
                "android_emulator_container".to_owned(),
            ),
        ]);
        let parsed = runtime_from_container(DockerContainer {
            id: "abc123".to_owned(),
            created: 10,
            labels: Some(labels.clone()),
        })
        .unwrap();
        assert_eq!(parsed.job_id.as_str(), "job_123");

        labels.remove(LEASE_ID_LABEL);
        assert!(matches!(
            runtime_from_container(DockerContainer {
                id: "abc123".to_owned(),
                created: 10,
                labels: Some(labels),
            }),
            Err(ReaperError::MalformedManagedResource {
                label: LEASE_ID_LABEL,
                ..
            })
        ));
    }

    #[test]
    fn docker_adapter_rejects_remote_plaintext_tcp() {
        assert!(matches!(
            DockerEngineReaper::new("http://docker.example.test:2375", Duration::from_secs(10)),
            Err(ReaperError::InsecureEndpoint)
        ));
        assert!(DockerEngineReaper::new("http://127.0.0.1:2375", Duration::from_secs(10)).is_ok());
    }

    /// Opt-in real Docker Engine probe. The caller supplies an image that is
    /// already present locally, keeping the test deterministic and offline.
    #[tokio::test]
    async fn docker_engine_reaper_removes_an_exactly_fenced_container() -> TestResult {
        if std::env::var("RUN_DOCKER_REAPER_INTEGRATION").as_deref() != Ok("true") {
            return Ok(());
        }
        let image = std::env::var("SCHEDULER_DOCKER_TEST_IMAGE").map_err(|_| {
            "SCHEDULER_DOCKER_TEST_IMAGE must name an already-present image when the Docker integration test is enabled"
        })?;
        let endpoint = std::env::var("SCHEDULER_DOCKER_ENDPOINT")
            .or_else(|_| std::env::var("DOCKER_HOST"))
            .unwrap_or_else(|_| default_test_docker_endpoint().to_owned());
        let reaper = DockerEngineReaper::new(&endpoint, Duration::from_secs(15))?;
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let job_id = JobId::new(format!("job_docker_{suffix}"))?;
        let worker_id = WorkerId::new(format!("wrk_docker_{suffix}"))?;
        let lease_id = LeaseId::new(format!("lease_docker_{suffix}"))?;
        let labels = BTreeMap::from([
            (MANAGED_LABEL.to_owned(), "true".to_owned()),
            (JOB_ID_LABEL.to_owned(), job_id.to_string()),
            (WORKER_ID_LABEL.to_owned(), worker_id.to_string()),
            (LEASE_ID_LABEL.to_owned(), lease_id.to_string()),
            (
                RUNTIME_KIND_LABEL.to_owned(),
                "android_emulator_container".to_owned(),
            ),
        ]);
        let response = reaper
            .client
            .post(reaper.url(&["containers", "create"])?)
            .query(&[("name", format!("raa-reaper-test-{suffix}"))])
            .json(&serde_json::json!({"Image": image, "Labels": labels}))
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(format!("Docker create returned HTTP {}", response.status()).into());
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "PascalCase")]
        struct CreatedContainer {
            id: String,
        }
        let created: CreatedContainer = response.json().await?;

        let result = async {
            let resource = reaper
                .inventory()
                .await?
                .into_iter()
                .find(|resource| resource.resource_id == created.id)
                .ok_or("new managed container was absent from Docker inventory")?;
            if resource.job_id != job_id
                || resource.worker_id != worker_id
                || resource.lease_id != lease_id
            {
                return Err("Docker inventory changed the fencing labels".into());
            }
            if reaper.reap(&resource).await? != ReapOutcome::Reaped {
                return Err("Docker reaper did not report a deletion".into());
            }
            if reaper.inspect_matches(&resource).await?.is_some() {
                return Err("Docker container still exists after reap".into());
            }
            Ok(())
        }
        .await;

        if result.is_err() {
            let _ = reaper
                .client
                .delete(reaper.url(&["containers", &created.id])?)
                .query(&[("force", "true"), ("v", "true")])
                .send()
                .await;
        }
        result
    }

    #[cfg(windows)]
    fn default_test_docker_endpoint() -> &'static str {
        "npipe:////./pipe/docker_engine"
    }

    #[cfg(not(windows))]
    fn default_test_docker_endpoint() -> &'static str {
        "unix:///var/run/docker.sock"
    }
}

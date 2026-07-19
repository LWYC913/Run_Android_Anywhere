use std::{collections::BTreeMap, env, sync::Arc, time::Duration};

use async_nats::jetstream::stream;
use async_trait::async_trait;
use axum::{
    Router,
    body::Body,
    http::{Method, Request, StatusCode, header},
};
use chrono::{Duration as ChronoDuration, Utc};
use http_body_util::BodyExt as _;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use run_anywhere_api::{
    ApiMetrics, AppState, Config,
    debug_token::{DebugTokenClaims, DebugTokenIssuer},
    object_store::{
        ObjectStore, ObjectStoreError, PresignedDownload, PresignedUpload, checksum_base64,
    },
    public_router,
    queue::{JetStreamPublisher, OutboxDispatcher, OutboxDispatcherConfig},
    webhook::{WebhookDispatcher, WebhookPolicy},
};
use run_anywhere_contracts::{
    ArtifactPage, ArtifactSelection, AutomationSpec, CreateJobRequest, CreateProjectRequest,
    CreateProjectResponse, CreateUploadRequest, CreateUploadResponse, CreateWebhookRequest,
    DebugSessionMode, DebugSessionRequest, DebugSessionToken, DurationSeconds, ErrorResponse,
    IsolationTier, Job, JobMode, JobPage, JobState, RuntimeProfileId, RuntimeProfilePage, Sha256,
    UploadKind, Uri, Webhook, WebhookEvent, WorkerPage,
};
use run_anywhere_repository::{MIGRATOR, Repository};
use serde::{Serialize, de::DeserializeOwned};
use sqlx::PgPool;
use tower::ServiceExt as _;
use uuid::Uuid;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

const ADMIN_TOKEN: &str = "part-three-integration-admin";
const PRIVATE_KEY: &str = r#"-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIGrD/e7uKYqSY4twDEsRfMMuLSrODf14dpTiTK6K1YI0
-----END PRIVATE KEY-----
"#;
const PUBLIC_KEY: &[u8] = br#"-----BEGIN PUBLIC KEY-----
MCowBQYDK2VwAyEA2+Jj2UvNCvQiUPNYRgSi0cJSPiJI6Rs6D0UTeEpQVj8=
-----END PUBLIC KEY-----
"#;
const PROFILE_ID: &str = "rtp_android_35_pixel_6_x86_64_emulator";

#[derive(Clone, Default)]
struct FakeObjectStore;

#[async_trait]
impl ObjectStore for FakeObjectStore {
    async fn presign_upload(
        &self,
        key: &str,
        content_type: &str,
        _size_bytes: u64,
        sha256: &Sha256,
    ) -> Result<PresignedUpload, ObjectStoreError> {
        Ok(PresignedUpload {
            url: Uri::new(format!("https://objects.example.test/{key}?signed=put"))
                .expect("test URL is valid"),
            required_headers: BTreeMap::from([
                ("Content-Type".to_owned(), content_type.to_owned()),
                ("x-amz-checksum-sha256".to_owned(), checksum_base64(sha256)),
            ]),
            expires_at: Utc::now() + ChronoDuration::minutes(15),
        })
    }

    async fn presign_download(&self, key: &str) -> Result<PresignedDownload, ObjectStoreError> {
        Ok(PresignedDownload {
            url: Uri::new(format!("https://objects.example.test/{key}?signed=get"))
                .expect("test URL is valid"),
            expires_at: Utc::now() + ChronoDuration::minutes(15),
        })
    }

    async fn verify_upload(
        &self,
        _key: &str,
        _expected_size: u64,
        _expected_sha256: &Sha256,
    ) -> Result<(), ObjectStoreError> {
        Ok(())
    }
}

#[sqlx::test(migrator = "MIGRATOR")]
async fn public_router_executes_the_part_three_contract(pool: PgPool) -> TestResult {
    let repository = Repository::new(pool.clone());
    let metrics = ApiMetrics::default();
    let config = Arc::new(Config::from_map(std::collections::HashMap::from([
        ("JWT_SIGNING_KEY_PEM".to_owned(), PRIVATE_KEY.to_owned()),
        ("JWT_KID".to_owned(), "debug-integration-v1".to_owned()),
        ("BOOTSTRAP_ADMIN_TOKEN".to_owned(), ADMIN_TOKEN.to_owned()),
        (
            "DEBUG_GATEWAY_BASE_URL".to_owned(),
            "https://debug.example.test".to_owned(),
        ),
        (
            "WEBHOOK_ALLOW_PRIVATE_NETWORKS".to_owned(),
            "true".to_owned(),
        ),
    ]))?);
    let (shutdown_tx, shutdown) = tokio::sync::watch::channel(false);
    let app = public_router(AppState {
        repository: repository.clone(),
        object_store: Arc::new(FakeObjectStore),
        debug_tokens: DebugTokenIssuer::from_ed25519_pkcs8_pem(
            PRIVATE_KEY.as_bytes(),
            "debug-integration-v1",
        )?,
        webhook_dispatcher: WebhookDispatcher::new(
            16,
            Duration::from_secs(1),
            WebhookPolicy {
                allow_private_networks: true,
            },
            metrics.clone(),
        )?,
        metrics,
        config,
        shutdown,
    });

    let unauthenticated = send_json(
        &app,
        Method::POST,
        "/v1/projects",
        None,
        &CreateProjectRequest {
            name: "unauthenticated".to_owned(),
        },
        &[],
    )
    .await;
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);
    let error: ErrorResponse = decode_json(unauthenticated).await;
    assert_eq!(
        error.error.code,
        run_anywhere_contracts::ErrorCode::Unauthorized
    );

    let first_project = create_project(&app, "Mobile QA").await;
    let project_key = first_project.api_key.clone();
    assert_eq!(first_project.scopes.len(), 3);
    let second_project = create_project(&app, "Other tenant").await;

    let cross_tenant = send_empty(
        &app,
        Method::GET,
        &format!("/v1/jobs?project_id={}", second_project.project.id),
        Some(&project_key),
        &[],
    )
    .await;
    assert_eq!(cross_tenant.status(), StatusCode::FORBIDDEN);

    let digest = Sha256::new("a".repeat(64))?;
    let upload = send_json(
        &app,
        Method::POST,
        "/v1/uploads/apk",
        Some(&project_key),
        &CreateUploadRequest {
            project_id: first_project.project.id.clone(),
            kind: UploadKind::Apk,
            file_name: "app.apk".to_owned(),
            content_type: "application/vnd.android.package-archive".to_owned(),
            size_bytes: 4_096,
            sha256: digest,
        },
        &[],
    )
    .await;
    assert_eq!(upload.status(), StatusCode::CREATED);
    let upload: CreateUploadResponse = decode_json(upload).await;
    assert!(upload.required_headers.contains_key("Content-Type"));
    assert!(
        upload
            .required_headers
            .contains_key("x-amz-checksum-sha256")
    );

    let job_request = CreateJobRequest {
        project_id: first_project.project.id.clone(),
        apk_upload_id: upload.upload_id,
        test_upload_id: None,
        runtime_profile: RuntimeProfileId::new(PROFILE_ID)?,
        mode: JobMode::HeadlessCi,
        min_isolation: IsolationTier::VmIsolated,
        automation: AutomationSpec::BuiltInSmoke,
        artifacts: ArtifactSelection {
            screenshots: true,
            video: false,
            logcat: true,
            junit: true,
        },
        timeout_seconds: DurationSeconds::new(300)?,
    };
    let created = create_job(&app, &project_key, &job_request, "ci-attempt-1").await;
    let replay = create_job(&app, &project_key, &job_request, "ci-attempt-1").await;
    assert_eq!(created.id, replay.id);
    assert_eq!(repository.pending_outbox_count().await?, 1);

    if env::var("RUN_QUEUE_INTEGRATION").as_deref() == Ok("true") {
        let client = async_nats::connect(env::var("NATS_URL")?).await?;
        let context = async_nats::jetstream::new(client);
        let stream_name = format!("PART3_HTTP_{}", Uuid::new_v4().simple());
        let mut stream = context
            .create_stream(stream::Config {
                name: stream_name.clone(),
                subjects: vec!["jobs.queued".to_owned()],
                duplicate_window: Duration::from_secs(120),
                ..stream::Config::default()
            })
            .await?;
        let dispatcher = OutboxDispatcher::new(
            repository.clone(),
            Arc::new(JetStreamPublisher::new(context.clone())),
            OutboxDispatcherConfig::new("part-three-http-integration"),
        )?;

        let first_dispatch = dispatcher.dispatch_once().await?;
        let second_dispatch = dispatcher.dispatch_once().await?;
        assert_eq!(first_dispatch.leased, 1);
        assert_eq!(first_dispatch.published, 1);
        assert_eq!(first_dispatch.retried, 0);
        assert_eq!(second_dispatch.leased, 0);
        assert_eq!(stream.info().await?.state.messages, 1);
        assert_eq!(repository.pending_outbox_count().await?, 0);
        context.delete_stream(&stream_name).await?;
    }

    let listed = send_empty(
        &app,
        Method::GET,
        &format!("/v1/jobs?project_id={}", first_project.project.id),
        Some(&project_key),
        &[],
    )
    .await;
    assert_eq!(listed.status(), StatusCode::OK);
    let listed: JobPage = decode_json(listed).await;
    assert_eq!(listed.items.len(), 1);
    assert_eq!(listed.items[0].id, created.id);

    let fetched = send_empty(
        &app,
        Method::GET,
        &format!("/v1/jobs/{}", created.id),
        Some(&project_key),
        &[],
    )
    .await;
    assert_eq!(fetched.status(), StatusCode::OK);
    assert_eq!(decode_json::<Job>(fetched).await.id, created.id);

    let queued_event = repository
        .list_job_events_after(&created.id, 0, 10)
        .await?
        .pop()
        .expect("job creation persists an event");
    let terminal_event = repository
        .append_job_event(
            &created.id,
            "job.finished",
            Some(JobState::Passed),
            BTreeMap::new(),
        )
        .await?;
    let events = send_empty(
        &app,
        Method::GET,
        &format!("/v1/jobs/{}/events", created.id),
        Some(&project_key),
        &[("Last-Event-ID", &queued_event.sequence.to_string())],
    )
    .await;
    assert_eq!(events.status(), StatusCode::OK);
    assert_eq!(
        events.headers().get(header::CONTENT_TYPE).unwrap(),
        "text/event-stream"
    );
    let event_body = body_text(events).await;
    assert!(event_body.contains(&format!("id: {}", terminal_event.sequence)));
    assert!(event_body.contains("event: job.finished"));
    assert!(event_body.contains(&format!("\"id\":\"{}\"", terminal_event.id)));

    sqlx::query(
        "UPDATE jobs SET state = 'debug_available', pending_outcome = 'failed', \
         started_at = COALESCE(started_at, clock_timestamp()) WHERE id = $1",
    )
    .bind(created.id.as_str())
    .execute(&pool)
    .await?;
    let debug = send_json(
        &app,
        Method::POST,
        &format!("/v1/jobs/{}/debug-sessions", created.id),
        Some(&project_key),
        &DebugSessionRequest {
            mode: DebugSessionMode::Controller,
        },
        &[],
    )
    .await;
    assert_eq!(debug.status(), StatusCode::CREATED);
    let debug: DebugSessionToken = decode_json(debug).await;
    assert_eq!(debug.job_id, created.id);
    assert_eq!(debug.mode, DebugSessionMode::Controller);
    assert_eq!(
        debug.connect_url.as_str(),
        format!("https://debug.example.test/sessions/{}", debug.session_id)
    );
    let persisted_sessions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM debug_sessions WHERE id = $1")
            .bind(debug.session_id.as_str())
            .fetch_one(&pool)
            .await?;
    let persisted_audits: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM audit_log WHERE action = 'debug_session.created' AND subject = $1",
    )
    .bind(debug.session_id.as_str())
    .fetch_one(&pool)
    .await?;
    assert_eq!((persisted_sessions, persisted_audits), (1, 1));
    let mut validation = Validation::new(Algorithm::EdDSA);
    validation.set_audience(&[format!("{}:{}", debug.job_id, debug.session_id)]);
    let claims = decode::<DebugTokenClaims>(
        &debug.token,
        &DecodingKey::from_ed_pem(PUBLIC_KEY)?,
        &validation,
    )?;
    assert_eq!(claims.claims.mode, DebugSessionMode::Controller);

    let cancellable = create_job(&app, &project_key, &job_request, "cancel-me").await;
    for _ in 0..2 {
        let cancelled = send_empty(
            &app,
            Method::POST,
            &format!("/v1/jobs/{}/cancel", cancellable.id),
            Some(&project_key),
            &[],
        )
        .await;
        assert_eq!(cancelled.status(), StatusCode::ACCEPTED);
        assert_eq!(
            decode_json::<Job>(cancelled).await.state,
            JobState::CollectingArtifacts
        );
    }

    let artifacts = send_empty(
        &app,
        Method::GET,
        &format!("/v1/jobs/{}/artifacts", created.id),
        Some(&project_key),
        &[],
    )
    .await;
    assert!(
        decode_json::<ArtifactPage>(artifacts)
            .await
            .items
            .is_empty()
    );

    let webhook = send_json(
        &app,
        Method::POST,
        "/v1/webhooks",
        Some(&project_key),
        &CreateWebhookRequest {
            project_id: first_project.project.id.clone(),
            url: Uri::new("http://10.0.0.8:9/job-events")?,
            events: vec![WebhookEvent::JobStateChanged],
        },
        &[],
    )
    .await;
    assert_eq!(webhook.status(), StatusCode::CREATED);
    assert!(decode_json::<Webhook>(webhook).await.active);

    let profiles = send_empty(
        &app,
        Method::GET,
        "/v1/runtime-profiles",
        Some(ADMIN_TOKEN),
        &[],
    )
    .await;
    assert_eq!(
        decode_json::<RuntimeProfilePage>(profiles)
            .await
            .items
            .len(),
        4
    );
    let workers = send_empty(&app, Method::GET, "/v1/workers", Some(ADMIN_TOKEN), &[]).await;
    assert!(decode_json::<WorkerPage>(workers).await.items.is_empty());

    // The endpoint uses a fixed page size of 50. Seed enough jobs to exercise
    // the opaque cursor rather than merely checking the page shape.
    for index in 0..49 {
        repository
            .create_job(job_request.clone(), format!("pagination-{index}"))
            .await?;
    }
    let first_page = send_empty(
        &app,
        Method::GET,
        &format!("/v1/jobs?project_id={}", first_project.project.id),
        Some(&project_key),
        &[],
    )
    .await;
    let first_page: JobPage = decode_json(first_page).await;
    assert_eq!(first_page.items.len(), 50);
    let cursor = first_page.next_cursor.expect("more jobs require a cursor");
    let second_page = send_empty(
        &app,
        Method::GET,
        &format!(
            "/v1/jobs?project_id={}&cursor={cursor}",
            first_project.project.id
        ),
        Some(&project_key),
        &[],
    )
    .await;
    let second_page: JobPage = decode_json(second_page).await;
    assert_eq!(second_page.items.len(), 1);
    assert!(second_page.next_cursor.is_none());

    let draining_events = send_empty(
        &app,
        Method::GET,
        &format!("/v1/jobs/{}/events", cancellable.id),
        Some(&project_key),
        &[],
    )
    .await;
    let draining_body = tokio::spawn(body_text(draining_events));
    shutdown_tx.send(true)?;
    tokio::time::timeout(Duration::from_secs(2), draining_body).await??;

    Ok(())
}

async fn create_project(app: &Router, name: &str) -> CreateProjectResponse {
    let response = send_json(
        app,
        Method::POST,
        "/v1/projects",
        Some(ADMIN_TOKEN),
        &CreateProjectRequest {
            name: name.to_owned(),
        },
        &[],
    )
    .await;
    assert_eq!(response.status(), StatusCode::CREATED);
    decode_json(response).await
}

async fn create_job(
    app: &Router,
    api_key: &str,
    request: &CreateJobRequest,
    idempotency_key: &str,
) -> Job {
    let response = send_json(
        app,
        Method::POST,
        "/v1/jobs",
        Some(api_key),
        request,
        &[("Idempotency-Key", idempotency_key)],
    )
    .await;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    decode_json(response).await
}

async fn send_json<T: Serialize>(
    app: &Router,
    method: Method,
    uri: &str,
    bearer: Option<&str>,
    body: &T,
    headers: &[(&str, &str)],
) -> axum::response::Response {
    send(
        app,
        method,
        uri,
        bearer,
        Body::from(serde_json::to_vec(body).expect("test body serializes")),
        true,
        headers,
    )
    .await
}

async fn send_empty(
    app: &Router,
    method: Method,
    uri: &str,
    bearer: Option<&str>,
    headers: &[(&str, &str)],
) -> axum::response::Response {
    send(app, method, uri, bearer, Body::empty(), false, headers).await
}

async fn send(
    app: &Router,
    method: Method,
    uri: &str,
    bearer: Option<&str>,
    body: Body,
    json_body: bool,
    headers: &[(&str, &str)],
) -> axum::response::Response {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(bearer) = bearer {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {bearer}"));
    }
    if json_body {
        builder = builder.header(header::CONTENT_TYPE, "application/json");
    }
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    app.clone()
        .oneshot(builder.body(body).expect("test request is valid"))
        .await
        .expect("router is infallible")
}

async fn decode_json<T: DeserializeOwned>(response: axum::response::Response) -> T {
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("response body is readable")
        .to_bytes();
    serde_json::from_slice(&bytes).unwrap_or_else(|error| {
        panic!(
            "HTTP {status} did not contain expected JSON: {error}; body={}",
            String::from_utf8_lossy(&bytes)
        )
    })
}

async fn body_text(response: axum::response::Response) -> String {
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("response body is readable")
        .to_bytes();
    String::from_utf8(bytes.to_vec()).expect("response body is UTF-8")
}

use std::{collections::HashMap, time::Duration};

use axum::{
    body::Body,
    http::{Request, StatusCode, header},
    response::IntoResponse,
};
use http_body_util::BodyExt as _;
use run_anywhere_api::{ApiError, ApiMetrics, Config, object_store, observability};
use run_anywhere_contracts::{ErrorCode, ErrorResponse, ProjectId, Sha256, UploadKind};
use tower::ServiceExt as _;

fn minimum_config() -> HashMap<String, String> {
    HashMap::from([(
        "JWT_SIGNING_KEY_PEM".to_owned(),
        "-----BEGIN PRIVATE KEY-----\nlocal-test-key\n-----END PRIVATE KEY-----".to_owned(),
    )])
}

#[test]
fn environment_contract_uses_safe_defaults_and_redacts_secrets() {
    let mut values = minimum_config();
    values.insert(
        "DATABASE_URL".to_owned(),
        "postgres://operator:database-secret@db.example.test/control".to_owned(),
    );
    values.insert(
        "BOOTSTRAP_ADMIN_TOKEN".to_owned(),
        "admin-secret".to_owned(),
    );
    values.insert(
        "NATS_URL".to_owned(),
        "nats://queue.example.test:4222".to_owned(),
    );
    values.insert(
        "S3_ENDPOINT".to_owned(),
        "https://objects.example.test/".to_owned(),
    );

    let config = Config::from_map(values).expect("documented environment must be valid");

    assert!(config.api_bind_addr.ip().is_loopback());
    assert!(config.metrics_bind_addr.ip().is_loopback());
    assert_eq!(config.s3.endpoint, "https://objects.example.test");
    assert!(!config.webhook_allow_private_networks);
    assert_eq!(config.debug_token_ttl, Duration::from_secs(900));

    let diagnostic = format!("{config:?}");
    assert!(!diagnostic.contains("database-secret"));
    assert!(!diagnostic.contains("admin-secret"));
    assert!(!diagnostic.contains("local-test-key"));
}

#[tokio::test]
async fn public_errors_keep_the_json_taxonomy_and_bearer_challenge() {
    let response = ApiError::unauthorized().into_response();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response.headers().get(header::WWW_AUTHENTICATE).unwrap(),
        "Bearer"
    );
    let body = response
        .into_body()
        .collect()
        .await
        .expect("error response body must be readable")
        .to_bytes();
    let decoded: ErrorResponse =
        serde_json::from_slice(&body).expect("error response must follow the Rust contract");

    assert_eq!(decoded.error.code, ErrorCode::Unauthorized);
    assert_eq!(decoded.error.message, "authentication is required");
    assert!(decoded.error.request_id.as_str().starts_with("req_"));
    assert!(decoded.error.details.is_none());
}

#[tokio::test]
async fn metrics_endpoint_exposes_request_and_job_create_counters() {
    let metrics = ApiMetrics::default();
    metrics.record_http(
        "POST",
        "/v1/jobs",
        StatusCode::ACCEPTED,
        Duration::from_millis(25),
    );
    metrics.record_job_created(false);
    metrics.record_job_created(true);

    let response = observability::metrics_router(metrics)
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("metrics route must be infallible");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "text/plain; version=0.0.4; charset=utf-8"
    );
    let body = response
        .into_body()
        .collect()
        .await
        .expect("metrics response body must be readable")
        .to_bytes();
    let body = std::str::from_utf8(&body).expect("Prometheus output must be UTF-8");

    assert!(
        body.contains(
            "raa_http_requests_total{method=\"POST\",route=\"/v1/jobs\",status=\"202\"} 1"
        )
    );
    assert!(body.contains("raa_jobs_created_total 1"));
    assert!(body.contains("raa_job_idempotency_replays_total 1"));
}

#[test]
fn upload_signing_inputs_are_checksum_exact_and_tenant_scoped() {
    let project_id = ProjectId::new("proj_contract").unwrap();
    let first = object_store::new_upload_object_key(&project_id, UploadKind::Apk);
    let second = object_store::new_upload_object_key(&project_id, UploadKind::Apk);
    let digest =
        Sha256::new("3a7bd3e2360a3d80e1797c5c2b7961e57092b45f72f874b4fbd02b5e35d7a64c").unwrap();

    assert!(first.starts_with("projects/proj_contract/uploads/apk/"));
    assert_ne!(first, second, "each registration must receive a unique key");
    assert_eq!(
        object_store::checksum_base64(&digest),
        "OnvT4jYKPYDheXxcK3lh5XCStF9y+HS0+9ArXjXXpkw="
    );
}

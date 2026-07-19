use axum::{
    Json,
    extract::State,
    http::{HeaderMap, StatusCode},
};
use run_anywhere_contracts::{AuthScope, CreateJobRequest, Job, JobPage, JobState, UploadKind};
use run_anywhere_repository::{JobListQuery, StoredUpload};
use serde::Deserialize;

use crate::{
    auth::{Authenticated, require_owned_resource},
    error::{ApiError, ApiResult},
    extract::{ApiJson, ApiPath, ApiQuery},
    observability::{current_trace_headers, record_job_id},
    params::{JobPath, validate_cursor},
    service_error,
    state::AppState,
};

const IDEMPOTENCY_KEY: &str = "idempotency-key";

#[derive(Debug, Deserialize)]
pub struct ListJobsQuery {
    pub project_id: run_anywhere_contracts::ProjectId,
    pub status: Option<JobState>,
    pub cursor: Option<String>,
}

pub async fn create_job(
    State(state): State<AppState>,
    Authenticated(auth): Authenticated,
    headers: HeaderMap,
    ApiJson(request): ApiJson<CreateJobRequest>,
) -> ApiResult<(StatusCode, Json<Job>)> {
    auth.require_scope(AuthScope::ProjectWrite)?;
    auth.require_project(&request.project_id)?;
    let idempotency_key = parse_idempotency_key(&headers)?;

    // A retry returns the winner before re-checking mutable external objects or
    // comparing the retried body. The key is scoped by project in PostgreSQL.
    if let Some(job) = state
        .repository
        .find_job_by_idempotency_key(&request.project_id, &idempotency_key)
        .await?
    {
        state.metrics.record_job_created(true);
        record_job_id(&job.id);
        return Ok((StatusCode::ACCEPTED, Json(job)));
    }

    if request.test_upload_id.as_ref() == Some(&request.apk_upload_id) {
        return Err(ApiError::validation(
            "APK and test uploads must be different",
        ));
    }

    let apk = load_owned_upload(&state, &auth, &request.apk_upload_id).await?;
    require_upload_kind(&apk, UploadKind::Apk)?;
    state
        .object_store
        .verify_upload(&apk.s3_key, apk.size_bytes, &apk.sha256)
        .await
        .map_err(service_error::object_store)?;

    if let Some(test_upload_id) = request.test_upload_id.as_ref() {
        let test = load_owned_upload(&state, &auth, test_upload_id).await?;
        require_upload_kind(&test, UploadKind::Test)?;
        state
            .object_store
            .verify_upload(&test.s3_key, test.size_bytes, &test.sha256)
            .await
            .map_err(service_error::object_store)?;
    }

    let created = state
        .repository
        .create_job_with_outbox(request, idempotency_key, current_trace_headers())
        .await?;
    state.metrics.record_job_created(!created.was_created);
    record_job_id(&created.job.id);

    Ok((StatusCode::ACCEPTED, Json(created.job)))
}

pub async fn list_jobs(
    State(state): State<AppState>,
    Authenticated(auth): Authenticated,
    ApiQuery(query): ApiQuery<ListJobsQuery>,
) -> ApiResult<Json<JobPage>> {
    auth.require_scope(AuthScope::ProjectRead)?;
    auth.require_project(&query.project_id)?;
    validate_cursor(query.cursor.as_deref())?;

    let mut repository_query = JobListQuery::new(query.project_id);
    repository_query.state = query.status;
    repository_query.cursor = query.cursor;
    Ok(Json(state.repository.list_jobs(repository_query).await?))
}

pub async fn get_job(
    State(state): State<AppState>,
    Authenticated(auth): Authenticated,
    ApiPath(path): ApiPath<JobPath>,
) -> ApiResult<Json<Job>> {
    auth.require_scope(AuthScope::ProjectRead)?;
    let job = state
        .repository
        .get_job(&path.job_id)
        .await?
        .ok_or_else(|| ApiError::not_found("job"))?;
    require_owned_resource(&auth, &job.project_id, "job")?;
    record_job_id(&job.id);
    Ok(Json(job))
}

pub async fn cancel_job(
    State(state): State<AppState>,
    Authenticated(auth): Authenticated,
    ApiPath(path): ApiPath<JobPath>,
) -> ApiResult<(StatusCode, Json<Job>)> {
    auth.require_scope(AuthScope::ProjectWrite)?;
    let current = state
        .repository
        .get_job(&path.job_id)
        .await?
        .ok_or_else(|| ApiError::not_found("job"))?;
    require_owned_resource(&auth, &current.project_id, "job")?;

    let mutation = state
        .repository
        .request_job_cancellation(&path.job_id)
        .await?;
    record_job_id(&mutation.job.id);
    Ok((StatusCode::ACCEPTED, Json(mutation.job)))
}

async fn load_owned_upload(
    state: &AppState,
    auth: &crate::auth::AuthContext,
    upload_id: &run_anywhere_contracts::UploadId,
) -> ApiResult<StoredUpload> {
    let upload = state
        .repository
        .get_upload(upload_id)
        .await?
        .ok_or_else(|| ApiError::not_found("upload"))?;
    require_owned_resource(auth, &upload.project_id, "upload")?;
    Ok(upload)
}

fn require_upload_kind(upload: &StoredUpload, expected: UploadKind) -> ApiResult<()> {
    if upload.kind == expected {
        Ok(())
    } else {
        Err(ApiError::validation(format!(
            "upload {} must have kind {}",
            upload.id,
            upload_kind_name(expected)
        )))
    }
}

const fn upload_kind_name(kind: UploadKind) -> &'static str {
    match kind {
        UploadKind::Apk => "apk",
        UploadKind::Test => "test",
        UploadKind::Script => "script",
    }
}

fn parse_idempotency_key(headers: &HeaderMap) -> ApiResult<String> {
    let value = headers
        .get(IDEMPOTENCY_KEY)
        .ok_or_else(|| ApiError::validation("Idempotency-Key header is required"))?
        .to_str()
        .map_err(|_| ApiError::validation("Idempotency-Key must contain visible ASCII"))?;
    if value.is_empty()
        || value.len() > 255
        || !value.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
    {
        return Err(ApiError::validation(
            "Idempotency-Key must contain 1 to 255 visible ASCII characters",
        ));
    }
    Ok(value.to_owned())
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;

    #[test]
    fn idempotency_key_is_required_and_visible_ascii() {
        let mut headers = HeaderMap::new();
        assert!(parse_idempotency_key(&headers).is_err());
        headers.insert(IDEMPOTENCY_KEY, HeaderValue::from_static("ci-42"));
        assert_eq!(parse_idempotency_key(&headers).unwrap(), "ci-42");
        headers.insert(IDEMPOTENCY_KEY, HeaderValue::from_static("has space"));
        assert!(parse_idempotency_key(&headers).is_err());
    }
}

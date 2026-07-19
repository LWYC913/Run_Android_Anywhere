use std::collections::BTreeMap;

use axum::{Json, extract::State, http::StatusCode};
use chrono::{Duration, Utc};
use run_anywhere_contracts::{AuthScope, DebugSessionRequest, DebugSessionToken, JobState, Uri};
use serde_json::json;
use uuid::Uuid;

use crate::{
    auth::{Authenticated, require_owned_resource},
    error::{ApiError, ApiResult},
    extract::{ApiJson, ApiPath},
    observability::record_job_id,
    params::JobPath,
    service_error,
    state::AppState,
};

const MINIMUM_TOKEN_LIFETIME: Duration = Duration::seconds(30);

pub async fn create_debug_session(
    State(state): State<AppState>,
    Authenticated(auth): Authenticated,
    ApiPath(path): ApiPath<JobPath>,
    ApiJson(request): ApiJson<DebugSessionRequest>,
) -> ApiResult<(StatusCode, Json<DebugSessionToken>)> {
    auth.require_scope(AuthScope::DebugCreate)?;
    let job = state
        .repository
        .get_job(&path.job_id)
        .await?
        .ok_or_else(|| ApiError::not_found("job"))?;
    require_owned_resource(&auth, &job.project_id, "job")?;
    record_job_id(&job.id);
    if job.state != JobState::DebugAvailable {
        return Err(ApiError::conflict(
            "debug sessions require a job in debug_available state",
        ));
    }

    let now = Utc::now();
    let configured_ttl = Duration::from_std(state.config.debug_token_ttl).map_err(|error| {
        tracing::error!(error = %error, "debug token TTL cannot be represented by chrono");
        ApiError::internal()
    })?;
    let started_at = job.started_at.ok_or_else(|| {
        tracing::error!(job_id = %job.id, "debug_available job has no started_at timestamp");
        ApiError::internal()
    })?;
    let job_timeout = i64::try_from(job.timeout_seconds.get()).map_err(|_| {
        tracing::error!(job_id = %job.id, "job timeout exceeds chrono range");
        ApiError::internal()
    })?;
    let job_deadline = started_at
        .checked_add_signed(Duration::seconds(job_timeout))
        .ok_or_else(|| {
            tracing::error!(job_id = %job.id, "job deadline overflows timestamp range");
            ApiError::internal()
        })?;
    let token_deadline = now.checked_add_signed(configured_ttl).ok_or_else(|| {
        tracing::error!("debug token deadline overflows timestamp range");
        ApiError::internal()
    })?;
    let expires_at = job_deadline.min(token_deadline);
    if expires_at < now + MINIMUM_TOKEN_LIFETIME {
        return Err(ApiError::conflict(
            "the remaining job lifetime is too short for a debug session",
        ));
    }

    let jti = format!("jti_{}", Uuid::new_v4().simple());
    let audit_payload = BTreeMap::from([
        ("job_id".to_owned(), json!(job.id)),
        ("project_id".to_owned(), json!(job.project_id)),
        ("mode".to_owned(), json!(request.mode)),
        ("expires_at".to_owned(), json!(expires_at)),
    ]);
    let created = state
        .repository
        .create_debug_session_with_audit(
            &job.id,
            &jti,
            &auth.actor,
            request.mode,
            expires_at,
            audit_payload,
        )
        .await?;
    let token = state
        .debug_tokens
        .mint(&job.id, &created.session.id, &jti, request.mode, expires_at)
        .map_err(service_error::debug_token)?;
    let connect_url = Uri::new(format!(
        "{}/sessions/{}",
        state.config.debug_gateway_base_url, created.session.id
    ))
    .map_err(|error| {
        tracing::error!(error = %error, "configured debug gateway produced an invalid URI");
        ApiError::internal()
    })?;
    state.metrics.record_debug_session_created();

    Ok((
        StatusCode::CREATED,
        Json(DebugSessionToken {
            session_id: created.session.id,
            job_id: job.id,
            mode: request.mode,
            token,
            connect_url,
            expires_at,
        }),
    ))
}

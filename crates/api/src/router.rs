use std::time::Duration;

use axum::{
    Router,
    extract::{DefaultBodyLimit, Request, State},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use run_anywhere_contracts::AuthScope;
use tower_http::catch_panic::CatchPanicLayer;

use crate::{
    artifacts, auth, auth::AuthState, debug_sessions, error::ApiError, events, jobs, observability,
    projects, request_context, runtime_profiles, state::AppState, uploads, webhooks, workers,
};

pub fn public_router(state: AppState) -> Router {
    let auth_state = AuthState::new(
        state.repository.clone(),
        state.config.bootstrap_admin_token.clone(),
    );
    let metrics = state.metrics.clone();
    let timeout = state.config.request_timeout;
    let max_json_body_bytes = state.config.max_json_body_bytes;

    Router::new()
        .merge(scoped(
            Router::new().route("/v1/projects", post(projects::create_project)),
            AuthScope::Admin,
        ))
        .merge(scoped(
            Router::new().route("/v1/uploads/apk", post(uploads::create_upload)),
            AuthScope::ProjectWrite,
        ))
        .merge(scoped(
            Router::new().route("/v1/jobs", post(jobs::create_job)),
            AuthScope::ProjectWrite,
        ))
        .merge(scoped(
            Router::new().route("/v1/jobs", get(jobs::list_jobs)),
            AuthScope::ProjectRead,
        ))
        .merge(scoped(
            Router::new().route("/v1/jobs/{job_id}", get(jobs::get_job)),
            AuthScope::ProjectRead,
        ))
        .merge(scoped(
            Router::new().route("/v1/jobs/{job_id}/events", get(events::stream_job_events)),
            AuthScope::ProjectRead,
        ))
        .merge(scoped(
            Router::new().route(
                "/v1/jobs/{job_id}/artifacts",
                get(artifacts::list_job_artifacts),
            ),
            AuthScope::ProjectRead,
        ))
        .merge(scoped(
            Router::new().route(
                "/v1/jobs/{job_id}/debug-sessions",
                post(debug_sessions::create_debug_session),
            ),
            AuthScope::DebugCreate,
        ))
        .merge(scoped(
            Router::new().route("/v1/jobs/{job_id}/cancel", post(jobs::cancel_job)),
            AuthScope::ProjectWrite,
        ))
        .merge(scoped(
            Router::new().route("/v1/webhooks", post(webhooks::create_webhook)),
            AuthScope::ProjectWrite,
        ))
        .merge(scoped(
            Router::new().route("/v1/workers", get(workers::list_workers)),
            AuthScope::Admin,
        ))
        .merge(scoped(
            Router::new().route(
                "/v1/runtime-profiles",
                get(runtime_profiles::list_runtime_profiles),
            ),
            AuthScope::Admin,
        ))
        .fallback(route_not_found)
        .layer(DefaultBodyLimit::max(max_json_body_bytes))
        .layer(middleware::from_fn_with_state(
            auth_state,
            auth::authenticate,
        ))
        .layer(CatchPanicLayer::custom(|_| {
            ApiError::internal().into_response()
        }))
        .layer(middleware::from_fn_with_state(timeout, request_timeout))
        .layer(middleware::from_fn_with_state(
            metrics,
            observability::observe_http,
        ))
        .layer(middleware::from_fn(request_context::request_context))
        .with_state(state)
}

fn scoped(router: Router<AppState>, scope: AuthScope) -> Router<AppState> {
    router.route_layer(middleware::from_fn_with_state(scope, auth::scope_guard))
}

async fn request_timeout(
    State(timeout): State<Duration>,
    request: Request,
    next: Next,
) -> Response {
    match tokio::time::timeout(timeout, next.run(request)).await {
        Ok(response) => response,
        Err(_) => ApiError::infra_failed().into_response(),
    }
}

async fn route_not_found() -> ApiError {
    ApiError::not_found("route")
}

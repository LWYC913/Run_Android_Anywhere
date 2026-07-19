use axum::{Json, extract::State, http::StatusCode};
use run_anywhere_contracts::{AuthScope, CreateProjectRequest, CreateProjectResponse};

use crate::{
    auth::Authenticated,
    error::{ApiError, ApiResult},
    extract::ApiJson,
    state::AppState,
};

const INITIAL_PROJECT_SCOPES: [AuthScope; 3] = [
    AuthScope::ProjectRead,
    AuthScope::ProjectWrite,
    AuthScope::DebugCreate,
];

pub async fn create_project(
    State(state): State<AppState>,
    Authenticated(auth): Authenticated,
    ApiJson(request): ApiJson<CreateProjectRequest>,
) -> ApiResult<(StatusCode, Json<CreateProjectResponse>)> {
    auth.require_scope(AuthScope::Admin)?;
    validate_name(&request.name)?;

    let created = state
        .repository
        .create_project_with_api_key(request.name, auth.actor, INITIAL_PROJECT_SCOPES.to_vec())
        .await?;

    Ok((
        StatusCode::CREATED,
        Json(CreateProjectResponse {
            project: created.project,
            api_key: created.api_key.key.into_secret(),
            scopes: INITIAL_PROJECT_SCOPES.to_vec(),
        }),
    ))
}

fn validate_name(name: &str) -> ApiResult<()> {
    if name.trim().is_empty() {
        return Err(ApiError::validation("project name must not be blank"));
    }
    if name.chars().count() > 255 {
        return Err(ApiError::validation(
            "project name must contain at most 255 characters",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_names_follow_the_openapi_bounds() {
        assert!(validate_name("Mobile QA").is_ok());
        assert!(validate_name(" \t ").is_err());
        assert!(validate_name(&"x".repeat(256)).is_err());
    }
}

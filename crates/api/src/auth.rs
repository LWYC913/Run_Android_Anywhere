//! Bearer API-key authentication and authorization helpers.

use axum::{
    extract::{FromRequestParts, Request, State},
    http::{header, request::Parts},
    middleware::Next,
    response::Response,
};
use run_anywhere_contracts::{AuthScope, ProjectId};
use run_anywhere_repository::{Repository, RepositoryError};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::{
    config::SecretString,
    error::{ApiError, ApiResult},
};

const MAX_BEARER_TOKEN_BYTES: usize = 4 * 1024;

#[derive(Clone, Debug)]
pub struct AuthState {
    pub repository: Repository,
    pub bootstrap_admin_token: Option<SecretString>,
}

impl AuthState {
    pub fn new(repository: Repository, bootstrap_admin_token: Option<SecretString>) -> Self {
        Self {
            repository,
            bootstrap_admin_token,
        }
    }
}

/// Identity and tenant boundary established by authentication middleware.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthContext {
    pub actor: String,
    pub key_id: Option<String>,
    pub project_id: Option<ProjectId>,
    pub scopes: Vec<AuthScope>,
}

impl AuthContext {
    pub fn has_scope(&self, required: AuthScope) -> bool {
        self.scopes.contains(&required)
    }

    pub fn require_scope(&self, required: AuthScope) -> ApiResult<()> {
        require_scope(self, required)
    }

    pub fn require_project(&self, project_id: &ProjectId) -> ApiResult<()> {
        require_project(self, project_id)
    }
}

/// Authentication middleware for use with `from_fn_with_state(AuthState, ...)`.
pub async fn authenticate(
    State(state): State<AuthState>,
    mut request: Request,
    next: Next,
) -> ApiResult<Response> {
    let token = bearer_token(&request)?;

    let context = if state
        .bootstrap_admin_token
        .as_ref()
        .is_some_and(|configured| constant_time_token_eq(token, configured.expose_secret()))
    {
        AuthContext {
            actor: "bootstrap-admin".to_owned(),
            key_id: None,
            project_id: None,
            // The bootstrap identity deliberately receives only `admin`;
            // admin does not implicitly bypass project route guards.
            scopes: vec![AuthScope::Admin],
        }
    } else {
        authenticate_api_key(&state.repository, token).await?
    };

    request.extensions_mut().insert(context);
    Ok(next.run(request).await)
}

async fn authenticate_api_key(repository: &Repository, token: &str) -> ApiResult<AuthContext> {
    let hash = Repository::hash_api_key(token);
    let Some(record) = repository.find_api_key_by_hash(hash).await? else {
        return Err(ApiError::unauthorized());
    };
    if record.revoked_at.is_some() {
        return Err(ApiError::unauthorized());
    }

    // Touch synchronously so a successful request cannot race ahead of the
    // key's audit metadata. A concurrent revocation makes the request fail.
    let record = match repository.touch_api_key_last_used(&record.id).await {
        Ok(record) => record,
        Err(RepositoryError::NotFound { .. }) => return Err(ApiError::unauthorized()),
        Err(error) => return Err(error.into()),
    };

    Ok(AuthContext {
        actor: format!("api_key:{}", record.id),
        key_id: Some(record.id),
        project_id: Some(record.project_id),
        scopes: record.scopes,
    })
}

/// Route-level scope guard for use with
/// `from_fn_with_state(AuthScope::ProjectRead, scope_guard)`.
pub async fn scope_guard(
    State(required): State<AuthScope>,
    request: Request,
    next: Next,
) -> ApiResult<Response> {
    let context = request
        .extensions()
        .get::<AuthContext>()
        .ok_or_else(ApiError::unauthorized)?;
    require_scope(context, required)?;
    Ok(next.run(request).await)
}

pub fn require_scope(context: &AuthContext, required: AuthScope) -> ApiResult<()> {
    if context.has_scope(required) {
        Ok(())
    } else {
        Err(ApiError::forbidden("the API key lacks the required scope"))
    }
}

/// Enforce an explicit project ID supplied in a body or query. A mismatch is
/// a permission error because the client itself supplied the tenant ID.
pub fn require_project(context: &AuthContext, project_id: &ProjectId) -> ApiResult<()> {
    if context.project_id.as_ref() == Some(project_id) {
        Ok(())
    } else {
        Err(ApiError::forbidden(
            "the requested project is outside the authenticated project",
        ))
    }
}

/// Enforce ownership of a resource loaded by path while hiding whether a
/// foreign-tenant resource exists.
pub fn require_owned_resource(
    context: &AuthContext,
    project_id: &ProjectId,
    resource_name: &str,
) -> ApiResult<()> {
    if context.project_id.as_ref() == Some(project_id) {
        Ok(())
    } else {
        Err(ApiError::not_found(resource_name))
    }
}

/// Extractor for handlers that prefer a typed identity over
/// `Extension<AuthContext>`.
#[derive(Clone, Debug)]
pub struct Authenticated(pub AuthContext);

impl<S> FromRequestParts<S> for Authenticated
where
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> ApiResult<Self> {
        parts
            .extensions
            .get::<AuthContext>()
            .cloned()
            .map(Self)
            .ok_or_else(ApiError::unauthorized)
    }
}

fn bearer_token(request: &Request) -> ApiResult<&str> {
    let value = request
        .headers()
        .get(header::AUTHORIZATION)
        .ok_or_else(ApiError::unauthorized)?
        .to_str()
        .map_err(|_| ApiError::unauthorized())?;
    let Some((scheme, token)) = value.split_once(' ') else {
        return Err(ApiError::unauthorized());
    };
    if !scheme.eq_ignore_ascii_case("bearer")
        || token.is_empty()
        || token.len() > MAX_BEARER_TOKEN_BYTES
        || token.bytes().any(|byte| byte.is_ascii_whitespace())
    {
        return Err(ApiError::unauthorized());
    }
    Ok(token)
}

fn constant_time_token_eq(candidate: &str, configured: &str) -> bool {
    let candidate_digest: [u8; 32] = Sha256::digest(candidate.as_bytes()).into();
    let configured_digest: [u8; 32] = Sha256::digest(configured.as_bytes()).into();
    bool::from(candidate_digest.ct_eq(&configured_digest))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn project(value: &str) -> ProjectId {
        ProjectId::new(value).unwrap()
    }

    #[test]
    fn bootstrap_admin_does_not_implicitly_cross_project_boundaries() {
        let context = AuthContext {
            actor: "bootstrap-admin".to_owned(),
            key_id: None,
            project_id: None,
            scopes: vec![AuthScope::Admin],
        };

        assert!(require_scope(&context, AuthScope::Admin).is_ok());
        assert!(require_scope(&context, AuthScope::ProjectRead).is_err());
        assert!(require_project(&context, &project("proj_one")).is_err());
    }

    #[test]
    fn project_scope_and_ownership_are_exact() {
        let context = AuthContext {
            actor: "api_key:key_one".to_owned(),
            key_id: Some("key_one".to_owned()),
            project_id: Some(project("proj_one")),
            scopes: vec![AuthScope::ProjectRead],
        };

        assert!(require_project(&context, &project("proj_one")).is_ok());
        assert!(require_project(&context, &project("proj_two")).is_err());
        assert_eq!(
            require_owned_resource(&context, &project("proj_two"), "job")
                .unwrap_err()
                .code(),
            run_anywhere_contracts::ErrorCode::NotFound
        );
    }

    #[test]
    fn bootstrap_comparison_checks_the_complete_token() {
        assert!(constant_time_token_eq("secret", "secret"));
        assert!(!constant_time_token_eq("secret", "secret2"));
        assert!(!constant_time_token_eq("", "secret"));
    }
}

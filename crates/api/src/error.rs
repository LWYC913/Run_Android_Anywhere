//! Stable HTTP error responses for the control-plane API.

use std::{collections::BTreeMap, fmt};

use axum::{
    Json,
    extract::rejection::{JsonRejection, PathRejection, QueryRejection},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use run_anywhere_contracts::{
    ApiError as ErrorBody, ErrorCode, ErrorResponse, PrimitiveValidationError,
};
use run_anywhere_repository::RepositoryError;
use serde_json::Value;

use crate::request_context::current_request_id;

pub type ApiResult<T> = Result<T, ApiError>;

/// An application error that is safe to serialize to an API client.
pub struct ApiError {
    code: ErrorCode,
    message: String,
    details: Option<BTreeMap<String, Value>>,
}

impl ApiError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            details: None,
        }
    }

    pub fn validation(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Validation, message)
    }

    pub fn unauthorized() -> Self {
        Self::new(ErrorCode::Unauthorized, "authentication is required")
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Forbidden, message)
    }

    pub fn not_found(entity: &str) -> Self {
        Self::new(ErrorCode::NotFound, format!("{entity} was not found"))
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Conflict, message)
    }

    pub fn quota_exceeded(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::QuotaExceeded, message)
    }

    pub fn infra_failed() -> Self {
        Self::new(
            ErrorCode::InfraFailed,
            "a required infrastructure service is unavailable",
        )
    }

    pub fn internal() -> Self {
        Self::new(ErrorCode::InternalError, "an internal error occurred")
    }

    pub fn with_details(mut self, details: BTreeMap<String, Value>) -> Self {
        if !details.is_empty() {
            self.details = Some(details);
        }
        self
    }

    pub fn with_detail(mut self, name: impl Into<String>, value: Value) -> Self {
        self.details
            .get_or_insert_with(BTreeMap::new)
            .insert(name.into(), value);
        self
    }

    pub const fn code(&self) -> ErrorCode {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn status(&self) -> StatusCode {
        StatusCode::from_u16(self.code.http_status())
            .expect("contract error codes always map to valid HTTP statuses")
    }
}

impl fmt::Debug for ApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApiError")
            .field("code", &self.code)
            .field("message", &self.message)
            .field("details", &self.details)
            .finish()
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for ApiError {}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let request_id = current_request_id();
        let status = self.status();

        if status.is_server_error() {
            tracing::error!(
                request_id = %request_id,
                error_code = ?self.code,
                "request failed"
            );
        }

        let body = ErrorResponse {
            error: ErrorBody {
                code: self.code,
                message: self.message,
                request_id,
                details: self.details,
            },
        };
        let mut response = (status, Json(body)).into_response();
        if status == StatusCode::UNAUTHORIZED {
            response.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                "Bearer".parse().expect("static header value is valid"),
            );
        }
        response
    }
}

impl From<RepositoryError> for ApiError {
    fn from(error: RepositoryError) -> Self {
        match error {
            RepositoryError::Validation(message) => Self::validation(message),
            RepositoryError::NotFound { entity, .. } => Self::not_found(entity),
            RepositoryError::Conflict(message) => Self::conflict(message),
            RepositoryError::InvalidTransition(error) => Self::conflict(error.to_string()),
            RepositoryError::CompareAndSwapLost { .. } => {
                Self::conflict("the resource changed while the request was being processed")
            }
            RepositoryError::Sqlx(_) | RepositoryError::Migration(_) => Self::infra_failed(),
            RepositoryError::Decode { .. } => Self::internal(),
        }
    }
}

impl From<PrimitiveValidationError> for ApiError {
    fn from(error: PrimitiveValidationError) -> Self {
        Self::validation(error.to_string())
    }
}

impl From<JsonRejection> for ApiError {
    fn from(error: JsonRejection) -> Self {
        Self::validation(error.body_text())
    }
}

impl From<QueryRejection> for ApiError {
    fn from(error: QueryRejection) -> Self {
        Self::validation(error.body_text())
    }
}

impl From<PathRejection> for ApiError {
    fn from(error: PathRejection) -> Self {
        Self::validation(error.body_text())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repository_failures_have_stable_taxonomy() {
        assert_eq!(
            ApiError::from(RepositoryError::Validation("bad input".to_owned())).code(),
            ErrorCode::Validation
        );
        assert_eq!(
            ApiError::from(RepositoryError::Conflict("duplicate".to_owned())).code(),
            ErrorCode::Conflict
        );
    }

    #[test]
    fn empty_detail_maps_are_omitted() {
        let error = ApiError::validation("bad input").with_details(BTreeMap::new());
        assert!(error.details.is_none());
    }
}

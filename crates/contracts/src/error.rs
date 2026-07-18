use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

use crate::RequestId;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    Validation,
    Unauthorized,
    Forbidden,
    NotFound,
    Conflict,
    QuotaExceeded,
    InfraFailed,
    InternalError,
}

impl ErrorCode {
    /// Stable HTTP status mapping without depending on an HTTP framework.
    pub const fn http_status(self) -> u16 {
        match self {
            Self::Validation => 400,
            Self::Unauthorized => 401,
            Self::Forbidden => 403,
            Self::NotFound => 404,
            Self::Conflict => 409,
            Self::QuotaExceeded => 429,
            Self::InfraFailed => 503,
            Self::InternalError => 500,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ApiError {
    pub code: ErrorCode,
    pub message: String,
    pub request_id: RequestId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<BTreeMap<String, Value>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ErrorResponse {
    pub error: ApiError,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_codes_have_stable_http_mappings() {
        let expected = [
            (ErrorCode::Validation, 400),
            (ErrorCode::Unauthorized, 401),
            (ErrorCode::Forbidden, 403),
            (ErrorCode::NotFound, 404),
            (ErrorCode::Conflict, 409),
            (ErrorCode::QuotaExceeded, 429),
            (ErrorCode::InfraFailed, 503),
            (ErrorCode::InternalError, 500),
        ];

        for (code, status) in expected {
            assert_eq!(code.http_status(), status);
        }
    }

    #[test]
    fn error_code_wire_names_are_snake_case() {
        assert_eq!(
            serde_json::to_string(&ErrorCode::QuotaExceeded).unwrap(),
            r#""quota_exceeded""#
        );
    }
}

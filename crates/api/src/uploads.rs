use axum::{Json, extract::State, http::StatusCode};
use run_anywhere_contracts::{AuthScope, CreateUploadRequest, CreateUploadResponse};

use crate::{
    auth::Authenticated,
    error::{ApiError, ApiResult},
    extract::ApiJson,
    object_store::new_upload_object_key,
    service_error,
    state::AppState,
};

pub async fn create_upload(
    State(state): State<AppState>,
    Authenticated(auth): Authenticated,
    ApiJson(request): ApiJson<CreateUploadRequest>,
) -> ApiResult<(StatusCode, Json<CreateUploadResponse>)> {
    auth.require_scope(AuthScope::ProjectWrite)?;
    auth.require_project(&request.project_id)?;
    validate_request(&request)?;

    let object_key = new_upload_object_key(&request.project_id, request.kind);
    let signed = state
        .object_store
        .presign_upload(
            &object_key,
            &request.content_type,
            request.size_bytes,
            &request.sha256,
        )
        .await
        .map_err(service_error::object_store)?;
    let stored = state
        .repository
        .create_upload(
            &request.project_id,
            request.kind,
            object_key,
            request.sha256,
            request.size_bytes,
        )
        .await?;

    Ok((
        StatusCode::CREATED,
        Json(CreateUploadResponse {
            upload_id: stored.id,
            upload_url: signed.url,
            required_headers: signed.required_headers,
            expires_at: signed.expires_at,
        }),
    ))
}

fn validate_request(request: &CreateUploadRequest) -> ApiResult<()> {
    if request.file_name.trim().is_empty() {
        return Err(ApiError::validation("file_name must not be blank"));
    }
    if request
        .file_name
        .bytes()
        .any(|byte| byte.is_ascii_control())
    {
        return Err(ApiError::validation(
            "file_name must not contain control characters",
        ));
    }
    if request.content_type.trim().is_empty()
        || request
            .content_type
            .parse::<axum::http::HeaderValue>()
            .is_err()
    {
        return Err(ApiError::validation(
            "content_type must be a valid HTTP header value",
        ));
    }
    if request.size_bytes > i64::MAX as u64 {
        return Err(ApiError::validation("size_bytes is too large"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use run_anywhere_contracts::{ProjectId, Sha256, UploadKind};

    use super::*;

    fn request() -> CreateUploadRequest {
        CreateUploadRequest {
            project_id: ProjectId::new("proj_demo").unwrap(),
            kind: UploadKind::Apk,
            file_name: "app.apk".to_owned(),
            content_type: "application/vnd.android.package-archive".to_owned(),
            size_bytes: 42,
            sha256: Sha256::new("a".repeat(64)).unwrap(),
        }
    }

    #[test]
    fn rejects_blank_names_and_invalid_header_values() {
        assert!(validate_request(&request()).is_ok());
        let mut invalid = request();
        invalid.file_name = "  ".to_owned();
        assert!(validate_request(&invalid).is_err());
        invalid = request();
        invalid.content_type = "bad\nvalue".to_owned();
        assert!(validate_request(&invalid).is_err());
    }
}

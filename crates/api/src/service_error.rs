//! Safe mappings from infrastructure adapters into the public error taxonomy.

use crate::{debug_token::DebugTokenError, error::ApiError, object_store::ObjectStoreError};

pub fn object_store(error: ObjectStoreError) -> ApiError {
    if error.is_upload_validation_failure()
        || matches!(
            &error,
            ObjectStoreError::InvalidContentType | ObjectStoreError::ObjectTooLarge
        )
    {
        ApiError::validation(error.to_string())
    } else if matches!(
        &error,
        ObjectStoreError::Presign(_) | ObjectStoreError::Head(_)
    ) {
        ApiError::infra_failed()
    } else {
        tracing::error!(error = %error, "object-store adapter rejected server configuration");
        ApiError::internal()
    }
}

pub fn debug_token(error: DebugTokenError) -> ApiError {
    tracing::error!(error = %error, "debug token minting failed");
    ApiError::internal()
}

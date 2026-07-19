use std::{collections::BTreeMap, fmt, time::Duration};

use async_trait::async_trait;
use aws_sdk_s3::{Client, presigning::PresigningConfig, types::ChecksumMode};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use chrono::{DateTime, Utc};
use run_anywhere_contracts::{ProjectId, Sha256, UploadKind, Uri};
use thiserror::Error;
use uuid::Uuid;

pub const CHECKSUM_SHA256_HEADER: &str = "x-amz-checksum-sha256";
pub const CONTENT_TYPE_HEADER: &str = "Content-Type";
pub const MAX_PRESIGN_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// The information a caller needs to perform one exact-key S3 upload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PresignedUpload {
    pub url: Uri,
    pub required_headers: BTreeMap<String, String>,
    pub expires_at: DateTime<Utc>,
}

/// A short-lived exact-key download URL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PresignedDownload {
    pub url: Uri,
    pub expires_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ObjectStoreError {
    #[error("object-store bucket must not be blank")]
    InvalidBucket,
    #[error("object key must not be blank or contain control characters")]
    InvalidObjectKey,
    #[error("content type must not be blank or contain control characters")]
    InvalidContentType,
    #[error("presigned URL TTL must be between one second and seven days")]
    InvalidPresignTtl,
    #[error("object size exceeds S3's signed content-length representation")]
    ObjectTooLarge,
    #[error("failed to create an S3 presigned request: {0}")]
    Presign(String),
    #[error("S3 returned a malformed presigned URL")]
    InvalidPresignedUrl,
    #[error("object does not exist")]
    NotFound,
    #[error("failed to inspect the object: {0}")]
    Head(String),
    #[error("object HEAD response omitted content length")]
    MissingContentLength,
    #[error("object size mismatch: expected {expected} bytes, received {actual} bytes")]
    SizeMismatch { expected: u64, actual: u64 },
    #[error("object HEAD response omitted the SHA-256 checksum")]
    MissingChecksum,
    #[error("object SHA-256 checksum does not match the registered upload")]
    ChecksumMismatch,
}

impl ObjectStoreError {
    /// Failures caused by an absent or mismatched client upload are request errors,
    /// while signing and transport failures are infrastructure errors.
    pub const fn is_upload_validation_failure(&self) -> bool {
        matches!(
            self,
            Self::NotFound
                | Self::MissingContentLength
                | Self::SizeMismatch { .. }
                | Self::MissingChecksum
                | Self::ChecksumMismatch
        )
    }
}

/// Object-storage boundary used by handlers. Tests can inject an in-memory fake
/// without constructing an AWS client.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn presign_upload(
        &self,
        key: &str,
        content_type: &str,
        size_bytes: u64,
        sha256: &Sha256,
    ) -> Result<PresignedUpload, ObjectStoreError>;

    async fn presign_download(&self, key: &str) -> Result<PresignedDownload, ObjectStoreError>;

    async fn verify_upload(
        &self,
        key: &str,
        expected_size: u64,
        expected_sha256: &Sha256,
    ) -> Result<(), ObjectStoreError>;
}

/// AWS S3/MinIO implementation. The client owns endpoint, region, credentials,
/// and path-style behavior; this type owns only bucket and URL lifetimes.
#[derive(Clone)]
pub struct S3ObjectStore {
    client: Client,
    bucket: String,
    put_ttl: Duration,
    get_ttl: Duration,
}

impl fmt::Debug for S3ObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("S3ObjectStore")
            .field("bucket", &self.bucket)
            .field("put_ttl", &self.put_ttl)
            .field("get_ttl", &self.get_ttl)
            .finish_non_exhaustive()
    }
}

impl S3ObjectStore {
    pub fn new(
        client: Client,
        bucket: impl Into<String>,
        put_ttl: Duration,
        get_ttl: Duration,
    ) -> Result<Self, ObjectStoreError> {
        let bucket = bucket.into();
        if bucket.trim().is_empty() {
            return Err(ObjectStoreError::InvalidBucket);
        }
        validate_ttl(put_ttl)?;
        validate_ttl(get_ttl)?;
        Ok(Self {
            client,
            bucket,
            put_ttl,
            get_ttl,
        })
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }
}

#[async_trait]
impl ObjectStore for S3ObjectStore {
    async fn presign_upload(
        &self,
        key: &str,
        content_type: &str,
        size_bytes: u64,
        sha256: &Sha256,
    ) -> Result<PresignedUpload, ObjectStoreError> {
        validate_key(key)?;
        validate_content_type(content_type)?;
        let content_length =
            i64::try_from(size_bytes).map_err(|_| ObjectStoreError::ObjectTooLarge)?;
        let checksum = checksum_base64(sha256);
        let config = PresigningConfig::expires_in(self.put_ttl)
            .map_err(|error| ObjectStoreError::Presign(error.to_string()))?;
        let request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_type(content_type)
            .content_length(content_length)
            .checksum_sha256(&checksum)
            .presigned(config)
            .await
            .map_err(|error| ObjectStoreError::Presign(error.to_string()))?;

        let mut required_headers = BTreeMap::new();
        required_headers.insert(CONTENT_TYPE_HEADER.to_owned(), content_type.to_owned());
        required_headers.insert(CHECKSUM_SHA256_HEADER.to_owned(), checksum);

        Ok(PresignedUpload {
            url: Uri::new(request.uri().to_string())
                .map_err(|_| ObjectStoreError::InvalidPresignedUrl)?,
            required_headers,
            expires_at: expires_at(self.put_ttl)?,
        })
    }

    async fn presign_download(&self, key: &str) -> Result<PresignedDownload, ObjectStoreError> {
        validate_key(key)?;
        let config = PresigningConfig::expires_in(self.get_ttl)
            .map_err(|error| ObjectStoreError::Presign(error.to_string()))?;
        let request = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .presigned(config)
            .await
            .map_err(|error| ObjectStoreError::Presign(error.to_string()))?;
        Ok(PresignedDownload {
            url: Uri::new(request.uri().to_string())
                .map_err(|_| ObjectStoreError::InvalidPresignedUrl)?,
            expires_at: expires_at(self.get_ttl)?,
        })
    }

    async fn verify_upload(
        &self,
        key: &str,
        expected_size: u64,
        expected_sha256: &Sha256,
    ) -> Result<(), ObjectStoreError> {
        validate_key(key)?;
        let output =
            self.client
                .head_object()
                .bucket(&self.bucket)
                .key(key)
                .checksum_mode(ChecksumMode::Enabled)
                .send()
                .await
                .map_err(|error| {
                    if error.as_service_error().is_some_and(
                        aws_sdk_s3::operation::head_object::HeadObjectError::is_not_found,
                    ) {
                        ObjectStoreError::NotFound
                    } else {
                        ObjectStoreError::Head(error.to_string())
                    }
                })?;

        let actual_size = output
            .content_length()
            .ok_or(ObjectStoreError::MissingContentLength)
            .and_then(|value| {
                u64::try_from(value).map_err(|_| ObjectStoreError::MissingContentLength)
            })?;
        if actual_size != expected_size {
            return Err(ObjectStoreError::SizeMismatch {
                expected: expected_size,
                actual: actual_size,
            });
        }

        let actual_checksum = output
            .checksum_sha256()
            .ok_or(ObjectStoreError::MissingChecksum)?;
        if actual_checksum != checksum_base64(expected_sha256) {
            return Err(ObjectStoreError::ChecksumMismatch);
        }
        Ok(())
    }
}

/// Generate a tenant-prefixed object key without incorporating any client-supplied
/// path or filename.
pub fn new_upload_object_key(project_id: &ProjectId, kind: UploadKind) -> String {
    let kind = match kind {
        UploadKind::Apk => "apk",
        UploadKind::Test => "test",
        UploadKind::Script => "script",
    };
    format!(
        "projects/{}/uploads/{kind}/{}",
        project_id.as_str(),
        Uuid::new_v4().simple()
    )
}

pub fn checksum_base64(sha256: &Sha256) -> String {
    let digest = sha256.as_str().as_bytes();
    let mut bytes = [0_u8; 32];
    for (index, pair) in digest.chunks_exact(2).enumerate() {
        bytes[index] = (hex_nibble(pair[0]) << 4) | hex_nibble(pair[1]);
    }
    STANDARD.encode(bytes)
}

fn validate_key(key: &str) -> Result<(), ObjectStoreError> {
    if key.trim().is_empty() || key.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(ObjectStoreError::InvalidObjectKey);
    }
    Ok(())
}

fn validate_content_type(content_type: &str) -> Result<(), ObjectStoreError> {
    if content_type.trim().is_empty() || content_type.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(ObjectStoreError::InvalidContentType);
    }
    Ok(())
}

fn validate_ttl(ttl: Duration) -> Result<(), ObjectStoreError> {
    if ttl.is_zero() || ttl > MAX_PRESIGN_TTL {
        return Err(ObjectStoreError::InvalidPresignTtl);
    }
    Ok(())
}

fn expires_at(ttl: Duration) -> Result<DateTime<Utc>, ObjectStoreError> {
    let ttl = chrono::Duration::from_std(ttl).map_err(|_| ObjectStoreError::InvalidPresignTtl)?;
    Utc::now()
        .checked_add_signed(ttl)
        .ok_or(ObjectStoreError::InvalidPresignTtl)
}

const fn hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_is_converted_to_the_s3_header_encoding() {
        let digest =
            Sha256::new("3a7bd3e2360a3d80e1797c5c2b7961e57092b45f72f874b4fbd02b5e35d7a64c")
                .unwrap();
        assert_eq!(
            checksum_base64(&digest),
            "OnvT4jYKPYDheXxcK3lh5XCStF9y+HS0+9ArXjXXpkw="
        );
    }

    #[test]
    fn generated_keys_are_tenant_prefixed_and_ignore_filenames() {
        let project_id = ProjectId::new("proj_demo").unwrap();
        let first = new_upload_object_key(&project_id, UploadKind::Apk);
        let second = new_upload_object_key(&project_id, UploadKind::Apk);
        assert!(first.starts_with("projects/proj_demo/uploads/apk/"));
        assert_ne!(first, second);
    }

    #[test]
    fn presign_ttl_is_bounded_by_s3_limits() {
        assert!(validate_ttl(Duration::ZERO).is_err());
        assert!(validate_ttl(Duration::from_secs(1)).is_ok());
        assert!(validate_ttl(MAX_PRESIGN_TTL).is_ok());
        assert!(validate_ttl(MAX_PRESIGN_TTL + Duration::from_secs(1)).is_err());
    }
}

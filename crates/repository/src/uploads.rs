use run_anywhere_contracts::{
    ProjectId, RuntimeProfile, RuntimeProfileId, Sha256, UploadId, UploadKind,
};

use crate::{
    Repository, RepositoryError, RepositoryResult, StoredUpload,
    auth::new_id,
    codec::{checked_i64, encode_enum},
    rows::{RuntimeProfileRow, UploadRow},
};

impl Repository {
    pub async fn create_upload(
        &self,
        project_id: &ProjectId,
        kind: UploadKind,
        s3_key: impl Into<String>,
        sha256: Sha256,
        size_bytes: u64,
    ) -> RepositoryResult<StoredUpload> {
        let s3_key = s3_key.into();
        if s3_key.trim().is_empty() {
            return Err(RepositoryError::Validation(
                "upload object key must not be blank".to_owned(),
            ));
        }
        let kind = encode_enum(kind)?;
        let size_bytes = checked_i64("size_bytes", size_bytes)?;
        let row = sqlx::query_as::<_, UploadRow>(
            "INSERT INTO uploads (id, project_id, kind, s3_key, sha256, size_bytes) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             RETURNING id, project_id, kind, s3_key, sha256, size_bytes, created_at",
        )
        .bind(new_id("upl_"))
        .bind(project_id.as_str())
        .bind(kind)
        .bind(s3_key)
        .bind(sha256.as_str())
        .bind(size_bytes)
        .fetch_one(&self.pool)
        .await
        .map_err(|error| RepositoryError::classify_write(error, "upload object already exists"))?;
        row.try_into()
    }

    pub async fn get_upload(&self, upload_id: &UploadId) -> RepositoryResult<Option<StoredUpload>> {
        sqlx::query_as::<_, UploadRow>(
            "SELECT id, project_id, kind, s3_key, sha256, size_bytes, created_at \
             FROM uploads WHERE id = $1",
        )
        .bind(upload_id.as_str())
        .fetch_optional(&self.pool)
        .await?
        .map(TryInto::try_into)
        .transpose()
    }

    pub async fn list_runtime_profiles(&self) -> RepositoryResult<Vec<RuntimeProfile>> {
        sqlx::query_as::<_, RuntimeProfileRow>(
            "SELECT id, android_api, device_profile, abi, host_arch, runtime_kind, image_ref, isolation_tier \
             FROM runtime_profiles ORDER BY android_api DESC, id",
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(TryInto::try_into)
        .collect()
    }

    pub async fn get_runtime_profile(
        &self,
        profile_id: &RuntimeProfileId,
    ) -> RepositoryResult<Option<RuntimeProfile>> {
        sqlx::query_as::<_, RuntimeProfileRow>(
            "SELECT id, android_api, device_profile, abi, host_arch, runtime_kind, image_ref, isolation_tier \
             FROM runtime_profiles WHERE id = $1",
        )
        .bind(profile_id.as_str())
        .fetch_optional(&self.pool)
        .await?
        .map(TryInto::try_into)
        .transpose()
    }
}

use std::collections::HashSet;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use rand::{RngCore, rngs::OsRng};
use run_anywhere_contracts::{AuthScope, Project, ProjectId};
use sha2::{Digest, Sha256};
use sqlx::PgExecutor;

use crate::{
    ApiKeyHash, ApiKeyRecord, ApiKeySecret, CreatedApiKey, Repository, RepositoryError,
    RepositoryResult,
    codec::encode_enum,
    rows::{ApiKeyRow, ProjectRow},
};

impl Repository {
    pub async fn create_project(
        &self,
        name: impl Into<String>,
        owner: impl Into<String>,
    ) -> RepositoryResult<Project> {
        let name = name.into();
        let owner = owner.into();
        if name.trim().is_empty() {
            return Err(RepositoryError::Validation(
                "project name must not be blank".to_owned(),
            ));
        }
        if owner.trim().is_empty() {
            return Err(RepositoryError::Validation(
                "project owner must not be blank".to_owned(),
            ));
        }

        let row = sqlx::query_as::<_, ProjectRow>(
            "INSERT INTO projects (id, name, owner) VALUES ($1, $2, $3) \
             RETURNING id, name, owner, created_at",
        )
        .bind(new_id("proj_"))
        .bind(name)
        .bind(owner)
        .fetch_one(&self.pool)
        .await
        .map_err(|error| RepositoryError::classify_write(error, "project already exists"))?;
        row.try_into()
    }

    pub async fn get_project(&self, project_id: &ProjectId) -> RepositoryResult<Option<Project>> {
        sqlx::query_as::<_, ProjectRow>(
            "SELECT id, name, owner, created_at FROM projects WHERE id = $1",
        )
        .bind(project_id.as_str())
        .fetch_optional(&self.pool)
        .await?
        .map(TryInto::try_into)
        .transpose()
    }

    pub async fn create_api_key(
        &self,
        project_id: &ProjectId,
        scopes: Vec<AuthScope>,
    ) -> RepositoryResult<CreatedApiKey> {
        if scopes.is_empty() {
            return Err(RepositoryError::Validation(
                "an API key must have at least one scope".to_owned(),
            ));
        }
        let mut seen = HashSet::new();
        let scopes = scopes
            .into_iter()
            .map(encode_enum)
            .collect::<RepositoryResult<Vec<_>>>()?;
        if !scopes.iter().all(|scope| seen.insert(scope.clone())) {
            return Err(RepositoryError::Validation(
                "API key scopes must be unique".to_owned(),
            ));
        }

        let mut random = [0_u8; 32];
        OsRng.fill_bytes(&mut random);
        let plaintext = format!("raa_sk_{}", URL_SAFE_NO_PAD.encode(random));
        let hash = Self::hash_api_key(&plaintext);

        let row = sqlx::query_as::<_, ApiKeyRow>(
            "INSERT INTO api_keys (id, project_id, key_hash, scopes) VALUES ($1, $2, $3, $4) \
             RETURNING id, project_id, key_hash, scopes, created_at, last_used_at, revoked_at",
        )
        .bind(new_id("key_"))
        .bind(project_id.as_str())
        .bind(hash.as_bytes().as_slice())
        .bind(scopes)
        .fetch_one(&self.pool)
        .await
        .map_err(|error| RepositoryError::classify_write(error, "API key already exists"))?;

        Ok(CreatedApiKey {
            key: ApiKeySecret::new(plaintext),
            record: row.into_record()?,
        })
    }

    pub fn hash_api_key(plaintext: &str) -> ApiKeyHash {
        let digest: [u8; 32] = Sha256::digest(plaintext.as_bytes()).into();
        ApiKeyHash::new(digest)
    }

    pub async fn find_api_key_by_hash(
        &self,
        hash: ApiKeyHash,
    ) -> RepositoryResult<Option<ApiKeyRecord>> {
        sqlx::query_as::<_, ApiKeyRow>(
            "SELECT id, project_id, key_hash, scopes, created_at, last_used_at, revoked_at \
             FROM api_keys WHERE key_hash = $1",
        )
        .bind(hash.as_bytes().as_slice())
        .fetch_optional(&self.pool)
        .await?
        .map(ApiKeyRow::into_record)
        .transpose()
    }

    pub async fn revoke_api_key(&self, key_id: &str) -> RepositoryResult<ApiKeyRecord> {
        update_api_key_timestamp(
            &self.pool,
            "UPDATE api_keys SET revoked_at = COALESCE(revoked_at, now()) WHERE id = $1 \
             RETURNING id, project_id, key_hash, scopes, created_at, last_used_at, revoked_at",
            key_id,
        )
        .await
    }

    pub async fn touch_api_key_last_used(&self, key_id: &str) -> RepositoryResult<ApiKeyRecord> {
        update_api_key_timestamp(
            &self.pool,
            "UPDATE api_keys SET last_used_at = now() WHERE id = $1 AND revoked_at IS NULL \
             RETURNING id, project_id, key_hash, scopes, created_at, last_used_at, revoked_at",
            key_id,
        )
        .await
    }
}

async fn update_api_key_timestamp<'e, E>(
    executor: E,
    statement: &str,
    key_id: &str,
) -> RepositoryResult<ApiKeyRecord>
where
    E: PgExecutor<'e>,
{
    let row = sqlx::query_as::<_, ApiKeyRow>(statement)
        .bind(key_id)
        .fetch_optional(executor)
        .await?
        .ok_or_else(|| RepositoryError::not_found("API key", key_id))?;
    row.into_record()
}

pub(crate) fn new_id(prefix: &str) -> String {
    format!("{prefix}{}", uuid::Uuid::new_v4().simple())
}

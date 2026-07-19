use axum::{Json, extract::State};
use run_anywhere_contracts::{ArtifactPage, AuthScope};

use crate::{
    auth::{Authenticated, require_owned_resource},
    error::{ApiError, ApiResult},
    extract::{ApiPath, ApiQuery},
    observability::record_job_id,
    params::{CursorQuery, JobPath, validate_cursor},
    service_error,
    state::AppState,
};

pub async fn list_job_artifacts(
    State(state): State<AppState>,
    Authenticated(auth): Authenticated,
    ApiPath(path): ApiPath<JobPath>,
    ApiQuery(query): ApiQuery<CursorQuery>,
) -> ApiResult<Json<ArtifactPage>> {
    auth.require_scope(AuthScope::ProjectRead)?;
    validate_cursor(query.cursor.as_deref())?;
    let job = state
        .repository
        .get_job(&path.job_id)
        .await?
        .ok_or_else(|| ApiError::not_found("job"))?;
    require_owned_resource(&auth, &job.project_id, "job")?;
    record_job_id(&job.id);

    let page = state
        .repository
        .list_artifacts_page(&path.job_id, query.cursor.as_deref())
        .await?;
    let mut items = Vec::with_capacity(page.items.len());
    for stored in page.items {
        let signed = state
            .object_store
            .presign_download(&stored.s3_key)
            .await
            .map_err(service_error::object_store)?;
        let mut artifact = stored.artifact;
        artifact.download_url = Some(signed.url);
        artifact.download_expires_at = Some(signed.expires_at);
        items.push(artifact);
    }

    Ok(Json(ArtifactPage {
        items,
        next_cursor: page.next_cursor,
    }))
}

use axum::{Json, extract::State};
use run_anywhere_contracts::{AuthScope, WorkerPage};

use crate::{
    auth::Authenticated,
    error::ApiResult,
    extract::ApiQuery,
    params::{CursorQuery, validate_cursor},
    state::AppState,
};

pub async fn list_workers(
    State(state): State<AppState>,
    Authenticated(auth): Authenticated,
    ApiQuery(query): ApiQuery<CursorQuery>,
) -> ApiResult<Json<WorkerPage>> {
    auth.require_scope(AuthScope::Admin)?;
    validate_cursor(query.cursor.as_deref())?;
    Ok(Json(
        state
            .repository
            .list_workers_page(query.cursor.as_deref())
            .await?,
    ))
}

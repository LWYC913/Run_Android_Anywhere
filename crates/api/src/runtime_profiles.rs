use axum::{Json, extract::State};
use run_anywhere_contracts::{AuthScope, RuntimeProfilePage};

use crate::{
    auth::Authenticated,
    error::ApiResult,
    extract::ApiQuery,
    params::{CursorQuery, validate_cursor},
    state::AppState,
};

pub async fn list_runtime_profiles(
    State(state): State<AppState>,
    Authenticated(auth): Authenticated,
    ApiQuery(query): ApiQuery<CursorQuery>,
) -> ApiResult<Json<RuntimeProfilePage>> {
    auth.require_scope(AuthScope::Admin)?;
    validate_cursor(query.cursor.as_deref())?;
    Ok(Json(
        state
            .repository
            .list_runtime_profiles_page(query.cursor.as_deref())
            .await?,
    ))
}

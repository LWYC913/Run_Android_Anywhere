use run_anywhere_contracts::JobId;
use serde::Deserialize;

use crate::error::{ApiError, ApiResult};

#[derive(Debug, Deserialize)]
pub struct JobPath {
    pub job_id: JobId,
}

#[derive(Debug, Default, Deserialize)]
pub struct CursorQuery {
    pub cursor: Option<String>,
}

pub fn validate_cursor(cursor: Option<&str>) -> ApiResult<()> {
    if let Some(cursor) = cursor {
        if cursor.is_empty() || cursor.len() > 2_048 {
            return Err(ApiError::validation(
                "cursor must contain between 1 and 2048 bytes",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_bounds_match_the_contract() {
        assert!(validate_cursor(None).is_ok());
        assert!(validate_cursor(Some("next")).is_ok());
        assert!(validate_cursor(Some("")).is_err());
        assert!(validate_cursor(Some(&"x".repeat(2_049))).is_err());
    }
}

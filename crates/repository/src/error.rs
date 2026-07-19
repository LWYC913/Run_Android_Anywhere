use run_anywhere_contracts::TransitionError;
use thiserror::Error;

pub type RepositoryResult<T> = Result<T, RepositoryError>;

#[derive(Debug, Error)]
pub enum RepositoryError {
    #[error("validation failed: {0}")]
    Validation(String),
    #[error("{entity} `{id}` was not found")]
    NotFound { entity: &'static str, id: String },
    #[error("conflict: {0}")]
    Conflict(String),
    #[error(transparent)]
    InvalidTransition(#[from] TransitionError),
    #[error("compare-and-swap lost while updating {entity} `{id}`")]
    CompareAndSwapLost { entity: &'static str, id: String },
    #[error("failed to decode database field `{field}`: {message}")]
    Decode {
        field: &'static str,
        message: String,
    },
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    Migration(#[from] sqlx::migrate::MigrateError),
}

impl RepositoryError {
    pub(crate) fn not_found(entity: &'static str, id: impl Into<String>) -> Self {
        Self::NotFound {
            entity,
            id: id.into(),
        }
    }

    pub(crate) fn decode(field: &'static str, error: impl ToString) -> Self {
        Self::Decode {
            field,
            message: error.to_string(),
        }
    }

    pub(crate) fn classify_write(error: sqlx::Error, conflict: &'static str) -> Self {
        if error
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::code)
            .is_some_and(|code| code == "23505")
        {
            Self::Conflict(conflict.to_owned())
        } else {
            Self::Sqlx(error)
        }
    }
}

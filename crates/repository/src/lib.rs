//! PostgreSQL-backed persistence for Run Android Anywhere.
//!
//! The crate deliberately keeps SQL representations private. Values crossing the
//! boundary are validated `run-anywhere-contracts` types or the persistence-only
//! records re-exported at the crate root.

#![forbid(unsafe_code)]

mod codec;
mod error;
mod models;
mod rows;

mod auth;
mod jobs;
mod misc;
mod uploads;
mod workers;

use sqlx::{PgPool, postgres::PgPoolOptions};

pub use error::{RepositoryError, RepositoryResult};
pub use models::*;

/// Root-level migrations embedded into every service that uses the repository.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

/// Cloneable handle to the application's PostgreSQL database.
#[derive(Clone, Debug)]
pub struct Repository {
    pool: PgPool,
}

impl Repository {
    /// Connect with conservative defaults suitable for service startup.
    pub async fn connect(database_url: &str) -> RepositoryResult<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await?;
        Ok(Self::new(pool))
    }

    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Apply all embedded migrations.
    pub async fn migrate(&self) -> RepositoryResult<()> {
        MIGRATOR.run(&self.pool).await?;
        Ok(())
    }
}

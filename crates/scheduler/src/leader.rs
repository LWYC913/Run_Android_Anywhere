//! Active/passive scheduler leadership on a dedicated PostgreSQL session.

use sqlx::{Connection as _, PgConnection};
use thiserror::Error;

/// Stable, repository-wide session advisory lock (`RAA_SCHD` in ASCII).
pub const SCHEDULER_ADVISORY_LOCK_KEY: i64 = 0x5241_415F_5343_4844;

#[derive(Debug, Error)]
pub enum LeaderLockError {
    #[error("could not open the PostgreSQL leadership session: {0}")]
    Connect(#[source] sqlx::Error),
    #[error("could not inspect or change scheduler leadership: {0}")]
    Query(#[source] sqlx::Error),
}

/// Ownership of the scheduler advisory lock and the session that holds it.
/// Dropping the connection releases leadership even after an ungraceful task
/// cancellation.
pub struct LeaderGuard {
    connection: PgConnection,
}

impl std::fmt::Debug for LeaderGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LeaderGuard")
            .field("connection", &"[dedicated PostgreSQL session]")
            .finish()
    }
}

impl LeaderGuard {
    pub async fn try_acquire(database_url: &str) -> Result<Option<Self>, LeaderLockError> {
        let mut connection = PgConnection::connect(database_url)
            .await
            .map_err(LeaderLockError::Connect)?;
        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(SCHEDULER_ADVISORY_LOCK_KEY)
            .fetch_one(&mut connection)
            .await
            .map_err(LeaderLockError::Query)?;
        Ok(acquired.then_some(Self { connection }))
    }

    /// The lock is session-scoped, so a successful check proves both that the
    /// connection is alive and that this dedicated session still owns a lock.
    pub async fn is_held(&mut self) -> Result<bool, LeaderLockError> {
        sqlx::query_scalar(
            "SELECT EXISTS (\
                 SELECT 1 FROM pg_locks \
                 WHERE locktype = 'advisory' AND pid = pg_backend_pid() AND granted\
             )",
        )
        .fetch_one(&mut self.connection)
        .await
        .map_err(LeaderLockError::Query)
    }

    pub async fn release(mut self) -> Result<(), LeaderLockError> {
        let _: bool = sqlx::query_scalar("SELECT pg_advisory_unlock($1)")
            .bind(SCHEDULER_ADVISORY_LOCK_KEY)
            .fetch_one(&mut self.connection)
            .await
            .map_err(LeaderLockError::Query)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn one_database_session_is_leader_at_a_time() {
        if std::env::var("RUN_SCHEDULER_INTEGRATION").as_deref() != Ok("true") {
            return;
        }
        let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL for integration");

        let mut first = LeaderGuard::try_acquire(&database_url)
            .await
            .expect("first leadership query")
            .expect("first session becomes leader");
        assert!(first.is_held().await.expect("leadership health check"));
        assert!(
            LeaderGuard::try_acquire(&database_url)
                .await
                .expect("second leadership query")
                .is_none()
        );
        first.release().await.expect("release leadership");
        assert!(
            LeaderGuard::try_acquire(&database_url)
                .await
                .expect("replacement leadership query")
                .is_some()
        );
    }
}

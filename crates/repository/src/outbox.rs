use chrono::{DateTime, Duration, Utc};

use crate::{
    OutboxMessage, Repository, RepositoryError, RepositoryResult, codec::to_u64, rows::OutboxRow,
};

const OUTBOX_COLUMNS: &str = "id, event_key, subject, payload, trace_headers, available_at, \
    attempts, locked_by, locked_at, published_at, last_error, created_at";

impl Repository {
    /// Lease ready messages without blocking other dispatcher replicas.
    /// Stale leases older than `lease_timeout` are reclaimed and every claim
    /// increments the durable attempt counter.
    pub async fn lease_outbox_messages(
        &self,
        dispatcher_id: &str,
        lease_timeout: Duration,
        limit: u32,
    ) -> RepositoryResult<Vec<OutboxMessage>> {
        validate_dispatcher_id(dispatcher_id)?;
        let lease_seconds = lease_timeout.num_milliseconds() as f64 / 1_000.0;
        if lease_seconds <= 0.0 {
            return Err(RepositoryError::Validation(
                "outbox lease timeout must be positive".to_owned(),
            ));
        }
        let limit = crate::models::checked_limit(limit)?;
        let query = format!(
            "WITH candidates AS (\
                 SELECT id FROM outbox_messages \
                 WHERE published_at IS NULL AND available_at <= clock_timestamp() \
                 AND (locked_at IS NULL OR locked_at <= \
                      clock_timestamp() - make_interval(secs => $2)) \
                 ORDER BY available_at, id \
                 FOR UPDATE SKIP LOCKED LIMIT $3\
             ) \
             UPDATE outbox_messages AS message \
             SET locked_by = $1, locked_at = clock_timestamp(), attempts = attempts + 1 \
             FROM candidates WHERE message.id = candidates.id \
             RETURNING {}",
            OUTBOX_COLUMNS
                .split(", ")
                .map(|column| format!("message.{column}"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let mut messages = sqlx::query_as::<_, OutboxRow>(&query)
            .bind(dispatcher_id)
            .bind(lease_seconds)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(TryInto::try_into)
            .collect::<RepositoryResult<Vec<OutboxMessage>>>()?;
        messages.sort_by_key(|message| (message.available_at, message.id));
        Ok(messages)
    }

    /// Lease only one logical outbox channel. Independent job-queue and
    /// webhook dispatchers can therefore share the durable table without
    /// stealing or poisoning each other's messages.
    pub async fn lease_outbox_messages_for_subject(
        &self,
        dispatcher_id: &str,
        subject: &str,
        lease_timeout: Duration,
        limit: u32,
    ) -> RepositoryResult<Vec<OutboxMessage>> {
        validate_dispatcher_id(dispatcher_id)?;
        validate_subject(subject)?;
        let lease_seconds = lease_timeout.num_milliseconds() as f64 / 1_000.0;
        if lease_seconds <= 0.0 {
            return Err(RepositoryError::Validation(
                "outbox lease timeout must be positive".to_owned(),
            ));
        }
        let limit = crate::models::checked_limit(limit)?;
        let query = format!(
            "WITH candidates AS (\
                 SELECT id FROM outbox_messages \
                 WHERE published_at IS NULL AND subject = $2 \
                 AND available_at <= clock_timestamp() \
                 AND (locked_at IS NULL OR locked_at <= \
                      clock_timestamp() - make_interval(secs => $3)) \
                 ORDER BY available_at, id \
                 FOR UPDATE SKIP LOCKED LIMIT $4\
             ) \
             UPDATE outbox_messages AS message \
             SET locked_by = $1, locked_at = clock_timestamp(), attempts = attempts + 1 \
             FROM candidates WHERE message.id = candidates.id \
             RETURNING {}",
            OUTBOX_COLUMNS
                .split(", ")
                .map(|column| format!("message.{column}"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let mut messages = sqlx::query_as::<_, OutboxRow>(&query)
            .bind(dispatcher_id)
            .bind(subject)
            .bind(lease_seconds)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(TryInto::try_into)
            .collect::<RepositoryResult<Vec<OutboxMessage>>>()?;
        messages.sort_by_key(|message| (message.available_at, message.id));
        Ok(messages)
    }

    pub async fn mark_outbox_published(
        &self,
        message_id: i64,
        dispatcher_id: &str,
    ) -> RepositoryResult<OutboxMessage> {
        validate_dispatcher_id(dispatcher_id)?;
        let query = format!(
            "UPDATE outbox_messages SET published_at = clock_timestamp(), \
             locked_by = NULL, locked_at = NULL, last_error = NULL \
             WHERE id = $1 AND published_at IS NULL AND locked_by = $2 \
             RETURNING {OUTBOX_COLUMNS}"
        );
        sqlx::query_as::<_, OutboxRow>(&query)
            .bind(message_id)
            .bind(dispatcher_id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| RepositoryError::CompareAndSwapLost {
                entity: "outbox message lease",
                id: message_id.to_string(),
            })?
            .try_into()
    }

    pub async fn retry_outbox_message(
        &self,
        message_id: i64,
        dispatcher_id: &str,
        available_at: DateTime<Utc>,
        last_error: impl Into<String>,
    ) -> RepositoryResult<OutboxMessage> {
        validate_dispatcher_id(dispatcher_id)?;
        let last_error = last_error.into();
        if last_error.trim().is_empty() {
            return Err(RepositoryError::Validation(
                "outbox retry error must not be blank".to_owned(),
            ));
        }
        let query = format!(
            "UPDATE outbox_messages SET available_at = $3, last_error = $4, \
             locked_by = NULL, locked_at = NULL \
             WHERE id = $1 AND published_at IS NULL AND locked_by = $2 \
             RETURNING {OUTBOX_COLUMNS}"
        );
        sqlx::query_as::<_, OutboxRow>(&query)
            .bind(message_id)
            .bind(dispatcher_id)
            .bind(available_at)
            .bind(last_error)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| RepositoryError::CompareAndSwapLost {
                entity: "outbox message lease",
                id: message_id.to_string(),
            })?
            .try_into()
    }

    pub async fn get_outbox_message(
        &self,
        event_key: &str,
    ) -> RepositoryResult<Option<OutboxMessage>> {
        let query = format!("SELECT {OUTBOX_COLUMNS} FROM outbox_messages WHERE event_key = $1");
        sqlx::query_as::<_, OutboxRow>(&query)
            .bind(event_key)
            .fetch_optional(&self.pool)
            .await?
            .map(TryInto::try_into)
            .transpose()
    }

    pub async fn pending_outbox_count(&self) -> RepositoryResult<u64> {
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM outbox_messages WHERE published_at IS NULL")
                .fetch_one(&self.pool)
                .await?;
        to_u64("outbox pending count", count)
    }

    pub async fn pending_outbox_count_for_subject(&self, subject: &str) -> RepositoryResult<u64> {
        validate_subject(subject)?;
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM outbox_messages WHERE published_at IS NULL AND subject = $1",
        )
        .bind(subject)
        .fetch_one(&self.pool)
        .await?;
        to_u64("outbox pending count", count)
    }
}

fn validate_dispatcher_id(dispatcher_id: &str) -> RepositoryResult<()> {
    if dispatcher_id.trim().is_empty() {
        return Err(RepositoryError::Validation(
            "outbox dispatcher ID must not be blank".to_owned(),
        ));
    }
    Ok(())
}

fn validate_subject(subject: &str) -> RepositoryResult<()> {
    if subject.trim().is_empty() {
        return Err(RepositoryError::Validation(
            "outbox subject must not be blank".to_owned(),
        ));
    }
    Ok(())
}

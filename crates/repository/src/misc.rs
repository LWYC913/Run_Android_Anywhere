use std::collections::{BTreeMap, HashSet};

use chrono::{DateTime, Utc};
use run_anywhere_contracts::{
    CreateWebhookRequest, DebugSessionMode, JobId, JobState, ProjectId, Webhook, WebhookEvent,
    WebhookId,
};
use serde_json::Value;
use sqlx::Postgres;

use crate::{
    AuditEntry, CreatedDebugSession, Repository, RepositoryError, RepositoryResult,
    StoredDebugSession,
    auth::new_id,
    codec::{decode_enum, encode_enum, encode_json},
    rows::{AuditRow, DebugSessionRow, WebhookRow},
};

impl Repository {
    pub async fn create_debug_session(
        &self,
        job_id: &JobId,
        jti: impl Into<String>,
        created_by: impl Into<String>,
        mode: DebugSessionMode,
        expires_at: DateTime<Utc>,
    ) -> RepositoryResult<StoredDebugSession> {
        let jti = jti.into();
        let created_by = created_by.into();
        validate_debug_session(&jti, &created_by)?;
        let mut tx = self.pool.begin().await?;
        lock_debug_session_job(&mut tx, job_id).await?;
        end_expired_debug_session(&mut tx, job_id).await?;
        let session =
            insert_debug_session(&mut tx, job_id, &jti, &created_by, mode, expires_at).await?;
        tx.commit().await?;
        Ok(session)
    }

    /// Persist a debug session and its immutable audit record atomically.
    /// This control-plane entry point also guards the job's debug state.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_debug_session_with_audit(
        &self,
        job_id: &JobId,
        jti: impl Into<String>,
        created_by: impl Into<String>,
        mode: DebugSessionMode,
        expires_at: DateTime<Utc>,
        audit_payload: BTreeMap<String, Value>,
    ) -> RepositoryResult<CreatedDebugSession> {
        let jti = jti.into();
        let created_by = created_by.into();
        validate_debug_session(&jti, &created_by)?;
        let mut tx = self.pool.begin().await?;
        let state = lock_debug_session_job(&mut tx, job_id).await?;
        let state: JobState = decode_enum("jobs.state", state)?;
        if state != JobState::DebugAvailable {
            return Err(RepositoryError::Conflict(
                "debug sessions require a job in debug_available state".to_owned(),
            ));
        }
        end_expired_debug_session(&mut tx, job_id).await?;
        let session =
            insert_debug_session(&mut tx, job_id, &jti, &created_by, mode, expires_at).await?;
        let audit = insert_audit(
            &mut tx,
            &created_by,
            "debug_session.created",
            session.id.as_str(),
            audit_payload,
        )
        .await?;
        tx.commit().await?;
        Ok(CreatedDebugSession { session, audit })
    }

    pub async fn end_debug_session(
        &self,
        session_id: &run_anywhere_contracts::DebugSessionId,
    ) -> RepositoryResult<StoredDebugSession> {
        let row = sqlx::query_as::<_, DebugSessionRow>(
            "UPDATE debug_sessions SET ended_at = COALESCE(ended_at, now()) WHERE id = $1 \
             RETURNING id, job_id, jti, created_by, mode, created_at, expires_at, ended_at",
        )
        .bind(session_id.as_str())
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| RepositoryError::not_found("debug session", session_id.as_str()))?;
        row.try_into()
    }

    pub async fn find_expired_debug_sessions(
        &self,
        expired_before: DateTime<Utc>,
        limit: u32,
    ) -> RepositoryResult<Vec<StoredDebugSession>> {
        let limit = crate::models::checked_limit(limit)?;
        sqlx::query_as::<_, DebugSessionRow>(
            "SELECT id, job_id, jti, created_by, mode, created_at, expires_at, ended_at \
             FROM debug_sessions WHERE ended_at IS NULL AND expires_at <= $1 \
             ORDER BY expires_at, id LIMIT $2",
        )
        .bind(expired_before)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(TryInto::try_into)
        .collect()
    }

    pub async fn append_audit(
        &self,
        actor: impl Into<String>,
        action: impl Into<String>,
        subject: impl Into<String>,
        payload: BTreeMap<String, Value>,
    ) -> RepositoryResult<AuditEntry> {
        let actor = actor.into();
        let action = action.into();
        let subject = subject.into();
        if actor.trim().is_empty() || action.trim().is_empty() || subject.trim().is_empty() {
            return Err(RepositoryError::Validation(
                "audit actor, action, and subject must not be blank".to_owned(),
            ));
        }
        let payload = encode_json("audit_log.payload", payload)?;
        let row = sqlx::query_as::<_, AuditRow>(
            "INSERT INTO audit_log (actor, action, subject, payload) VALUES ($1, $2, $3, $4) \
             RETURNING id, actor, action, subject, timestamp, payload",
        )
        .bind(actor)
        .bind(action)
        .bind(subject)
        .bind(payload)
        .fetch_one(&self.pool)
        .await?;
        row.try_into()
    }

    pub async fn create_webhook(&self, request: CreateWebhookRequest) -> RepositoryResult<Webhook> {
        if request.events.is_empty() {
            return Err(RepositoryError::Validation(
                "webhook must subscribe to at least one event".to_owned(),
            ));
        }
        let events = request
            .events
            .into_iter()
            .map(encode_enum)
            .collect::<RepositoryResult<Vec<_>>>()?;
        if events.iter().collect::<HashSet<_>>().len() != events.len() {
            return Err(RepositoryError::Validation(
                "webhook events must be unique".to_owned(),
            ));
        }
        let row = sqlx::query_as::<_, WebhookRow>(
            "INSERT INTO webhooks (id, project_id, url, events) VALUES ($1, $2, $3, $4) \
             RETURNING id, project_id, url, events, active, created_at",
        )
        .bind(new_id("wh_"))
        .bind(request.project_id.as_str())
        .bind(request.url.as_str())
        .bind(events)
        .fetch_one(&self.pool)
        .await
        .map_err(|error| {
            RepositoryError::classify_write(error, "webhook URL already registered for project")
        })?;
        row.try_into()
    }

    pub async fn get_webhook(&self, webhook_id: &WebhookId) -> RepositoryResult<Option<Webhook>> {
        sqlx::query_as::<_, WebhookRow>(
            "SELECT id, project_id, url, events, active, created_at FROM webhooks WHERE id = $1",
        )
        .bind(webhook_id.as_str())
        .fetch_optional(&self.pool)
        .await?
        .map(TryInto::try_into)
        .transpose()
    }

    pub async fn list_active_webhooks(
        &self,
        project_id: &ProjectId,
        event: Option<WebhookEvent>,
    ) -> RepositoryResult<Vec<Webhook>> {
        let event = event.map(encode_enum).transpose()?;
        sqlx::query_as::<_, WebhookRow>(
            "SELECT id, project_id, url, events, active, created_at FROM webhooks \
             WHERE project_id = $1 AND active AND ($2::TEXT IS NULL OR $2 = ANY(events)) \
             ORDER BY created_at, id",
        )
        .bind(project_id.as_str())
        .bind(event)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(TryInto::try_into)
        .collect()
    }

    pub async fn deactivate_webhook(&self, webhook_id: &WebhookId) -> RepositoryResult<Webhook> {
        let row = sqlx::query_as::<_, WebhookRow>(
            "UPDATE webhooks SET active = FALSE WHERE id = $1 \
             RETURNING id, project_id, url, events, active, created_at",
        )
        .bind(webhook_id.as_str())
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| RepositoryError::not_found("webhook", webhook_id.as_str()))?;
        row.try_into()
    }
}

fn validate_debug_session(jti: &str, created_by: &str) -> RepositoryResult<()> {
    if jti.trim().is_empty() || created_by.trim().is_empty() {
        return Err(RepositoryError::Validation(
            "debug session jti and creator must not be blank".to_owned(),
        ));
    }
    Ok(())
}

async fn lock_debug_session_job(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    job_id: &JobId,
) -> RepositoryResult<String> {
    sqlx::query_scalar("SELECT state FROM jobs WHERE id = $1 FOR UPDATE")
        .bind(job_id.as_str())
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| RepositoryError::not_found("job", job_id.as_str()))
}

async fn end_expired_debug_session(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    job_id: &JobId,
) -> RepositoryResult<()> {
    sqlx::query(
        "UPDATE debug_sessions SET ended_at = clock_timestamp() \
         WHERE job_id = $1 AND ended_at IS NULL AND expires_at <= clock_timestamp()",
    )
    .bind(job_id.as_str())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn insert_debug_session(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    job_id: &JobId,
    jti: &str,
    created_by: &str,
    mode: DebugSessionMode,
    expires_at: DateTime<Utc>,
) -> RepositoryResult<StoredDebugSession> {
    let mode = encode_enum(mode)?;
    let row = sqlx::query_as::<_, DebugSessionRow>(
        "INSERT INTO debug_sessions (id, job_id, jti, created_by, mode, expires_at) \
         SELECT $1, $2, $3, $4, $5, $6 WHERE $6 > clock_timestamp() \
         RETURNING id, job_id, jti, created_by, mode, created_at, expires_at, ended_at",
    )
    .bind(new_id("dbg_"))
    .bind(job_id.as_str())
    .bind(jti)
    .bind(created_by)
    .bind(mode)
    .bind(expires_at)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| {
        RepositoryError::classify_write(
            error,
            "an active debug session already exists for the job or jti",
        )
    })?
    .ok_or_else(|| {
        RepositoryError::Validation(
            "debug session expiry must be later than database time".to_owned(),
        )
    })?;
    row.try_into()
}

async fn insert_audit(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    actor: &str,
    action: &str,
    subject: &str,
    payload: BTreeMap<String, Value>,
) -> RepositoryResult<AuditEntry> {
    let payload = encode_json("audit_log.payload", payload)?;
    let row = sqlx::query_as::<_, AuditRow>(
        "INSERT INTO audit_log (actor, action, subject, payload) VALUES ($1, $2, $3, $4) \
         RETURNING id, actor, action, subject, timestamp, payload",
    )
    .bind(actor)
    .bind(action)
    .bind(subject)
    .bind(payload)
    .fetch_one(&mut **tx)
    .await?;
    row.try_into()
}

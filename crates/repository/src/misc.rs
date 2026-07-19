use std::collections::{BTreeMap, HashSet};

use chrono::{DateTime, Utc};
use run_anywhere_contracts::{
    CreateWebhookRequest, DebugSessionMode, JobId, ProjectId, Webhook, WebhookEvent, WebhookId,
};
use serde_json::Value;

use crate::{
    AuditEntry, Repository, RepositoryError, RepositoryResult, StoredDebugSession,
    auth::new_id,
    codec::{encode_enum, encode_json},
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
        if jti.trim().is_empty() || created_by.trim().is_empty() {
            return Err(RepositoryError::Validation(
                "debug session jti and creator must not be blank".to_owned(),
            ));
        }
        let mode = encode_enum(mode)?;
        let row = sqlx::query_as::<_, DebugSessionRow>(
            "INSERT INTO debug_sessions (id, job_id, jti, created_by, mode, expires_at) \
             SELECT $1, $2, $3, $4, $5, $6 WHERE $6 > now() \
             RETURNING id, job_id, jti, created_by, mode, created_at, expires_at, ended_at",
        )
        .bind(new_id("dbg_"))
        .bind(job_id.as_str())
        .bind(jti)
        .bind(created_by)
        .bind(mode)
        .bind(expires_at)
        .fetch_optional(&self.pool)
        .await
        .map_err(|error| {
            RepositoryError::classify_write(error, "debug session jti already exists")
        })?
        .ok_or_else(|| {
            RepositoryError::Validation(
                "debug session expiry must be later than database time".to_owned(),
            )
        })?;
        row.try_into()
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

use std::time::Duration;

use axum::{Json, extract::State, http::StatusCode};
use chrono::{Duration as ChronoDuration, Utc};
use futures_util::future::join_all;
use run_anywhere_contracts::{AuthScope, CreateWebhookRequest, Webhook};
use run_anywhere_repository::{OutboxMessage, WebhookOutboxPayload};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    auth::Authenticated,
    error::{ApiError, ApiResult},
    extract::ApiJson,
    state::AppState,
    webhook::{WebhookDelivery, WebhookValidationError},
};

const WEBHOOK_OUTBOX_SUBJECT: &str = "webhooks.job_state_changed";
const OUTBOX_BATCH_SIZE: u32 = 50;
const OUTBOX_LEASE_TIMEOUT: ChronoDuration = ChronoDuration::seconds(90);
const OUTBOX_RETRY_DELAY: ChronoDuration = ChronoDuration::seconds(5);
const OUTBOX_POLL_INTERVAL: Duration = Duration::from_millis(500);

pub async fn create_webhook(
    State(state): State<AppState>,
    Authenticated(auth): Authenticated,
    ApiJson(request): ApiJson<CreateWebhookRequest>,
) -> ApiResult<(StatusCode, Json<Webhook>)> {
    auth.require_scope(AuthScope::ProjectWrite)?;
    auth.require_project(&request.project_id)?;
    if request.events.is_empty() {
        return Err(ApiError::validation(
            "webhook must subscribe to at least one event",
        ));
    }
    state
        .webhook_dispatcher
        .validate_url(&request.url)
        .await
        .map_err(|error| ApiError::validation(error.to_string()))?;
    let webhook = state.repository.create_webhook(request).await?;
    Ok((StatusCode::CREATED, Json(webhook)))
}

/// Lease transactional state-event rows. `SKIP LOCKED` in the repository makes
/// this safe across API replicas and avoids relying on non-commit-ordered
/// PostgreSQL sequences.
pub async fn run_outbox_dispatcher(
    state: AppState,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let dispatcher_id = format!("webhook-api-{}", Uuid::new_v4().simple());
    loop {
        if *shutdown.borrow() {
            return;
        }
        let messages = match state
            .repository
            .lease_outbox_messages_for_subject(
                &dispatcher_id,
                WEBHOOK_OUTBOX_SUBJECT,
                OUTBOX_LEASE_TIMEOUT,
                OUTBOX_BATCH_SIZE,
            )
            .await
        {
            Ok(messages) => messages,
            Err(error) => {
                tracing::warn!(error = %error, "webhook outbox lease failed");
                if wait_or_shutdown(OUTBOX_POLL_INTERVAL, &mut shutdown).await {
                    return;
                }
                continue;
            }
        };
        let full_batch = messages.len() == OUTBOX_BATCH_SIZE as usize;
        join_all(
            messages
                .into_iter()
                .map(|message| process_outbox_message(&state, &dispatcher_id, message)),
        )
        .await;
        if !full_batch && wait_or_shutdown(OUTBOX_POLL_INTERVAL, &mut shutdown).await {
            return;
        }
    }
}

async fn process_outbox_message(state: &AppState, dispatcher_id: &str, message: OutboxMessage) {
    match dispatch_outbox_message(state, &message).await {
        Ok(()) => {
            if let Err(error) = state
                .repository
                .mark_outbox_published(message.id, dispatcher_id)
                .await
            {
                state.metrics.record_outbox_finalization_failure();
                tracing::error!(message_id = message.id, error = %error, "failed to finalize webhook outbox row");
            }
        }
        Err(error) => {
            let available_at = Utc::now()
                .checked_add_signed(OUTBOX_RETRY_DELAY)
                .unwrap_or_else(Utc::now);
            if let Err(repository_error) = state
                .repository
                .retry_outbox_message(
                    message.id,
                    dispatcher_id,
                    available_at,
                    truncate_error(&error.to_string()),
                )
                .await
            {
                state.metrics.record_outbox_finalization_failure();
                tracing::error!(message_id = message.id, error = %repository_error, "failed to release webhook outbox row for retry");
            }
        }
    }
}

async fn dispatch_outbox_message(
    state: &AppState,
    message: &OutboxMessage,
) -> Result<(), WebhookOutboxError> {
    if message.subject != WEBHOOK_OUTBOX_SUBJECT {
        return Err(WebhookOutboxError::InvalidMessage);
    }
    let payload: WebhookOutboxPayload = serde_json::from_value(message.payload.clone())
        .map_err(|_| WebhookOutboxError::InvalidMessage)?;
    if message.event_key != format!("webhook:{}:{}", payload.event.id, payload.webhook_id)
        || payload.event.state.is_none()
    {
        return Err(WebhookOutboxError::InvalidMessage);
    }
    let Some(webhook) = state.repository.get_webhook(&payload.webhook_id).await? else {
        tracing::warn!(webhook_id = %payload.webhook_id, event_id = %payload.event.id, "webhook outbox references a missing registration");
        return Ok(());
    };
    if !webhook.active {
        return Ok(());
    }
    if webhook.url != payload.url {
        return Err(WebhookOutboxError::InvalidMessage);
    }
    state
        .webhook_dispatcher
        .deliver(&WebhookDelivery {
            delivery_id: message.event_key.clone(),
            url: payload.url,
            event: payload.event,
        })
        .await?;
    Ok(())
}

#[derive(Debug, Error)]
enum WebhookOutboxError {
    #[error("invalid webhook outbox message")]
    InvalidMessage,
    #[error(transparent)]
    Repository(#[from] run_anywhere_repository::RepositoryError),
    #[error(transparent)]
    Delivery(#[from] WebhookValidationError),
}

fn truncate_error(error: &str) -> String {
    const MAX_BYTES: usize = 1_024;
    if error.len() <= MAX_BYTES {
        return error.to_owned();
    }
    let mut end = MAX_BYTES;
    while !error.is_char_boundary(end) {
        end -= 1;
    }
    error[..end].to_owned()
}

async fn wait_or_shutdown(
    duration: Duration,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> bool {
    tokio::select! {
        changed = shutdown.changed() => changed.is_err() || *shutdown.borrow(),
        () = tokio::time::sleep(duration) => false,
    }
}

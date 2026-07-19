use std::{collections::VecDeque, convert::Infallible, time::Duration};

use axum::{
    extract::State,
    http::HeaderMap,
    response::sse::{Event, KeepAlive, Sse},
};
use futures_util::{Stream, stream};
use run_anywhere_contracts::{AuthScope, JobEvent, JobId};
use run_anywhere_repository::Repository;

use crate::{
    auth::{Authenticated, require_owned_resource},
    error::{ApiError, ApiResult},
    extract::ApiPath,
    observability::{ApiMetrics, record_job_id},
    params::JobPath,
    state::AppState,
};

const LAST_EVENT_ID: &str = "last-event-id";
const EVENT_BATCH_SIZE: u32 = 100;
const EMPTY_POLL_INTERVAL: Duration = Duration::from_millis(500);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);

pub async fn stream_job_events(
    State(state): State<AppState>,
    Authenticated(auth): Authenticated,
    headers: HeaderMap,
    ApiPath(path): ApiPath<JobPath>,
) -> ApiResult<Sse<impl Stream<Item = Result<Event, Infallible>>>> {
    auth.require_scope(AuthScope::ProjectRead)?;
    let job = state
        .repository
        .get_job(&path.job_id)
        .await?
        .ok_or_else(|| ApiError::not_found("job"))?;
    require_owned_resource(&auth, &job.project_id, "job")?;
    record_job_id(&job.id);
    let after_sequence = parse_last_event_id(&headers)?;

    state.metrics.sse_connection_opened();
    let stream = stream::unfold(
        EventStreamState {
            repository: state.repository,
            job_id: path.job_id,
            after_sequence,
            pending: VecDeque::new(),
            close_next: false,
            shutdown: state.shutdown,
            _connection: SseConnectionGuard(state.metrics),
        },
        next_event,
    );

    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(HEARTBEAT_INTERVAL)
            .text("heartbeat"),
    ))
}

struct EventStreamState {
    repository: Repository,
    job_id: JobId,
    after_sequence: u64,
    pending: VecDeque<JobEvent>,
    close_next: bool,
    shutdown: tokio::sync::watch::Receiver<bool>,
    _connection: SseConnectionGuard,
}

struct SseConnectionGuard(ApiMetrics);

impl Drop for SseConnectionGuard {
    fn drop(&mut self) {
        self.0.sse_connection_closed();
    }
}

async fn next_event(
    mut state: EventStreamState,
) -> Option<(Result<Event, Infallible>, EventStreamState)> {
    loop {
        if state.close_next || *state.shutdown.borrow() {
            return None;
        }

        if let Some(event) = state.pending.pop_front() {
            state.after_sequence = event.sequence;
            state.close_next = event.state.is_some_and(|job_state| job_state.is_terminal());
            let data = match serde_json::to_string(&event) {
                Ok(data) => data,
                Err(error) => {
                    tracing::error!(
                        job_id = %state.job_id,
                        error = %error,
                        "failed to serialize persisted job event"
                    );
                    return None;
                }
            };
            let wire_event = Event::default()
                .id(event.sequence.to_string())
                .event(event.event_type)
                .data(data);
            return Some((Ok(wire_event), state));
        }

        match state
            .repository
            .list_job_events_after(&state.job_id, state.after_sequence, EVENT_BATCH_SIZE)
            .await
        {
            Ok(events) if !events.is_empty() => {
                state.pending.extend(events);
                continue;
            }
            Ok(_) => {}
            Err(error) => {
                tracing::error!(
                    job_id = %state.job_id,
                    error = %error,
                    "job-event polling failed; closing SSE stream"
                );
                return None;
            }
        }

        match state.repository.get_job(&state.job_id).await {
            Ok(Some(job)) if job.state.is_terminal() => return None,
            Ok(Some(_)) => {
                tokio::select! {
                    changed = state.shutdown.changed() => {
                        if changed.is_err() || *state.shutdown.borrow() {
                            return None;
                        }
                    }
                    () = tokio::time::sleep(EMPTY_POLL_INTERVAL) => {}
                }
            }
            Ok(None) => {
                tracing::warn!(job_id = %state.job_id, "job disappeared during SSE stream");
                return None;
            }
            Err(error) => {
                tracing::error!(
                    job_id = %state.job_id,
                    error = %error,
                    "job terminal-state check failed; closing SSE stream"
                );
                return None;
            }
        }
    }
}

fn parse_last_event_id(headers: &HeaderMap) -> ApiResult<u64> {
    let Some(value) = headers.get(LAST_EVENT_ID) else {
        return Ok(0);
    };
    let value = value
        .to_str()
        .map_err(|_| ApiError::validation("Last-Event-ID must be a non-negative integer"))?;
    let sequence = value
        .parse::<u64>()
        .map_err(|_| ApiError::validation("Last-Event-ID must be a non-negative integer"))?;
    if sequence > i64::MAX as u64 {
        return Err(ApiError::validation(
            "Last-Event-ID exceeds the supported integer range",
        ));
    }
    Ok(sequence)
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;

    #[test]
    fn last_event_id_is_optional_and_non_negative() {
        let mut headers = HeaderMap::new();
        assert_eq!(parse_last_event_id(&headers).unwrap(), 0);
        headers.insert(LAST_EVENT_ID, HeaderValue::from_static("42"));
        assert_eq!(parse_last_event_id(&headers).unwrap(), 42);
        headers.insert(LAST_EVENT_ID, HeaderValue::from_static("-1"));
        assert!(parse_last_event_id(&headers).is_err());
    }
}

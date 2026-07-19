//! Structured tracing, optional OTLP export, and Prometheus metrics.

use std::{
    collections::BTreeMap,
    fmt::Write as _,
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use axum::{
    Router,
    extract::{MatchedPath, Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
    routing::get,
};
use opentelemetry::{
    global,
    propagation::{Extractor, Injector},
    trace::TracerProvider as _,
};
use opentelemetry_otlp::WithExportConfig as _;
use opentelemetry_sdk::{Resource, propagation::TraceContextPropagator, trace::SdkTracerProvider};
use thiserror::Error;
use tracing::Instrument as _;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt as _, util::SubscriberInitExt as _};

use crate::{config::SecretString, request_context::RequestContext};

#[derive(Debug, Error)]
pub enum ObservabilityError {
    #[error("failed to configure OTLP trace export: {0}")]
    Otlp(String),
    #[error("failed to install the global tracing subscriber: {0}")]
    Subscriber(String),
}

/// Keeps the optional exporter alive and flushes it during graceful shutdown.
pub struct TelemetryGuard {
    tracer_provider: Option<SdkTracerProvider>,
}

impl TelemetryGuard {
    pub fn shutdown(&self) {
        if let Some(provider) = &self.tracer_provider {
            if let Err(error) = provider.shutdown() {
                tracing::warn!(error = %error, "failed to flush OpenTelemetry spans");
            }
        }
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Install JSON logging and, when configured, an OTLP/HTTP protobuf exporter.
/// The collector endpoint is treated as a secret and is never logged.
pub fn init_tracing(
    service_name: &'static str,
    otlp_endpoint: Option<&SecretString>,
) -> Result<TelemetryGuard, ObservabilityError> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("run_anywhere_api=info,tower_http=info"));

    global::set_text_map_propagator(TraceContextPropagator::new());

    let resource = Resource::builder().with_service_name(service_name).build();
    let provider = if let Some(endpoint) = otlp_endpoint {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(endpoint.expose_secret())
            .with_timeout(Duration::from_secs(5))
            .build()
            .map_err(|error| ObservabilityError::Otlp(error.to_string()))?;
        SdkTracerProvider::builder()
            .with_resource(resource)
            .with_batch_exporter(exporter)
            .build()
    } else {
        SdkTracerProvider::builder().with_resource(resource).build()
    };
    let tracer = provider.tracer(service_name);
    tracing_subscriber::registry()
        .with(filter)
        .with(
            tracing_subscriber::fmt::layer()
                .json()
                .flatten_event(true)
                .with_current_span(true)
                .with_span_list(true),
        )
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .try_init()
        .map_err(|error| ObservabilityError::Subscriber(error.to_string()))?;
    global::set_tracer_provider(provider.clone());

    tracing::info!(
        service.name = service_name,
        otlp.enabled = otlp_endpoint.is_some(),
        "tracing initialized"
    );
    Ok(TelemetryGuard {
        tracer_provider: Some(provider),
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WebhookDeliveryResult {
    Success,
    Failed,
    Dropped,
}

#[derive(Clone, Debug, Default)]
pub struct ApiMetrics {
    inner: Arc<MetricsInner>,
}

#[derive(Debug, Default)]
struct MetricsInner {
    http: Mutex<BTreeMap<HttpMetricKey, HttpMetricValue>>,
    jobs_created: AtomicU64,
    job_replays: AtomicU64,
    outbox_backlog: AtomicI64,
    outbox_publish_failures: AtomicU64,
    outbox_finalization_failures: AtomicU64,
    sse_connections: AtomicU64,
    debug_sessions_created: AtomicU64,
    webhook_success: AtomicU64,
    webhook_failed: AtomicU64,
    webhook_dropped: AtomicU64,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct HttpMetricKey {
    method: String,
    route: String,
    status: u16,
}

#[derive(Clone, Copy, Debug, Default)]
struct HttpMetricValue {
    count: u64,
    latency_seconds_sum: f64,
}

impl ApiMetrics {
    pub fn record_http(
        &self,
        method: impl Into<String>,
        route: impl Into<String>,
        status: StatusCode,
        latency: Duration,
    ) {
        let key = HttpMetricKey {
            method: method.into(),
            route: route.into(),
            status: status.as_u16(),
        };
        let mut values = self
            .inner
            .http
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let value = values.entry(key).or_default();
        value.count = value.count.saturating_add(1);
        value.latency_seconds_sum += latency.as_secs_f64();
    }

    pub fn record_job_created(&self, replay: bool) {
        if replay {
            self.inner.job_replays.fetch_add(1, Ordering::Relaxed);
        } else {
            self.inner.jobs_created.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn set_outbox_backlog(&self, value: i64) {
        self.inner
            .outbox_backlog
            .store(value.max(0), Ordering::Relaxed);
    }

    pub fn record_outbox_publish_failure(&self) {
        self.inner
            .outbox_publish_failures
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_outbox_finalization_failure(&self) {
        self.inner
            .outbox_finalization_failures
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn sse_connection_opened(&self) {
        self.inner.sse_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn sse_connection_closed(&self) {
        saturating_decrement(&self.inner.sse_connections);
    }

    pub fn record_debug_session_created(&self) {
        self.inner
            .debug_sessions_created
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_webhook_delivery(&self, result: WebhookDeliveryResult) {
        let counter = match result {
            WebhookDeliveryResult::Success => &self.inner.webhook_success,
            WebhookDeliveryResult::Failed => &self.inner.webhook_failed,
            WebhookDeliveryResult::Dropped => &self.inner.webhook_dropped,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Render the registry in Prometheus' text exposition format.
    pub fn render(&self) -> String {
        let mut output = String::new();
        output.push_str("# HELP raa_http_requests_total HTTP requests completed.\n");
        output.push_str("# TYPE raa_http_requests_total counter\n");
        output
            .push_str("# HELP raa_http_request_duration_seconds HTTP request latency by route.\n");
        output.push_str("# TYPE raa_http_request_duration_seconds summary\n");

        let http = self
            .inner
            .http
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (key, value) in http.iter() {
            let method = escape_label(&key.method);
            let route = escape_label(&key.route);
            let labels = format!(
                "method=\"{method}\",route=\"{route}\",status=\"{}\"",
                key.status
            );
            let _ = writeln!(
                output,
                "raa_http_requests_total{{{labels}}} {}",
                value.count
            );
            let _ = writeln!(
                output,
                "raa_http_request_duration_seconds_sum{{{labels}}} {}",
                value.latency_seconds_sum
            );
            let _ = writeln!(
                output,
                "raa_http_request_duration_seconds_count{{{labels}}} {}",
                value.count
            );
        }
        drop(http);

        render_counter(
            &mut output,
            "raa_jobs_created_total",
            "New jobs committed.",
            self.inner.jobs_created.load(Ordering::Relaxed),
        );
        render_counter(
            &mut output,
            "raa_job_idempotency_replays_total",
            "Idempotent job-create replays.",
            self.inner.job_replays.load(Ordering::Relaxed),
        );
        render_gauge(
            &mut output,
            "raa_outbox_backlog",
            "Pending outbox messages.",
            self.inner.outbox_backlog.load(Ordering::Relaxed),
        );
        render_counter(
            &mut output,
            "raa_outbox_publish_failures_total",
            "Outbox publish failures.",
            self.inner.outbox_publish_failures.load(Ordering::Relaxed),
        );
        render_counter(
            &mut output,
            "raa_outbox_finalization_failures_total",
            "Outbox database finalization failures.",
            self.inner
                .outbox_finalization_failures
                .load(Ordering::Relaxed),
        );
        render_gauge(
            &mut output,
            "raa_sse_connections",
            "Open SSE connections.",
            self.inner.sse_connections.load(Ordering::Relaxed) as i64,
        );
        render_counter(
            &mut output,
            "raa_debug_sessions_created_total",
            "Debug sessions created.",
            self.inner.debug_sessions_created.load(Ordering::Relaxed),
        );
        output.push_str("# HELP raa_webhook_deliveries_total Webhook delivery outcomes.\n");
        output.push_str("# TYPE raa_webhook_deliveries_total counter\n");
        for (result, value) in [
            (
                "success",
                self.inner.webhook_success.load(Ordering::Relaxed),
            ),
            ("failed", self.inner.webhook_failed.load(Ordering::Relaxed)),
            (
                "dropped",
                self.inner.webhook_dropped.load(Ordering::Relaxed),
            ),
        ] {
            let _ = writeln!(
                output,
                "raa_webhook_deliveries_total{{result=\"{result}\"}} {value}"
            );
        }
        output
    }
}

/// Record edge spans, structured completion logs, and HTTP metrics. Mount this
/// after request-context middleware so the request ID is available.
pub async fn observe_http(
    State(metrics): State<ApiMetrics>,
    request: Request,
    next: Next,
) -> Response {
    let started_at = Instant::now();
    let method = request.method().clone();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(MatchedPath::as_str)
        .unwrap_or("unmatched")
        .to_owned();
    let request_id = request
        .extensions()
        .get::<RequestContext>()
        .map(|context| context.request_id.as_str())
        .unwrap_or("unknown")
        .to_owned();
    let parent_context = global::get_text_map_propagator(|propagator| {
        propagator.extract(&HttpHeaderExtractor(request.headers()))
    });
    let span = tracing::info_span!(
        "http.request",
        request_id = %request_id,
        job_id = tracing::field::Empty,
        http.request.method = %method,
        http.route = %route,
        http.response.status_code = tracing::field::Empty,
        duration_ms = tracing::field::Empty,
    );
    span.set_parent(parent_context);

    async move {
        let response = next.run(request).await;
        let latency = started_at.elapsed();
        let status = response.status();
        tracing::Span::current().record("http.response.status_code", status.as_u16());
        tracing::Span::current().record("duration_ms", latency.as_secs_f64() * 1_000.0);
        metrics.record_http(method.as_str(), route.as_str(), status, latency);
        tracing::info!("request completed");
        response
    }
    .instrument(span)
    .await
}

/// Inject the current request span for durable propagation through the outbox.
pub fn current_trace_headers() -> BTreeMap<String, String> {
    let mut headers = BTreeMap::new();
    let context = tracing::Span::current().context();
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&context, &mut TraceHeaderInjector(&mut headers));
    });
    headers
}

struct HttpHeaderExtractor<'a>(&'a HeaderMap);

impl Extractor for HttpHeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(axum::http::HeaderName::as_str).collect()
    }
}

struct TraceHeaderInjector<'a>(&'a mut BTreeMap<String, String>);

impl Injector for TraceHeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_owned(), value);
    }
}

/// Attach job correlation to the current request span from a path handler.
pub fn record_job_id(job_id: impl std::fmt::Display) {
    tracing::Span::current().record("job_id", job_id.to_string());
}

pub fn metrics_router(metrics: ApiMetrics) -> Router {
    Router::new()
        .route("/metrics", get(metrics_endpoint))
        .with_state(metrics)
}

async fn metrics_endpoint(State(metrics): State<ApiMetrics>) -> impl IntoResponse {
    let mut response = metrics.render().into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    response
}

fn saturating_decrement(value: &AtomicU64) {
    let mut current = value.load(Ordering::Relaxed);
    while current > 0 {
        match value.compare_exchange_weak(
            current,
            current - 1,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

fn render_counter(output: &mut String, name: &str, help: &str, value: u64) {
    let _ = writeln!(output, "# HELP {name} {help}");
    let _ = writeln!(output, "# TYPE {name} counter");
    let _ = writeln!(output, "{name} {value}");
}

fn render_gauge(output: &mut String, name: &str, help: &str, value: i64) {
    let _ = writeln!(output, "# HELP {name} {help}");
    let _ = writeln!(output, "# TYPE {name} gauge");
    let _ = writeln!(output, "{name} {value}");
}

fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prometheus_output_contains_required_metrics() {
        let metrics = ApiMetrics::default();
        metrics.record_http(
            "GET",
            "/v1/jobs/{job_id}",
            StatusCode::OK,
            Duration::from_millis(25),
        );
        metrics.record_job_created(false);
        metrics.record_job_created(true);
        metrics.sse_connection_opened();
        metrics.record_webhook_delivery(WebhookDeliveryResult::Failed);

        let output = metrics.render();
        assert!(output.contains("raa_http_requests_total"));
        assert!(output.contains("raa_jobs_created_total 1"));
        assert!(output.contains("raa_job_idempotency_replays_total 1"));
        assert!(output.contains("raa_sse_connections 1"));
        assert!(output.contains("result=\"failed\"} 1"));
    }

    #[test]
    fn active_connection_gauge_does_not_underflow() {
        let metrics = ApiMetrics::default();
        metrics.sse_connection_closed();
        assert!(metrics.render().contains("raa_sse_connections 0"));
    }
}

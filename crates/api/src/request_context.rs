//! Per-request correlation context.

use axum::{
    extract::Request,
    http::{HeaderName, HeaderValue},
    middleware::Next,
    response::Response,
};
use run_anywhere_contracts::RequestId;
use uuid::Uuid;

pub static REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

tokio::task_local! {
    static CURRENT_REQUEST_ID: RequestId;
}

#[derive(Clone, Debug)]
pub struct RequestContext {
    pub request_id: RequestId,
}

impl RequestContext {
    pub fn generate() -> Self {
        Self {
            request_id: generate_request_id(),
        }
    }
}

/// Assign a server-generated request ID, expose it to handlers and errors, and
/// return it in the response. Client-provided IDs are intentionally ignored so
/// untrusted input cannot forge log correlation.
pub async fn request_context(mut request: Request, next: Next) -> Response {
    let context = RequestContext::generate();
    let request_id = context.request_id.clone();
    request.extensions_mut().insert(context);

    CURRENT_REQUEST_ID
        .scope(request_id.clone(), async move {
            let mut response = next.run(request).await;
            response.headers_mut().insert(
                REQUEST_ID_HEADER.clone(),
                HeaderValue::from_str(request_id.as_str())
                    .expect("generated request IDs contain only HTTP header characters"),
            );
            response
        })
        .await
}

/// Return the request ID in the current middleware task. A fresh ID is used
/// for errors constructed outside a request (for example, startup probes).
pub fn current_request_id() -> RequestId {
    CURRENT_REQUEST_ID
        .try_with(Clone::clone)
        .unwrap_or_else(|_| generate_request_id())
}

fn generate_request_id() -> RequestId {
    RequestId::new(format!("req_{}", Uuid::new_v4().simple()))
        .expect("a UUID always satisfies the request ID contract")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_request_ids_satisfy_the_contract() {
        let context = RequestContext::generate();
        assert!(context.request_id.as_str().starts_with("req_"));
    }
}

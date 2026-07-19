//! Minimal bounded webhook delivery with registration-time SSRF checks.

use std::{
    fmt,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use reqwest::redirect::Policy as RedirectPolicy;
use run_anywhere_contracts::{JobEvent, Uri};
use thiserror::Error;
use tokio::{net::lookup_host, sync::Semaphore};
use url::{Host, Url};

use crate::observability::{ApiMetrics, WebhookDeliveryResult};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WebhookPolicy {
    pub allow_private_networks: bool,
}

#[derive(Clone, Debug)]
pub struct WebhookDelivery {
    pub delivery_id: String,
    pub url: Uri,
    pub event: JobEvent,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum WebhookValidationError {
    #[error("webhook URL must be a valid absolute URL")]
    InvalidUrl,
    #[error("webhook URL must use http or https")]
    UnsupportedScheme,
    #[error("webhook URL must not contain embedded credentials or a fragment")]
    UnsafeUrlComponents,
    #[error("webhook hostname could not be resolved: {0}")]
    Resolve(String),
    #[error("webhook hostname did not resolve to an address")]
    NoAddresses,
    #[error("webhook targets private, local, reserved, or non-routable networking")]
    PrivateNetwork,
    #[error("webhook validation or delivery timed out")]
    Timeout,
    #[error("webhook delivery failed: {0}")]
    Delivery(String),
}

#[derive(Debug, Error)]
pub enum WebhookDispatcherError {
    #[error("webhook concurrency and timeout must be positive")]
    InvalidConfig,
}

#[derive(Clone)]
pub struct WebhookDispatcher {
    permits: Arc<Semaphore>,
    policy: WebhookPolicy,
    timeout: Duration,
    metrics: ApiMetrics,
}

impl fmt::Debug for WebhookDispatcher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WebhookDispatcher")
            .field("available_permits", &self.permits.available_permits())
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl WebhookDispatcher {
    pub fn new(
        concurrency: usize,
        timeout: Duration,
        policy: WebhookPolicy,
        metrics: ApiMetrics,
    ) -> Result<Self, WebhookDispatcherError> {
        if concurrency == 0 || timeout.is_zero() {
            return Err(WebhookDispatcherError::InvalidConfig);
        }
        Ok(Self {
            permits: Arc::new(Semaphore::new(concurrency)),
            policy,
            timeout,
            metrics,
        })
    }

    pub async fn validate_url(&self, url: &Uri) -> Result<(), WebhookValidationError> {
        tokio::time::timeout(
            self.timeout,
            validate_webhook_url(url.as_str(), self.policy),
        )
        .await
        .map_err(|_| WebhookValidationError::Timeout)?
    }

    pub async fn deliver(&self, delivery: &WebhookDelivery) -> Result<(), WebhookValidationError> {
        let _permit = self
            .permits
            .acquire()
            .await
            .map_err(|_| WebhookValidationError::Delivery("dispatcher is closed".to_owned()))?;
        let result = deliver_webhook(delivery, self.policy, self.timeout).await;
        match &result {
            Ok(()) => {
                self.metrics
                    .record_webhook_delivery(WebhookDeliveryResult::Success);
                tracing::debug!(delivery_id = %delivery.delivery_id, event_id = %delivery.event.id, "webhook delivered");
            }
            Err(error) => {
                self.metrics
                    .record_webhook_delivery(WebhookDeliveryResult::Failed);
                tracing::warn!(delivery_id = %delivery.delivery_id, event_id = %delivery.event.id, error = %error, "webhook delivery failed");
            }
        }
        result
    }
}

async fn deliver_webhook(
    delivery: &WebhookDelivery,
    policy: WebhookPolicy,
    timeout: Duration,
) -> Result<(), WebhookValidationError> {
    tokio::time::timeout(timeout, async {
        // Re-resolve immediately before delivery, validate every answer, then
        // pin reqwest to those exact sockets. This closes the DNS-rebinding
        // window while retaining the original hostname for Host and TLS SNI.
        let target = resolve_webhook_target(delivery.url.as_str(), policy).await?;
        let mut client = reqwest::Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout)
            .redirect(RedirectPolicy::none())
            .no_proxy()
            .user_agent("run-anywhere-control-plane/0.1");
        if let Some(domain) = target.domain.as_deref() {
            client = client.resolve_to_addrs(domain, &target.addresses);
        }
        let client = client
            .build()
            .map_err(|error| WebhookValidationError::Delivery(error.to_string()))?;
        let response = client
            .post(target.url)
            .header("x-run-anywhere-event", "job_state_changed")
            .header("x-run-anywhere-delivery", delivery.delivery_id.as_str())
            .json(&delivery.event)
            .send()
            .await
            .map_err(|error| WebhookValidationError::Delivery(error.to_string()))?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(WebhookValidationError::Delivery(format!(
                "endpoint returned HTTP {}",
                response.status()
            )))
        }
    })
    .await
    .map_err(|_| WebhookValidationError::Timeout)?
}

#[derive(Debug)]
struct ResolvedWebhookTarget {
    url: Url,
    domain: Option<String>,
    addresses: Vec<SocketAddr>,
}

pub async fn validate_webhook_url(
    value: &str,
    policy: WebhookPolicy,
) -> Result<(), WebhookValidationError> {
    resolve_webhook_target(value, policy).await.map(|_| ())
}

async fn resolve_webhook_target(
    value: &str,
    policy: WebhookPolicy,
) -> Result<ResolvedWebhookTarget, WebhookValidationError> {
    let parsed = Url::parse(value).map_err(|_| WebhookValidationError::InvalidUrl)?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(WebhookValidationError::UnsupportedScheme);
    }
    if !parsed.username().is_empty() || parsed.password().is_some() || parsed.fragment().is_some() {
        return Err(WebhookValidationError::UnsafeUrlComponents);
    }
    let host = parsed.host().ok_or(WebhookValidationError::InvalidUrl)?;
    let port = parsed
        .port_or_known_default()
        .ok_or(WebhookValidationError::InvalidUrl)?;
    let (domain, mut addresses) = match host {
        Host::Ipv4(address) => (None, vec![SocketAddr::new(IpAddr::V4(address), port)]),
        Host::Ipv6(address) => (None, vec![SocketAddr::new(IpAddr::V6(address), port)]),
        Host::Domain(domain) => (
            Some(domain.to_owned()),
            lookup_host((domain, port))
                .await
                .map_err(|error| WebhookValidationError::Resolve(error.to_string()))?
                .collect(),
        ),
    };
    if addresses.is_empty() {
        return Err(WebhookValidationError::NoAddresses);
    }
    addresses.sort_unstable();
    addresses.dedup();
    if addresses
        .iter()
        .any(|address| is_forbidden_target(address.ip(), policy))
    {
        return Err(WebhookValidationError::PrivateNetwork);
    }
    Ok(ResolvedWebhookTarget {
        url: parsed,
        domain,
        addresses,
    })
}

fn is_forbidden_target(address: IpAddr, policy: WebhookPolicy) -> bool {
    is_always_forbidden_ip(address) || (!policy.allow_private_networks && is_private_ip(address))
}

fn is_private_ip(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => {
            let [a, b, ..] = address.octets();
            a == 10 || (a == 172 && (16..=31).contains(&b)) || (a == 192 && b == 168)
        }
        IpAddr::V6(address) => {
            if let Some(mapped) = address.to_ipv4() {
                return is_private_ip(IpAddr::V4(mapped));
            }
            let segments = address.segments();
            (segments[0] & 0xfe00) == 0xfc00
        }
    }
}

fn is_always_forbidden_ip(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => {
            let [a, b, c, d] = address.octets();
            a == 0
                || a == 127
                || (a == 100 && (64..=127).contains(&b))
                || (a == 169 && b == 254)
                || (a == 192 && b == 0 && c == 0)
                || (a == 192 && b == 0 && c == 2)
                || (a == 198 && (b == 18 || b == 19))
                || (a == 198 && b == 51 && c == 100)
                || (a == 203 && b == 0 && c == 113)
                || a >= 224
                || (a == 255 && b == 255 && c == 255 && d == 255)
        }
        IpAddr::V6(address) => {
            if let Some(mapped) = address.to_ipv4() {
                return is_always_forbidden_ip(IpAddr::V4(mapped));
            }
            let segments = address.segments();
            address.is_unspecified()
                || address.is_loopback()
                || (segments[0] == 0x0064
                    && segments[1] == 0xff9b
                    && ((segments[2..6].iter().all(|part| *part == 0)) || segments[2] == 0x0001))
                || (segments[0] & 0xffc0) == 0xfe80
                || (segments[0] & 0xffc0) == 0xfec0
                || (segments[0] & 0xff00) == 0xff00
                || (segments[0] == 0x2001 && segments[1] == 0x0db8)
                || (segments[0] == 0x0100 && segments[1..].iter().all(|part| *part == 0))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr};

    use super::*;

    #[test]
    fn private_and_reserved_addresses_are_rejected() {
        let public_only = WebhookPolicy {
            allow_private_networks: false,
        };
        for address in [
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3)),
            IpAddr::V4(Ipv4Addr::new(169, 254, 1, 1)),
            IpAddr::V4(Ipv4Addr::new(203, 0, 113, 1)),
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            "fd00::1".parse().unwrap(),
            "2001:db8::1".parse().unwrap(),
        ] {
            assert!(is_forbidden_target(address, public_only), "{address}");
        }
        assert!(!is_forbidden_target(
            "1.1.1.1".parse().unwrap(),
            public_only
        ));
        assert!(!is_forbidden_target(
            "2606:4700:4700::1111".parse().unwrap(),
            public_only
        ));
    }

    #[test]
    fn private_network_mode_never_allows_local_or_metadata_targets() {
        let private_allowed = WebhookPolicy {
            allow_private_networks: true,
        };
        assert!(!is_forbidden_target(
            "10.0.0.8".parse().unwrap(),
            private_allowed
        ));
        for address in [
            "127.0.0.1",
            "169.254.169.254",
            "::1",
            "fe80::1",
            "64:ff9b::a9fe:a9fe",
            "64:ff9b:1::a9fe:a9fe",
        ] {
            assert!(is_forbidden_target(
                address.parse().unwrap(),
                private_allowed
            ));
        }
    }

    #[tokio::test]
    async fn url_components_and_scheme_are_constrained_before_dns() {
        let policy = WebhookPolicy {
            allow_private_networks: true,
        };
        assert_eq!(
            validate_webhook_url("file:///tmp/hook", policy).await,
            Err(WebhookValidationError::UnsupportedScheme)
        );
        assert_eq!(
            validate_webhook_url("https://user:pass@example.test/hook", policy).await,
            Err(WebhookValidationError::UnsafeUrlComponents)
        );
    }
}

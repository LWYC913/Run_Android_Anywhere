//! Environment-driven scheduler configuration with secure production defaults.

use std::{
    collections::HashMap,
    env, fmt,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    time::Duration,
};

use thiserror::Error;
use url::{Host, Url};

const DEFAULT_DATABASE_URL: &str = "postgres://postgres:postgres@127.0.0.1:5432/run_anywhere_dev";
const DEFAULT_NATS_URL: &str = "nats://127.0.0.1:4222";

/// A value whose contents must not appear in diagnostics.
#[derive(Clone, PartialEq, Eq)]
pub struct SecretString(String);

impl SecretString {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretString([redacted])")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NatsConfig {
    pub url: SecretString,
    /// A NATS `.creds` file containing the user JWT and NKey seed.
    pub credentials_file: Option<PathBuf>,
    /// A raw NKey seed file for operator-managed deployments.
    pub nkey_seed_file: Option<PathBuf>,
    pub tls_ca_file: Option<PathBuf>,
    pub tls_client_cert_file: Option<PathBuf>,
    pub tls_client_key_file: Option<PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopologySettings {
    pub job_stream_max_bytes: i64,
    pub dlq_stream_max_bytes: i64,
    pub advisory_stream_max_bytes: i64,
    pub stream_replicas: usize,
    pub dlq_retention: Duration,
    pub advisory_retention: Duration,
    pub pull_batch_size: usize,
    pub max_ack_pending: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimingSettings {
    pub worker_heartbeat: Duration,
    pub stale_worker_threshold: Duration,
    pub ack_wait: Duration,
    pub database_lease_ttl: Duration,
    pub reconciliation_interval: Duration,
    pub reap_grace: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FairnessSettings {
    pub buffer_size: usize,
    pub interactive_weight: usize,
    pub batch_weight: usize,
    pub batch_aging: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DockerSettings {
    pub endpoint: String,
    pub request_timeout: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    pub database_url: SecretString,
    pub run_migrations: bool,
    pub nats: NatsConfig,
    pub topology: TopologySettings,
    pub timing: TimingSettings,
    pub fairness: FairnessSettings,
    pub max_run_attempts: u32,
    pub metrics_bind_addr: SocketAddr,
    pub docker: DockerSettings,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("environment variable `{name}` is invalid: {message}")]
    Invalid { name: &'static str, message: String },
    #[error(
        "scheduler timing must satisfy heartbeat < stale-worker threshold < ack wait < database lease TTL"
    )]
    InvalidTimingOrder,
    #[error("remote NATS requires a `tls://` or `wss://` endpoint")]
    RemoteNatsRequiresTls,
    #[error(
        "remote NATS requires NKey/JWT credentials via `NATS_CREDENTIALS_FILE` or `NATS_NKEY_SEED_FILE`"
    )]
    RemoteNatsRequiresCredentials,
    #[error("`NATS_CREDENTIALS_FILE` and `NATS_NKEY_SEED_FILE` are mutually exclusive")]
    ConflictingNatsCredentials,
    #[error(
        "`NATS_TLS_CLIENT_CERT_FILE` and `NATS_TLS_CLIENT_KEY_FILE` must be configured together"
    )]
    IncompleteNatsClientCertificate,
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_map(env::vars().collect())
    }

    /// Load from an explicit map so tests never mutate process-global state.
    pub fn from_map(values: HashMap<String, String>) -> Result<Self, ConfigError> {
        let get = |name: &'static str| {
            values
                .get(name)
                .map(String::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
        };

        let metrics_bind_addr = parse_or(
            get("SCHEDULER_METRICS_BIND_ADDR"),
            "SCHEDULER_METRICS_BIND_ADDR",
            "127.0.0.1:9091",
        )?;
        if !socket_is_loopback(metrics_bind_addr) {
            return Err(ConfigError::Invalid {
                name: "SCHEDULER_METRICS_BIND_ADDR",
                message: "must bind to a loopback address".to_owned(),
            });
        }

        let nats_url = get("NATS_URL").unwrap_or(DEFAULT_NATS_URL);
        let parsed_nats = Url::parse(nats_url).map_err(|error| ConfigError::Invalid {
            name: "NATS_URL",
            message: error.to_string(),
        })?;
        if !matches!(parsed_nats.scheme(), "nats" | "tls" | "ws" | "wss")
            || parsed_nats.host().is_none()
        {
            return Err(ConfigError::Invalid {
                name: "NATS_URL",
                message: "must be an absolute nats://, tls://, ws://, or wss:// URL".to_owned(),
            });
        }
        if !parsed_nats.username().is_empty() || parsed_nats.password().is_some() {
            return Err(ConfigError::Invalid {
                name: "NATS_URL",
                message: "must not embed credentials; use an NKey/JWT credential file".to_owned(),
            });
        }

        let credentials_file = get("NATS_CREDENTIALS_FILE").map(PathBuf::from);
        let nkey_seed_file = get("NATS_NKEY_SEED_FILE").map(PathBuf::from);
        if credentials_file.is_some() && nkey_seed_file.is_some() {
            return Err(ConfigError::ConflictingNatsCredentials);
        }
        let local_nats = nats_endpoint_is_loopback(&parsed_nats);
        if !local_nats && !matches!(parsed_nats.scheme(), "tls" | "wss") {
            return Err(ConfigError::RemoteNatsRequiresTls);
        }
        if !local_nats && credentials_file.is_none() && nkey_seed_file.is_none() {
            return Err(ConfigError::RemoteNatsRequiresCredentials);
        }

        let tls_client_cert_file = get("NATS_TLS_CLIENT_CERT_FILE").map(PathBuf::from);
        let tls_client_key_file = get("NATS_TLS_CLIENT_KEY_FILE").map(PathBuf::from);
        if tls_client_cert_file.is_some() != tls_client_key_file.is_some() {
            return Err(ConfigError::IncompleteNatsClientCertificate);
        }

        let timing = TimingSettings {
            worker_heartbeat: duration_seconds(
                get("SCHEDULER_WORKER_HEARTBEAT_SECONDS"),
                "SCHEDULER_WORKER_HEARTBEAT_SECONDS",
                15,
            )?,
            stale_worker_threshold: duration_seconds(
                get("SCHEDULER_STALE_WORKER_SECONDS"),
                "SCHEDULER_STALE_WORKER_SECONDS",
                45,
            )?,
            ack_wait: duration_seconds(
                get("SCHEDULER_ACK_WAIT_SECONDS"),
                "SCHEDULER_ACK_WAIT_SECONDS",
                60,
            )?,
            database_lease_ttl: duration_seconds(
                get("SCHEDULER_DATABASE_LEASE_SECONDS"),
                "SCHEDULER_DATABASE_LEASE_SECONDS",
                75,
            )?,
            reconciliation_interval: duration_seconds(
                get("SCHEDULER_RECONCILE_INTERVAL_SECONDS"),
                "SCHEDULER_RECONCILE_INTERVAL_SECONDS",
                10,
            )?,
            reap_grace: duration_seconds(
                get("SCHEDULER_REAP_GRACE_SECONDS"),
                "SCHEDULER_REAP_GRACE_SECONDS",
                120,
            )?,
        };
        if !timing_values_are_nonzero(&timing) {
            return Err(ConfigError::Invalid {
                name: "SCHEDULER_WORKER_HEARTBEAT_SECONDS",
                message: "all scheduler durations must be greater than zero".to_owned(),
            });
        }
        if !(timing.worker_heartbeat < timing.stale_worker_threshold
            && timing.stale_worker_threshold < timing.ack_wait
            && timing.ack_wait < timing.database_lease_ttl)
        {
            return Err(ConfigError::InvalidTimingOrder);
        }

        let pull_batch_size = parse_or(
            get("SCHEDULER_PULL_BATCH_SIZE"),
            "SCHEDULER_PULL_BATCH_SIZE",
            "64",
        )?;
        let max_ack_pending = parse_or(
            get("SCHEDULER_MAX_ACK_PENDING"),
            "SCHEDULER_MAX_ACK_PENDING",
            "256",
        )?;
        let buffer_size = parse_or(
            get("SCHEDULER_FAIRNESS_BUFFER_SIZE"),
            "SCHEDULER_FAIRNESS_BUFFER_SIZE",
            "1024",
        )?;
        if pull_batch_size == 0 {
            return invalid_positive("SCHEDULER_PULL_BATCH_SIZE");
        }
        if max_ack_pending <= 0 {
            return invalid_positive("SCHEDULER_MAX_ACK_PENDING");
        }
        if i64::try_from(pull_batch_size).map_or(true, |batch| batch > max_ack_pending) {
            return Err(ConfigError::Invalid {
                name: "SCHEDULER_PULL_BATCH_SIZE",
                message: "must fit within SCHEDULER_MAX_ACK_PENDING".to_owned(),
            });
        }
        if usize::try_from(max_ack_pending).map_or(true, |limit| buffer_size < limit) {
            return Err(ConfigError::Invalid {
                name: "SCHEDULER_FAIRNESS_BUFFER_SIZE",
                message:
                    "must be at least SCHEDULER_MAX_ACK_PENDING so pending work is never dropped"
                        .to_owned(),
            });
        }

        let topology = TopologySettings {
            job_stream_max_bytes: positive_i64(
                get("SCHEDULER_JOB_STREAM_MAX_BYTES"),
                "SCHEDULER_JOB_STREAM_MAX_BYTES",
                10 * 1024 * 1024 * 1024,
            )?,
            dlq_stream_max_bytes: positive_i64(
                get("SCHEDULER_DLQ_STREAM_MAX_BYTES"),
                "SCHEDULER_DLQ_STREAM_MAX_BYTES",
                2 * 1024 * 1024 * 1024,
            )?,
            advisory_stream_max_bytes: positive_i64(
                get("SCHEDULER_ADVISORY_STREAM_MAX_BYTES"),
                "SCHEDULER_ADVISORY_STREAM_MAX_BYTES",
                256 * 1024 * 1024,
            )?,
            stream_replicas: parse_or(
                get("SCHEDULER_STREAM_REPLICAS"),
                "SCHEDULER_STREAM_REPLICAS",
                "1",
            )?,
            dlq_retention: duration_seconds(
                get("SCHEDULER_DLQ_RETENTION_SECONDS"),
                "SCHEDULER_DLQ_RETENTION_SECONDS",
                30 * 24 * 60 * 60,
            )?,
            advisory_retention: duration_seconds(
                get("SCHEDULER_ADVISORY_RETENTION_SECONDS"),
                "SCHEDULER_ADVISORY_RETENTION_SECONDS",
                30 * 24 * 60 * 60,
            )?,
            pull_batch_size,
            max_ack_pending,
        };
        if !(1..=5).contains(&topology.stream_replicas) {
            return Err(ConfigError::Invalid {
                name: "SCHEDULER_STREAM_REPLICAS",
                message: "must be between 1 and 5".to_owned(),
            });
        }
        if topology.dlq_retention.is_zero() || topology.advisory_retention.is_zero() {
            return Err(ConfigError::Invalid {
                name: "SCHEDULER_DLQ_RETENTION_SECONDS",
                message: "retention durations must be greater than zero".to_owned(),
            });
        }

        let fairness = FairnessSettings {
            buffer_size,
            interactive_weight: parse_or(
                get("SCHEDULER_INTERACTIVE_WEIGHT"),
                "SCHEDULER_INTERACTIVE_WEIGHT",
                "3",
            )?,
            batch_weight: parse_or(get("SCHEDULER_BATCH_WEIGHT"), "SCHEDULER_BATCH_WEIGHT", "1")?,
            batch_aging: duration_seconds(
                get("SCHEDULER_BATCH_AGING_SECONDS"),
                "SCHEDULER_BATCH_AGING_SECONDS",
                5 * 60,
            )?,
        };
        if fairness.interactive_weight == 0 {
            return invalid_positive("SCHEDULER_INTERACTIVE_WEIGHT");
        }
        if fairness.batch_weight == 0 {
            return invalid_positive("SCHEDULER_BATCH_WEIGHT");
        }
        if fairness
            .interactive_weight
            .checked_add(fairness.batch_weight)
            .is_none_or(|combined| combined > 1024)
        {
            return Err(ConfigError::Invalid {
                name: "SCHEDULER_INTERACTIVE_WEIGHT",
                message: "combined lane weights must not exceed 1024".to_owned(),
            });
        }
        if fairness.batch_aging.is_zero() {
            return invalid_positive("SCHEDULER_BATCH_AGING_SECONDS");
        }

        let max_run_attempts = parse_or(
            get("SCHEDULER_MAX_RUN_ATTEMPTS"),
            "SCHEDULER_MAX_RUN_ATTEMPTS",
            "3",
        )?;
        if max_run_attempts == 0 || max_run_attempts == u32::MAX {
            return Err(ConfigError::Invalid {
                name: "SCHEDULER_MAX_RUN_ATTEMPTS",
                message: "must be between 1 and 4294967294".to_owned(),
            });
        }

        let docker_endpoint = get("SCHEDULER_DOCKER_ENDPOINT")
            .or_else(|| get("DOCKER_HOST"))
            .unwrap_or(default_docker_endpoint())
            .to_owned();
        validate_docker_endpoint(&docker_endpoint)?;
        let docker_request_timeout = duration_seconds(
            get("SCHEDULER_DOCKER_TIMEOUT_SECONDS"),
            "SCHEDULER_DOCKER_TIMEOUT_SECONDS",
            10,
        )?;
        if docker_request_timeout.is_zero() {
            return invalid_positive("SCHEDULER_DOCKER_TIMEOUT_SECONDS");
        }

        Ok(Self {
            database_url: SecretString::new(get("DATABASE_URL").unwrap_or(DEFAULT_DATABASE_URL)),
            run_migrations: parse_bool(get("RUN_MIGRATIONS"), "RUN_MIGRATIONS", true)?,
            nats: NatsConfig {
                url: SecretString::new(nats_url),
                credentials_file,
                nkey_seed_file,
                tls_ca_file: get("NATS_TLS_CA_FILE").map(PathBuf::from),
                tls_client_cert_file,
                tls_client_key_file,
            },
            topology,
            timing,
            fairness,
            max_run_attempts,
            metrics_bind_addr,
            docker: DockerSettings {
                endpoint: docker_endpoint,
                request_timeout: docker_request_timeout,
            },
        })
    }
}

fn timing_values_are_nonzero(timing: &TimingSettings) -> bool {
    !timing.worker_heartbeat.is_zero()
        && !timing.stale_worker_threshold.is_zero()
        && !timing.ack_wait.is_zero()
        && !timing.database_lease_ttl.is_zero()
        && !timing.reconciliation_interval.is_zero()
        && !timing.reap_grace.is_zero()
}

fn socket_is_loopback(address: SocketAddr) -> bool {
    match address.ip() {
        IpAddr::V4(ip) => ip.is_loopback(),
        IpAddr::V6(ip) => ip.is_loopback(),
    }
}

fn nats_endpoint_is_loopback(url: &Url) -> bool {
    match url.host() {
        Some(Host::Domain(host)) => {
            host.eq_ignore_ascii_case("localhost")
                || host
                    .parse::<IpAddr>()
                    .is_ok_and(|address| address.is_loopback())
        }
        Some(Host::Ipv4(ip)) => ip.is_loopback(),
        Some(Host::Ipv6(ip)) => ip.is_loopback(),
        None => false,
    }
}

fn validate_docker_endpoint(value: &str) -> Result<(), ConfigError> {
    let parsed = Url::parse(value).map_err(|error| ConfigError::Invalid {
        name: "SCHEDULER_DOCKER_ENDPOINT",
        message: error.to_string(),
    })?;
    match parsed.scheme() {
        "unix" | "npipe" => Ok(()),
        "https" if parsed.host().is_some() => Ok(()),
        "http" if parsed.host().is_some() && nats_endpoint_is_loopback(&parsed) => Ok(()),
        "http" => Err(ConfigError::Invalid {
            name: "SCHEDULER_DOCKER_ENDPOINT",
            message: "plaintext Docker Engine TCP is allowed only on loopback".to_owned(),
        }),
        _ => Err(ConfigError::Invalid {
            name: "SCHEDULER_DOCKER_ENDPOINT",
            message: "must use unix://, npipe://, loopback http://, or https://".to_owned(),
        }),
    }
}

#[cfg(windows)]
fn default_docker_endpoint() -> &'static str {
    "npipe:////./pipe/docker_engine"
}

#[cfg(not(windows))]
fn default_docker_endpoint() -> &'static str {
    "unix:///var/run/docker.sock"
}

fn positive_i64(value: Option<&str>, name: &'static str, default: i64) -> Result<i64, ConfigError> {
    let parsed = parse_or(value, name, &default.to_string())?;
    if parsed <= 0 {
        return invalid_positive(name);
    }
    Ok(parsed)
}

fn duration_seconds(
    value: Option<&str>,
    name: &'static str,
    default: u64,
) -> Result<Duration, ConfigError> {
    let seconds = parse_or(value, name, &default.to_string())?;
    let duration = Duration::from_secs(seconds);
    chrono::Duration::from_std(duration).map_err(|_| ConfigError::Invalid {
        name,
        message: "is too large for scheduler date arithmetic".to_owned(),
    })?;
    Ok(duration)
}

fn parse_or<T>(value: Option<&str>, name: &'static str, default: &str) -> Result<T, ConfigError>
where
    T: std::str::FromStr,
    T::Err: fmt::Display,
{
    value
        .unwrap_or(default)
        .parse()
        .map_err(|error: T::Err| ConfigError::Invalid {
            name,
            message: error.to_string(),
        })
}

fn parse_bool(value: Option<&str>, name: &'static str, default: bool) -> Result<bool, ConfigError> {
    let Some(value) = value else {
        return Ok(default);
    };
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(ConfigError::Invalid {
            name,
            message: "expected true/false, yes/no, on/off, or 1/0".to_owned(),
        }),
    }
}

fn invalid_positive<T>(name: &'static str) -> Result<T, ConfigError> {
    Err(ConfigError::Invalid {
        name,
        message: "must be greater than zero".to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_encode_required_timing_and_security_policy() {
        let config = Config::from_map(HashMap::new()).unwrap();
        assert_eq!(config.timing.worker_heartbeat, Duration::from_secs(15));
        assert_eq!(
            config.timing.stale_worker_threshold,
            Duration::from_secs(45)
        );
        assert_eq!(config.timing.ack_wait, Duration::from_secs(60));
        assert_eq!(config.timing.database_lease_ttl, Duration::from_secs(75));
        assert_eq!(config.max_run_attempts, 3);
        assert!(config.run_migrations);
        assert_eq!(config.topology.stream_replicas, 1);
        assert_eq!(config.metrics_bind_addr.to_string(), "127.0.0.1:9091");
    }

    #[test]
    fn rejects_invalid_timing_order() {
        let values = HashMap::from([("SCHEDULER_ACK_WAIT_SECONDS".to_owned(), "40".to_owned())]);
        assert_eq!(
            Config::from_map(values),
            Err(ConfigError::InvalidTimingOrder)
        );
    }

    #[test]
    fn rejects_durations_that_cannot_be_used_for_date_arithmetic() {
        assert!(matches!(
            duration_seconds(
                Some(&u64::MAX.to_string()),
                "SCHEDULER_ACK_WAIT_SECONDS",
                60,
            ),
            Err(ConfigError::Invalid {
                name: "SCHEDULER_ACK_WAIT_SECONDS",
                ..
            })
        ));
    }

    #[test]
    fn remote_nats_requires_tls_then_credentials() {
        let plaintext = HashMap::from([(
            "NATS_URL".to_owned(),
            "nats://nats.example.test:4222".to_owned(),
        )]);
        assert_eq!(
            Config::from_map(plaintext),
            Err(ConfigError::RemoteNatsRequiresTls)
        );

        let tls = HashMap::from([(
            "NATS_URL".to_owned(),
            "tls://nats.example.test:4222".to_owned(),
        )]);
        assert_eq!(
            Config::from_map(tls),
            Err(ConfigError::RemoteNatsRequiresCredentials)
        );
    }

    #[test]
    fn remote_tls_with_creds_is_valid() {
        let values = HashMap::from([
            (
                "NATS_URL".to_owned(),
                "tls://nats.example.test:4222".to_owned(),
            ),
            (
                "NATS_CREDENTIALS_FILE".to_owned(),
                "/run/secrets/scheduler.creds".to_owned(),
            ),
        ]);
        assert!(Config::from_map(values).is_ok());
    }

    #[test]
    fn secrets_are_redacted() {
        assert_eq!(
            format!("{:?}", SecretString::new("super-secret")),
            "SecretString([redacted])"
        );
    }

    #[test]
    fn fairness_buffer_must_hold_every_ack_pending_delivery() {
        let values = HashMap::from([
            ("SCHEDULER_MAX_ACK_PENDING".to_owned(), "257".to_owned()),
            (
                "SCHEDULER_FAIRNESS_BUFFER_SIZE".to_owned(),
                "256".to_owned(),
            ),
        ]);
        assert!(matches!(
            Config::from_map(values),
            Err(ConfigError::Invalid {
                name: "SCHEDULER_FAIRNESS_BUFFER_SIZE",
                ..
            })
        ));
    }
}

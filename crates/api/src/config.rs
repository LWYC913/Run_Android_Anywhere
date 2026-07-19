//! Environment-driven control-plane configuration.
//!
//! Secret-bearing values deliberately do not implement `Display` and redact
//! their `Debug` output. This keeps otherwise useful startup diagnostics from
//! leaking credentials or signing material.

use std::{
    collections::HashMap, env, fmt, fs, net::SocketAddr, path::Path, str::FromStr, time::Duration,
};

use thiserror::Error;

const DEFAULT_DATABASE_URL: &str = "postgres://postgres:postgres@127.0.0.1:5432/run_anywhere_dev";
const DEFAULT_NATS_URL: &str = "nats://127.0.0.1:4222";
const DEFAULT_S3_ENDPOINT: &str = "http://127.0.0.1:9000";
const DEFAULT_DEBUG_GATEWAY_URL: &str = "http://127.0.0.1:8081";

/// A string whose value must never appear in logs or diagnostics.
#[derive(Clone, PartialEq, Eq)]
pub struct SecretString(String);

impl SecretString {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn expose_secret(&self) -> &str {
        &self.0
    }

    pub fn into_secret(self) -> String {
        self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretString([redacted])")
    }
}

/// S3-compatible object-store settings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct S3Config {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key: SecretString,
    pub secret_key: SecretString,
    pub force_path_style: bool,
}

/// Complete control-plane process configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    /// Public API listener. It defaults to loopback; containers must opt in to
    /// a wider bind address.
    pub api_bind_addr: SocketAddr,
    /// Prometheus listener. It intentionally defaults to loopback and is not
    /// mounted into the public `/v1` router.
    pub metrics_bind_addr: SocketAddr,
    pub database_url: SecretString,
    pub bootstrap_admin_token: Option<SecretString>,
    pub nats_url: SecretString,
    pub s3: S3Config,
    /// Ed25519 private key encoded as PKCS#8 PEM.
    pub jwt_signing_key: SecretString,
    pub jwt_kid: String,
    pub debug_gateway_base_url: String,
    pub debug_token_ttl: Duration,
    pub request_timeout: Duration,
    pub max_json_body_bytes: usize,
    pub otel_endpoint: Option<SecretString>,
    pub webhook_allow_private_networks: bool,
    pub run_migrations: bool,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("required environment variable `{0}` is not set")]
    Missing(&'static str),
    #[error("environment variable `{name}` is invalid: {message}")]
    Invalid { name: &'static str, message: String },
    #[error("could not read secret file configured by `{name}`: {source}")]
    ReadSecretFile {
        name: &'static str,
        #[source]
        source: std::io::Error,
    },
}

impl Config {
    /// Load and validate configuration from the process environment.
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_map(env::vars().collect())
    }

    /// Load configuration from an explicit map. This is public to allow tests
    /// and embedders to avoid mutating process-global environment variables.
    pub fn from_map(values: HashMap<String, String>) -> Result<Self, ConfigError> {
        let get = |name: &'static str| {
            values
                .get(name)
                .map(String::as_str)
                .filter(|value| !value.trim().is_empty())
        };

        let api_bind_addr = parse_or(get("API_BIND_ADDR"), "API_BIND_ADDR", "127.0.0.1:8080")?;
        let metrics_bind_addr = parse_or(
            get("METRICS_BIND_ADDR"),
            "METRICS_BIND_ADDR",
            "127.0.0.1:9090",
        )?;

        let jwt_signing_key = match (
            get("JWT_SIGNING_KEY_PEM").or_else(|| get("JWT_SIGNING_KEY")),
            get("JWT_SIGNING_KEY_FILE"),
        ) {
            (Some(value), _) => SecretString::new(expand_escaped_newlines(value)),
            (None, Some(path)) => {
                SecretString::new(read_secret_file("JWT_SIGNING_KEY_FILE", Path::new(path))?)
            }
            (None, None) => return Err(ConfigError::Missing("JWT_SIGNING_KEY_PEM")),
        };

        let debug_token_ttl = duration_seconds(
            get("DEBUG_TOKEN_TTL_SECONDS"),
            "DEBUG_TOKEN_TTL_SECONDS",
            15 * 60,
        )?;
        if debug_token_ttl.is_zero() || debug_token_ttl > Duration::from_secs(15 * 60) {
            return Err(ConfigError::Invalid {
                name: "DEBUG_TOKEN_TTL_SECONDS",
                message: "must be between 1 and 900 seconds".to_owned(),
            });
        }

        let request_timeout = duration_seconds(
            get("REQUEST_TIMEOUT_SECONDS"),
            "REQUEST_TIMEOUT_SECONDS",
            30,
        )?;
        if request_timeout.is_zero() {
            return Err(ConfigError::Invalid {
                name: "REQUEST_TIMEOUT_SECONDS",
                message: "must be greater than zero".to_owned(),
            });
        }

        let max_json_body_bytes =
            parse_or(get("MAX_JSON_BODY_BYTES"), "MAX_JSON_BODY_BYTES", "1048576")?;
        if max_json_body_bytes == 0 {
            return Err(ConfigError::Invalid {
                name: "MAX_JSON_BODY_BYTES",
                message: "must be greater than zero".to_owned(),
            });
        }

        let s3_endpoint = get("S3_ENDPOINT")
            .unwrap_or(DEFAULT_S3_ENDPOINT)
            .trim_end_matches('/')
            .to_owned();
        require_http_url("S3_ENDPOINT", &s3_endpoint)?;

        let debug_gateway_base_url = get("DEBUG_GATEWAY_BASE_URL")
            .unwrap_or(DEFAULT_DEBUG_GATEWAY_URL)
            .trim_end_matches('/')
            .to_owned();
        require_http_url("DEBUG_GATEWAY_BASE_URL", &debug_gateway_base_url)?;

        Ok(Self {
            api_bind_addr,
            metrics_bind_addr,
            database_url: SecretString::new(get("DATABASE_URL").unwrap_or(DEFAULT_DATABASE_URL)),
            bootstrap_admin_token: get("BOOTSTRAP_ADMIN_TOKEN").map(SecretString::new),
            nats_url: SecretString::new(get("NATS_URL").unwrap_or(DEFAULT_NATS_URL)),
            s3: S3Config {
                endpoint: s3_endpoint,
                region: get("S3_REGION")
                    .or_else(|| get("AWS_REGION"))
                    .unwrap_or("us-east-1")
                    .to_owned(),
                bucket: get("S3_BUCKET").unwrap_or("run-anywhere").to_owned(),
                access_key: SecretString::new(
                    get("S3_ACCESS_KEY_ID")
                        .or_else(|| get("AWS_ACCESS_KEY_ID"))
                        .unwrap_or("minioadmin"),
                ),
                secret_key: SecretString::new(
                    get("S3_SECRET_ACCESS_KEY")
                        .or_else(|| get("AWS_SECRET_ACCESS_KEY"))
                        .unwrap_or("minioadmin"),
                ),
                force_path_style: parse_bool(
                    get("S3_FORCE_PATH_STYLE"),
                    "S3_FORCE_PATH_STYLE",
                    true,
                )?,
            },
            jwt_signing_key,
            jwt_kid: get("JWT_KID").unwrap_or("raa-debug-v1").to_owned(),
            debug_gateway_base_url,
            debug_token_ttl,
            request_timeout,
            max_json_body_bytes,
            otel_endpoint: get("OTEL_EXPORTER_OTLP_ENDPOINT").map(SecretString::new),
            webhook_allow_private_networks: parse_bool(
                get("WEBHOOK_ALLOW_PRIVATE_NETWORKS"),
                "WEBHOOK_ALLOW_PRIVATE_NETWORKS",
                false,
            )?,
            run_migrations: parse_bool(get("RUN_MIGRATIONS"), "RUN_MIGRATIONS", true)?,
        })
    }
}

fn parse_or<T>(value: Option<&str>, name: &'static str, default: &str) -> Result<T, ConfigError>
where
    T: FromStr,
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

fn duration_seconds(
    value: Option<&str>,
    name: &'static str,
    default: u64,
) -> Result<Duration, ConfigError> {
    let seconds = match value {
        Some(value) => value.parse::<u64>().map_err(|error| ConfigError::Invalid {
            name,
            message: error.to_string(),
        })?,
        None => default,
    };
    Ok(Duration::from_secs(seconds))
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

fn require_http_url(name: &'static str, value: &str) -> Result<(), ConfigError> {
    let parsed = url::Url::parse(value).map_err(|error| ConfigError::Invalid {
        name,
        message: error.to_string(),
    })?;
    if !matches!(parsed.scheme(), "http" | "https") || parsed.host().is_none() {
        return Err(ConfigError::Invalid {
            name,
            message: "must be an absolute HTTP(S) URL".to_owned(),
        });
    }
    Ok(())
}

fn read_secret_file(name: &'static str, path: &Path) -> Result<String, ConfigError> {
    fs::read_to_string(path)
        .map(|value| value.trim().to_owned())
        .map_err(|source| ConfigError::ReadSecretFile { name, source })
}

fn expand_escaped_newlines(value: &str) -> String {
    value.replace("\\n", "\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn required_values() -> HashMap<String, String> {
        HashMap::from([(
            "JWT_SIGNING_KEY_PEM".to_owned(),
            "-----BEGIN PRIVATE KEY-----\\ntest\\n-----END PRIVATE KEY-----".to_owned(),
        )])
    }

    #[test]
    fn defaults_are_loopback_and_security_conservative() {
        let config = Config::from_map(required_values()).unwrap();

        assert!(config.api_bind_addr.ip().is_loopback());
        assert!(config.metrics_bind_addr.ip().is_loopback());
        assert!(!config.webhook_allow_private_networks);
        assert_eq!(config.debug_token_ttl, Duration::from_secs(900));
        assert!(config.jwt_signing_key.expose_secret().contains('\n'));
    }

    #[test]
    fn debug_never_exposes_secrets() {
        let mut values = required_values();
        values.insert("DATABASE_URL".to_owned(), "database-secret".to_owned());
        values.insert(
            "BOOTSTRAP_ADMIN_TOKEN".to_owned(),
            "admin-secret".to_owned(),
        );
        let rendered = format!("{:?}", Config::from_map(values).unwrap());

        assert!(!rendered.contains("database-secret"));
        assert!(!rendered.contains("admin-secret"));
        assert!(!rendered.contains("BEGIN PRIVATE KEY"));
    }

    #[test]
    fn token_ttl_cannot_exceed_part_three_limit() {
        let mut values = required_values();
        values.insert("DEBUG_TOKEN_TTL_SECONDS".to_owned(), "901".to_owned());

        assert!(matches!(
            Config::from_map(values),
            Err(ConfigError::Invalid {
                name: "DEBUG_TOKEN_TTL_SECONDS",
                ..
            })
        ));
    }
}

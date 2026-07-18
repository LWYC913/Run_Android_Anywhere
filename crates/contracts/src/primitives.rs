use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use thiserror::Error;
use utoipa::ToSchema;

/// Error returned when a validated wire primitive is malformed.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error("{message}")]
pub struct PrimitiveValidationError {
    message: &'static str,
}

impl PrimitiveValidationError {
    const fn new(message: &'static str) -> Self {
        Self { message }
    }
}

fn validate_identifier(value: &str, prefix: &'static str) -> Result<(), PrimitiveValidationError> {
    let Some(suffix) = value.strip_prefix(prefix) else {
        return Err(PrimitiveValidationError::new(
            "identifier has an unexpected prefix",
        ));
    };

    if suffix.is_empty()
        || !suffix
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    {
        return Err(PrimitiveValidationError::new(
            "identifier suffix must contain only ASCII letters, digits, underscores, or hyphens",
        ));
    }

    Ok(())
}

macro_rules! identifier {
    ($name:ident, $prefix:literal, $pattern:literal) => {
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, ToSchema)]
        #[schema(value_type = String, pattern = $pattern)]
        pub struct $name(String);

        impl $name {
            pub const PREFIX: &'static str = $prefix;

            pub fn new(value: impl Into<String>) -> Result<Self, PrimitiveValidationError> {
                let value = value.into();
                validate_identifier(&value, Self::PREFIX)?;
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn into_inner(self) -> String {
                self.0
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = PrimitiveValidationError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::new(value).map_err(de::Error::custom)
            }
        }
    };
}

identifier!(ProjectId, "proj_", "^proj_[A-Za-z0-9_-]+$");
identifier!(UploadId, "upl_", "^upl_[A-Za-z0-9_-]+$");
identifier!(RuntimeProfileId, "rtp_", "^rtp_[A-Za-z0-9_-]+$");
identifier!(JobId, "job_", "^job_[A-Za-z0-9_-]+$");
identifier!(JobEventId, "evt_", "^evt_[A-Za-z0-9_-]+$");
identifier!(ArtifactId, "art_", "^art_[A-Za-z0-9_-]+$");
identifier!(WorkerId, "wrk_", "^wrk_[A-Za-z0-9_-]+$");
identifier!(DebugSessionId, "dbg_", "^dbg_[A-Za-z0-9_-]+$");
identifier!(WebhookId, "wh_", "^wh_[A-Za-z0-9_-]+$");
identifier!(LeaseId, "lease_", "^lease_[A-Za-z0-9_-]+$");
identifier!(RequestId, "req_", "^req_[A-Za-z0-9_-]+$");

/// A canonical, lowercase hexadecimal SHA-256 digest.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, ToSchema)]
#[schema(value_type = String, pattern = "^[0-9a-f]{64}$")]
pub struct Sha256(String);

impl Sha256 {
    pub fn new(value: impl Into<String>) -> Result<Self, PrimitiveValidationError> {
        let value = value.into();
        if value.len() != 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(PrimitiveValidationError::new(
                "SHA-256 must be 64 lowercase hexadecimal characters",
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl fmt::Display for Sha256 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for Sha256 {
    type Err = PrimitiveValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl Serialize for Sha256 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Sha256 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

macro_rules! validated_string {
    ($name:ident, $validator:ident, $message:literal $(, $format:literal)?) => {
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, ToSchema)]
        #[schema(value_type = String $(, format = $format)?)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, PrimitiveValidationError> {
                let value = value.into();
                if !$validator(&value) {
                    return Err(PrimitiveValidationError::new($message));
                }
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn into_inner(self) -> String {
                self.0
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = PrimitiveValidationError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                Self::new(String::deserialize(deserializer)?).map_err(de::Error::custom)
            }
        }
    };
}

fn valid_uri(value: &str) -> bool {
    let Some(colon) = value.find(':') else {
        return false;
    };
    colon > 0
        && value[..colon]
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'-' | b'.'))
        && !value
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
}

fn valid_reference(value: &str) -> bool {
    !value.is_empty() && !value.bytes().any(|byte| byte.is_ascii_control())
}

validated_string!(
    Uri,
    valid_uri,
    "URI must have a scheme and contain no whitespace or control characters",
    "uri"
);
validated_string!(
    ScriptRef,
    valid_reference,
    "script reference must be non-empty and contain no control characters"
);

/// A duration represented as a positive number of whole seconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DurationSeconds(u64);

impl utoipa::PartialSchema for DurationSeconds {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        use utoipa::openapi::schema::{KnownFormat, ObjectBuilder, Schema, SchemaFormat, Type};

        utoipa::openapi::RefOr::T(Schema::Object(
            ObjectBuilder::new()
                .schema_type(Type::Integer)
                .format(Some(SchemaFormat::KnownFormat(KnownFormat::Int64)))
                .minimum(Some(1))
                .build(),
        ))
    }
}

impl ToSchema for DurationSeconds {}

impl DurationSeconds {
    pub fn new(value: u64) -> Result<Self, PrimitiveValidationError> {
        if value == 0 {
            return Err(PrimitiveValidationError::new(
                "duration must be at least one second",
            ));
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl TryFrom<u64> for DurationSeconds {
    type Error = PrimitiveValidationError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl Serialize for DurationSeconds {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.0)
    }
}

impl<'de> Deserialize<'de> for DurationSeconds {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(u64::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_validate_prefix_and_suffix() {
        assert_eq!(JobId::new("job_abc-123").unwrap().as_str(), "job_abc-123");
        assert!(JobId::new("evt_abc").is_err());
        assert!(JobId::new("job_").is_err());
        assert!(JobId::new("job_not valid").is_err());
    }

    #[test]
    fn identifier_deserialization_cannot_bypass_validation() {
        assert!(serde_json::from_str::<ProjectId>(r#""bad_123""#).is_err());
        assert_eq!(
            serde_json::from_str::<ProjectId>(r#""proj_123""#)
                .unwrap()
                .as_str(),
            "proj_123"
        );
    }

    #[test]
    fn sha256_is_canonical_lowercase_hex() {
        let valid = "a".repeat(64);
        assert_eq!(Sha256::new(valid.clone()).unwrap().as_str(), valid);
        assert!(Sha256::new("A".repeat(64)).is_err());
        assert!(Sha256::new("a".repeat(63)).is_err());
    }

    #[test]
    fn duration_must_be_positive_even_when_deserialized() {
        assert_eq!(DurationSeconds::new(1).unwrap().get(), 1);
        assert!(DurationSeconds::new(0).is_err());
        assert!(serde_json::from_str::<DurationSeconds>("0").is_err());
    }

    #[test]
    fn uri_and_script_references_are_validated() {
        assert!(Uri::new("https://example.test/path").is_ok());
        assert!(Uri::new("not a uri").is_err());
        assert!(ScriptRef::new("s3://bucket/test.zip").is_ok());
        assert!(ScriptRef::new("").is_err());
    }
}

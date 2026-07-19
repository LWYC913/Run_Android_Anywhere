use std::collections::BTreeMap;

use run_anywhere_contracts::{ArtifactSelection, AutomationSpec, FailureDetail};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::{RepositoryError, RepositoryResult};

pub(crate) fn encode_enum<T: Serialize>(value: T) -> RepositoryResult<String> {
    match serde_json::to_value(value).map_err(|error| RepositoryError::decode("enum", error))? {
        Value::String(value) => Ok(value),
        _ => Err(RepositoryError::decode(
            "enum",
            "wire enum did not serialize to a string",
        )),
    }
}

pub(crate) fn decode_enum<T: DeserializeOwned>(
    field: &'static str,
    value: String,
) -> RepositoryResult<T> {
    serde_json::from_value(Value::String(value))
        .map_err(|error| RepositoryError::decode(field, error))
}

pub(crate) fn encode_json<T: Serialize>(field: &'static str, value: T) -> RepositoryResult<Value> {
    serde_json::to_value(value).map_err(|error| RepositoryError::decode(field, error))
}

pub(crate) fn decode_json<T: DeserializeOwned>(
    field: &'static str,
    value: Value,
) -> RepositoryResult<T> {
    serde_json::from_value(value).map_err(|error| RepositoryError::decode(field, error))
}

pub(crate) fn payload_from_value(
    field: &'static str,
    value: Value,
) -> RepositoryResult<BTreeMap<String, Value>> {
    decode_json(field, value)
}

pub(crate) fn automation_from_value(value: Value) -> RepositoryResult<AutomationSpec> {
    decode_json("automation", value)
}

pub(crate) fn artifacts_from_value(value: Value) -> RepositoryResult<ArtifactSelection> {
    decode_json("requested_artifacts", value)
}

pub(crate) fn failure_from_value(value: Option<Value>) -> RepositoryResult<Option<FailureDetail>> {
    value.map(|value| decode_json("failure", value)).transpose()
}

pub(crate) fn to_u64(field: &'static str, value: i64) -> RepositoryResult<u64> {
    u64::try_from(value).map_err(|error| RepositoryError::decode(field, error))
}

pub(crate) fn to_u32(field: &'static str, value: i64) -> RepositoryResult<u32> {
    u32::try_from(value).map_err(|error| RepositoryError::decode(field, error))
}

pub(crate) fn to_u16(field: &'static str, value: i32) -> RepositoryResult<u16> {
    u16::try_from(value).map_err(|error| RepositoryError::decode(field, error))
}

pub(crate) fn checked_i64(field: &'static str, value: u64) -> RepositoryResult<i64> {
    i64::try_from(value)
        .map_err(|_| RepositoryError::Validation(format!("{field} exceeds PostgreSQL BIGINT")))
}

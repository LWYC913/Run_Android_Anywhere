use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;

use run_anywhere_contracts::*;
use serde_json::{Map, Value, json};
use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(components(schemas(
    ProjectId,
    UploadId,
    RuntimeProfileId,
    JobId,
    JobEventId,
    ArtifactId,
    WorkerId,
    DebugSessionId,
    WebhookId,
    LeaseId,
    RequestId,
    Sha256,
    Uri,
    ScriptRef,
    DurationSeconds,
    AuthScope,
    UploadKind,
    RuntimeKind,
    IsolationTier,
    HostArch,
    AndroidAbi,
    JobMode,
    JobState,
    JobOutcome,
    ArtifactKind,
    WorkerState,
    DebugSessionMode,
    WebhookEvent,
    ErrorCode,
    CreateProjectRequest,
    Project,
    CreateProjectResponse,
    CreateUploadRequest,
    CreateUploadResponse,
    RuntimeProfile,
    AutomationSpec,
    ArtifactSelection,
    CreateJobRequest,
    FailureDetail,
    Job,
    JobSummary,
    JobEvent,
    Artifact,
    WorkerStatus,
    DebugSessionRequest,
    DebugSessionToken,
    CreateWebhookRequest,
    Webhook,
    JobPage,
    ArtifactPage,
    WorkerPage,
    RuntimeProfilePage,
    ApiError,
    ErrorResponse,
    WorkerRegistration,
    WorkerHeartbeat,
    JobQueued,
    JobClaim,
    JobLeaseExtension,
    JobResult
)))]
struct RustContractSchemas;

const OBJECT_SCHEMAS: &[&str] = &[
    "CreateProjectRequest",
    "Project",
    "CreateProjectResponse",
    "CreateUploadRequest",
    "CreateUploadResponse",
    "RuntimeProfile",
    "ArtifactSelection",
    "CreateJobRequest",
    "FailureDetail",
    "Job",
    "JobSummary",
    "JobEvent",
    "Artifact",
    "WorkerStatus",
    "DebugSessionRequest",
    "DebugSessionToken",
    "CreateWebhookRequest",
    "Webhook",
    "JobPage",
    "ArtifactPage",
    "WorkerPage",
    "RuntimeProfilePage",
    "ApiError",
    "ErrorResponse",
    "WorkerRegistration",
    "WorkerHeartbeat",
    "JobQueued",
    "JobClaim",
    "JobLeaseExtension",
    "JobResult",
];

const ENUM_SCHEMAS: &[&str] = &[
    "AuthScope",
    "UploadKind",
    "RuntimeKind",
    "IsolationTier",
    "HostArch",
    "AndroidAbi",
    "JobMode",
    "JobState",
    "JobOutcome",
    "ArtifactKind",
    "WorkerState",
    "DebugSessionMode",
    "WebhookEvent",
    "ErrorCode",
];

const PRIMITIVE_SCHEMAS: &[&str] = &[
    "ProjectId",
    "UploadId",
    "RuntimeProfileId",
    "JobId",
    "JobEventId",
    "ArtifactId",
    "WorkerId",
    "DebugSessionId",
    "WebhookId",
    "LeaseId",
    "RequestId",
    "Sha256",
    "Uri",
    "ScriptRef",
    "DurationSeconds",
];

const UNION_SCHEMAS: &[&str] = &["AutomationSpec"];

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn openapi_document() -> Value {
    let source = fs::read_to_string(repository_root().join("openapi/v1.yaml"))
        .expect("openapi/v1.yaml must be readable");
    serde_yaml::from_str(&source).expect("openapi/v1.yaml must parse as YAML")
}

fn rust_components() -> Value {
    serde_json::to_value(RustContractSchemas::openapi())
        .expect("Rust schemas must serialize")
        .pointer("/components/schemas")
        .expect("generated schemas must have components")
        .clone()
}

fn spec_components(spec: &Value) -> &Map<String, Value> {
    spec.pointer("/components/schemas")
        .and_then(Value::as_object)
        .expect("OpenAPI components.schemas must be an object")
}

fn string_set(value: Option<&Value>) -> BTreeSet<String> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(ToOwned::to_owned)
        .collect()
}

fn property_names(schema: &Value) -> BTreeSet<String> {
    schema
        .get("properties")
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(|properties| properties.keys().cloned())
        .collect()
}

fn enum_values(schema: &Value) -> BTreeSet<String> {
    string_set(schema.get("enum"))
}

fn ref_name(value: &Value) -> Option<&str> {
    value
        .get("$ref")
        .and_then(Value::as_str)
        .and_then(|reference| reference.rsplit('/').next())
}

fn operation<'a>(spec: &'a Value, path: &str, method: &str) -> &'a Value {
    spec.get("paths")
        .and_then(|paths| paths.get(path))
        .and_then(|path_item| path_item.get(method))
        .unwrap_or_else(|| panic!("{method} {path} must exist"))
}

fn property_shape(value: &Value) -> Value {
    for union_key in ["oneOf", "anyOf"] {
        if let Some(variants) = value.get(union_key).and_then(Value::as_array) {
            let non_null = variants
                .iter()
                .filter(|variant| variant.get("type").and_then(Value::as_str) != Some("null"))
                .collect::<Vec<_>>();
            if let [only] = non_null.as_slice() {
                return property_shape(only);
            }
        }
    }

    if let Some(reference) = ref_name(value) {
        return json!({ "ref": reference });
    }

    let mut shape = BTreeMap::<String, Value>::new();
    for key in ["type", "format", "pattern", "minimum", "maximum"] {
        if let Some(item) = value.get(key) {
            let normalized = if key == "type" {
                item.as_array()
                    .map(|types| {
                        types
                            .iter()
                            .filter(|kind| kind.as_str() != Some("null"))
                            .cloned()
                            .collect::<Vec<_>>()
                    })
                    .and_then(|types| match types.as_slice() {
                        [only] => Some(only.clone()),
                        _ => None,
                    })
                    .unwrap_or_else(|| item.clone())
            } else {
                item.clone()
            };
            shape.insert(key.to_owned(), normalized);
        }
    }
    if let Some(items) = value.get("items") {
        shape.insert("items".to_owned(), property_shape(items));
    }
    if let Some(additional) = value.get("additionalProperties") {
        shape.insert(
            "additionalProperties".to_owned(),
            property_shape(additional),
        );
    }
    serde_json::to_value(shape).expect("shape must serialize")
}

fn tagged_union_shape(schema: &Value, components: &Map<String, Value>) -> Vec<Value> {
    let schema = ref_name(schema)
        .and_then(|name| components.get(name))
        .unwrap_or(schema);
    let variants = schema
        .get("oneOf")
        .or_else(|| schema.get("anyOf"))
        .and_then(Value::as_array)
        .expect("tagged union must contain oneOf or anyOf");
    let mut shapes = variants
        .iter()
        .map(|variant| {
            let variant = ref_name(variant)
                .and_then(|name| components.get(name))
                .unwrap_or(variant);
            let properties = variant
                .get("properties")
                .and_then(Value::as_object)
                .expect("tagged-union variants must be objects");
            let properties = properties
                .iter()
                .map(|(name, property)| {
                    let shape = if let Some(value) = property.get("const") {
                        json!({ "literals": [value] })
                    } else if let Some(values) = property.get("enum") {
                        json!({ "literals": values })
                    } else {
                        property_shape(property)
                    };
                    (name.clone(), shape)
                })
                .collect::<BTreeMap<_, _>>();
            json!({
                "required": string_set(variant.get("required")),
                "properties": properties,
            })
        })
        .collect::<Vec<_>>();
    shapes.sort_by_key(Value::to_string);
    shapes
}

#[test]
fn rust_and_openapi_component_structures_match() {
    let spec = openapi_document();
    let documented = spec_components(&spec);
    let generated = rust_components();
    let generated = generated
        .as_object()
        .expect("generated components must be an object");

    for name in OBJECT_SCHEMAS {
        let rust = generated
            .get(*name)
            .unwrap_or_else(|| panic!("Rust schema {name} is missing"));
        let openapi = documented
            .get(*name)
            .unwrap_or_else(|| panic!("OpenAPI schema {name} is missing"));

        assert_eq!(
            property_names(rust),
            property_names(openapi),
            "property drift in {name}"
        );
        assert_eq!(
            string_set(rust.get("required")),
            string_set(openapi.get("required")),
            "required-field drift in {name}"
        );

        let rust_properties = rust
            .get("properties")
            .and_then(Value::as_object)
            .expect("Rust object schema must have properties");
        let openapi_properties = openapi
            .get("properties")
            .and_then(Value::as_object)
            .expect("OpenAPI object schema must have properties");
        for property in rust_properties.keys() {
            let rust_shape = property_shape(&rust_properties[property]);
            let openapi_shape = property_shape(&openapi_properties[property]);
            assert_eq!(
                rust_shape, openapi_shape,
                "shape drift in {name}.{property}"
            );
        }
    }

    for name in ENUM_SCHEMAS {
        let rust = generated
            .get(*name)
            .unwrap_or_else(|| panic!("Rust enum {name} is missing"));
        let openapi = documented
            .get(*name)
            .unwrap_or_else(|| panic!("OpenAPI enum {name} is missing"));
        assert_eq!(
            enum_values(rust),
            enum_values(openapi),
            "enum drift in {name}"
        );
    }

    for name in PRIMITIVE_SCHEMAS {
        let rust = generated
            .get(*name)
            .unwrap_or_else(|| panic!("Rust primitive {name} is missing"));
        let openapi = documented
            .get(*name)
            .unwrap_or_else(|| panic!("OpenAPI primitive {name} is missing"));
        assert_eq!(
            property_shape(rust),
            property_shape(openapi),
            "primitive drift in {name}"
        );
    }

    for name in UNION_SCHEMAS {
        let rust = generated
            .get(*name)
            .unwrap_or_else(|| panic!("Rust union {name} is missing"));
        let openapi = documented
            .get(*name)
            .unwrap_or_else(|| panic!("OpenAPI union {name} is missing"));
        assert_eq!(
            tagged_union_shape(rust, generated),
            tagged_union_shape(openapi, documented),
            "tagged-union drift in {name}"
        );
    }

    let generated_names = generated.keys().cloned().collect::<BTreeSet<_>>();
    let documented_names = documented.keys().cloned().collect::<BTreeSet<_>>();
    assert!(
        generated_names.is_subset(&documented_names),
        "OpenAPI is missing generated Rust components: {:?}",
        generated_names
            .difference(&documented_names)
            .collect::<Vec<_>>()
    );
}

#[test]
fn openapi_operation_inventory_and_cross_cutting_contracts_match() {
    let spec = openapi_document();
    assert_eq!(spec.get("openapi").and_then(Value::as_str), Some("3.1.0"));

    let expected = BTreeSet::from([
        ("/v1/projects", "post"),
        ("/v1/uploads/apk", "post"),
        ("/v1/jobs", "post"),
        ("/v1/jobs", "get"),
        ("/v1/jobs/{job_id}", "get"),
        ("/v1/jobs/{job_id}/events", "get"),
        ("/v1/jobs/{job_id}/artifacts", "get"),
        ("/v1/jobs/{job_id}/debug-sessions", "post"),
        ("/v1/jobs/{job_id}/cancel", "post"),
        ("/v1/webhooks", "post"),
        ("/v1/workers", "get"),
        ("/v1/runtime-profiles", "get"),
    ]);

    let paths = spec
        .get("paths")
        .and_then(Value::as_object)
        .expect("paths must be an object");
    let mut actual = BTreeSet::new();
    for (path, item) in paths {
        let operations = item.as_object().expect("path item must be an object");
        for method in operations
            .keys()
            .filter(|method| matches!(method.as_str(), "get" | "post" | "put" | "patch" | "delete"))
        {
            actual.insert((path.as_str(), method.as_str()));
            let operation = &operations[method];
            let uses_bearer = operation
                .get("security")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_object)
                .any(|security| security.contains_key("BearerAuth"));
            assert!(uses_bearer, "{method} {path} must require BearerAuth");
            assert!(
                operation.get("x-required-scopes").is_some(),
                "{method} {path} must declare x-required-scopes"
            );
        }
    }
    assert_eq!(actual, expected, "public method/path inventory drift");

    let idempotency = spec
        .pointer("/paths/~1v1~1jobs/post/parameters")
        .and_then(Value::as_array)
        .expect("POST /v1/jobs must have parameters");
    assert!(idempotency.iter().any(|parameter| {
        parameter.get("$ref").and_then(Value::as_str)
            == Some("#/components/parameters/IdempotencyKey")
    }));
    assert_eq!(
        spec.pointer("/components/parameters/IdempotencyKey/required")
            .and_then(Value::as_bool),
        Some(true)
    );
    assert!(
        spec.pointer(
            "/paths/~1v1~1jobs~1{job_id}~1events/get/responses/200/content/text~1event-stream"
        )
        .is_some(),
        "events endpoint must document text/event-stream"
    );
}

#[test]
fn operation_scopes_bodies_responses_and_examples_are_exact() {
    struct ExpectedOperation<'a> {
        path: &'a str,
        method: &'a str,
        scopes: &'a [&'a str],
        request: Option<&'a str>,
        success_status: &'a str,
        success_schema: &'a str,
        content_type: &'a str,
    }

    let expected = [
        ExpectedOperation {
            path: "/v1/projects",
            method: "post",
            scopes: &["admin"],
            request: Some("CreateProjectRequest"),
            success_status: "201",
            success_schema: "CreateProjectResponse",
            content_type: "application/json",
        },
        ExpectedOperation {
            path: "/v1/uploads/apk",
            method: "post",
            scopes: &["project:write"],
            request: Some("CreateUploadRequest"),
            success_status: "201",
            success_schema: "CreateUploadResponse",
            content_type: "application/json",
        },
        ExpectedOperation {
            path: "/v1/jobs",
            method: "post",
            scopes: &["project:write"],
            request: Some("CreateJobRequest"),
            success_status: "202",
            success_schema: "Job",
            content_type: "application/json",
        },
        ExpectedOperation {
            path: "/v1/jobs",
            method: "get",
            scopes: &["project:read"],
            request: None,
            success_status: "200",
            success_schema: "JobPage",
            content_type: "application/json",
        },
        ExpectedOperation {
            path: "/v1/jobs/{job_id}",
            method: "get",
            scopes: &["project:read"],
            request: None,
            success_status: "200",
            success_schema: "Job",
            content_type: "application/json",
        },
        ExpectedOperation {
            path: "/v1/jobs/{job_id}/events",
            method: "get",
            scopes: &["project:read"],
            request: None,
            success_status: "200",
            success_schema: "JobEventSse",
            content_type: "text/event-stream",
        },
        ExpectedOperation {
            path: "/v1/jobs/{job_id}/artifacts",
            method: "get",
            scopes: &["project:read"],
            request: None,
            success_status: "200",
            success_schema: "ArtifactPage",
            content_type: "application/json",
        },
        ExpectedOperation {
            path: "/v1/jobs/{job_id}/debug-sessions",
            method: "post",
            scopes: &["debug:create"],
            request: Some("DebugSessionRequest"),
            success_status: "201",
            success_schema: "DebugSessionToken",
            content_type: "application/json",
        },
        ExpectedOperation {
            path: "/v1/jobs/{job_id}/cancel",
            method: "post",
            scopes: &["project:write"],
            request: None,
            success_status: "202",
            success_schema: "Job",
            content_type: "application/json",
        },
        ExpectedOperation {
            path: "/v1/webhooks",
            method: "post",
            scopes: &["project:write"],
            request: Some("CreateWebhookRequest"),
            success_status: "201",
            success_schema: "Webhook",
            content_type: "application/json",
        },
        ExpectedOperation {
            path: "/v1/workers",
            method: "get",
            scopes: &["admin"],
            request: None,
            success_status: "200",
            success_schema: "WorkerPage",
            content_type: "application/json",
        },
        ExpectedOperation {
            path: "/v1/runtime-profiles",
            method: "get",
            scopes: &["admin"],
            request: None,
            success_status: "200",
            success_schema: "RuntimeProfilePage",
            content_type: "application/json",
        },
    ];

    let spec = openapi_document();
    for contract in expected {
        let operation = operation(&spec, contract.path, contract.method);
        assert_eq!(
            string_set(operation.get("x-required-scopes")),
            contract
                .scopes
                .iter()
                .map(|scope| (*scope).to_owned())
                .collect(),
            "scope drift in {} {}",
            contract.method,
            contract.path
        );

        match contract.request {
            Some(expected_schema) => {
                let request_schema = operation
                    .pointer("/requestBody/content/application~1json/schema")
                    .and_then(ref_name);
                assert_eq!(
                    request_schema,
                    Some(expected_schema),
                    "request schema drift in {} {}",
                    contract.method,
                    contract.path
                );
                assert!(
                    operation
                        .pointer("/requestBody/content/application~1json/example")
                        .is_some(),
                    "request example missing in {} {}",
                    contract.method,
                    contract.path
                );
            }
            None => assert!(
                operation.get("requestBody").is_none(),
                "unexpected request body in {} {}",
                contract.method,
                contract.path
            ),
        }

        let response = operation
            .get("responses")
            .and_then(|responses| responses.get(contract.success_status))
            .unwrap_or_else(|| {
                panic!(
                    "success response {} missing in {} {}",
                    contract.success_status, contract.method, contract.path
                )
            });
        let content = response
            .get("content")
            .and_then(|content| content.get(contract.content_type))
            .unwrap_or_else(|| {
                panic!(
                    "{} content missing in {} {}",
                    contract.content_type, contract.method, contract.path
                )
            });
        if contract.success_schema == "JobEventSse" {
            assert_eq!(
                content.pointer("/schema/type").and_then(Value::as_str),
                Some("string"),
                "SSE wire schema drift in {} {}",
                contract.method,
                contract.path
            );
            assert!(
                content
                    .get("example")
                    .and_then(Value::as_str)
                    .is_some_and(|example| example.contains("data:")),
                "SSE example must contain a data frame"
            );
        } else {
            assert_eq!(
                content.get("schema").and_then(ref_name),
                Some(contract.success_schema),
                "success schema drift in {} {}",
                contract.method,
                contract.path
            );
        }
        assert!(
            content.get("example").is_some(),
            "success example missing in {} {}",
            contract.method,
            contract.path
        );

        let responses = operation
            .get("responses")
            .and_then(Value::as_object)
            .expect("responses must be an object");
        for status in ["401", "403", "500", "503"] {
            assert!(
                responses.contains_key(status),
                "standard {status} response missing in {} {}",
                contract.method,
                contract.path
            );
        }
    }

    assert_eq!(
        spec.pointer(
            "/paths/~1v1~1uploads~1apk/post/responses/201/content/application~1json/example/required_headers/x-amz-checksum-sha256"
        )
        .and_then(Value::as_str),
        Some("OnvT4jYKPYDheXxcK3lh5XCStF9y+HS0+9ArXjXXpkw="),
        "signed upload examples must require the SHA-256 checksum header"
    );
}

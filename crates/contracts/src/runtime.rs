use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

use crate::RuntimeProfileId;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeKind {
    AndroidEmulatorContainer,
    Redroid,
    Cuttlefish,
    BrowserNativeWasm,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum IsolationTier {
    VmIsolated,
    SharedKernelPrivileged,
}

impl IsolationTier {
    /// Whether this runtime isolation tier satisfies a job's minimum tier.
    pub const fn satisfies(self, minimum: Self) -> bool {
        matches!(
            (self, minimum),
            (Self::VmIsolated, _) | (Self::SharedKernelPrivileged, Self::SharedKernelPrivileged)
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum HostArch {
    X86_64,
    Aarch64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum AndroidAbi {
    X86,
    X86_64,
    ArmeabiV7a,
    Arm64V8a,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RuntimeProfile {
    pub id: RuntimeProfileId,
    pub android_api: u16,
    pub device_profile: String,
    pub abi: AndroidAbi,
    pub host_arch: HostArch,
    pub runtime_kind: RuntimeKind,
    pub image_ref: String,
    pub isolation_tier: IsolationTier,
}

impl RuntimeProfile {
    /// Validate profile combinations supported by the current release.
    pub fn validate(&self) -> Result<(), RuntimeProfileValidationError> {
        if self.runtime_kind == RuntimeKind::BrowserNativeWasm {
            return Err(RuntimeProfileValidationError::UnsupportedRuntimeKind(
                self.runtime_kind,
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum RuntimeProfileValidationError {
    #[error("runtime kind {0:?} is reserved and is not valid in Part 01 profiles")]
    UnsupportedRuntimeKind(RuntimeKind),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile(runtime_kind: RuntimeKind) -> RuntimeProfile {
        RuntimeProfile {
            id: RuntimeProfileId::new("rtp_android_35").unwrap(),
            android_api: 35,
            device_profile: "pixel_6".to_owned(),
            abi: AndroidAbi::X86_64,
            host_arch: HostArch::X86_64,
            runtime_kind,
            image_ref: "registry.example/android:35".to_owned(),
            isolation_tier: IsolationTier::VmIsolated,
        }
    }

    #[test]
    fn vm_isolation_satisfies_both_minimums() {
        assert!(IsolationTier::VmIsolated.satisfies(IsolationTier::VmIsolated));
        assert!(IsolationTier::VmIsolated.satisfies(IsolationTier::SharedKernelPrivileged));
    }

    #[test]
    fn shared_kernel_does_not_satisfy_vm_minimum() {
        assert!(
            IsolationTier::SharedKernelPrivileged.satisfies(IsolationTier::SharedKernelPrivileged)
        );
        assert!(!IsolationTier::SharedKernelPrivileged.satisfies(IsolationTier::VmIsolated));
    }

    #[test]
    fn browser_native_profiles_are_reserved() {
        assert!(
            profile(RuntimeKind::AndroidEmulatorContainer)
                .validate()
                .is_ok()
        );
        assert_eq!(
            profile(RuntimeKind::BrowserNativeWasm).validate(),
            Err(RuntimeProfileValidationError::UnsupportedRuntimeKind(
                RuntimeKind::BrowserNativeWasm
            ))
        );
    }

    #[test]
    fn wire_enums_use_snake_case() {
        assert_eq!(
            serde_json::to_string(&AndroidAbi::ArmeabiV7a).unwrap(),
            r#""armeabi_v7a""#
        );
        assert_eq!(
            serde_json::to_string(&RuntimeKind::BrowserNativeWasm).unwrap(),
            r#""browser_native_wasm""#
        );
    }
}

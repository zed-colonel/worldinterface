//! wasi:http/outgoing-handler — policy-gated HTTP requests.
//!
//! The WASI HTTP handler is provided by `wasmtime_wasi` via
//! `add_to_linker_sync()`. For policy enforcement, the `CapabilityPolicy`
//! is checked via the custom `process` host function or direct HTTP
//! calls through the resource pool.
//!
//! **Current limitation:** The WASI HTTP outgoing handler registered by
//! wasmtime-wasi does not perform per-URL policy checking against the
//! module's `CapabilityPolicy.http_patterns`. For modules that need
//! HTTP access, the manifest's `capabilities.http` patterns define what's
//! *intended* to be allowed, but enforcement currently relies on network
//! isolation (Docker networking, firewall rules) rather than in-runtime
//! URL filtering.
//!
//! A future enhancement could replace the wasmtime-wasi HTTP handler
//! with a custom implementation that checks `policy.check_http(url)`
//! before each request, similar to how the `process` host function
//! checks `policy.check_process(command)`.
//!
//! The `WasmResourcePool` maintains a separate `reqwest::blocking::Client`
//! for this purpose — ready for when the custom handler is implemented.

use crate::policy::CapabilityPolicy;

/// Check if HTTP access is permitted for a given URL.
///
/// Used by the custom HTTP handler (when implemented) to gate outgoing requests.
pub fn check_http_policy(policy: &CapabilityPolicy, url: &str) -> Result<(), String> {
    policy.check_http(url).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::CapabilityPolicy;

    // ── E2S3-T37 partial: HTTP policy check for allowed URL ──

    #[test]
    fn http_policy_allows_matching_url() {
        let manifest_toml = r#"
[connector]
name = "test.http"

[capabilities]
http = ["api.example.com"]
"#;
        let manifest = crate::manifest::ConnectorManifest::from_toml(manifest_toml).unwrap();
        let policy = CapabilityPolicy::from_manifest(&manifest).unwrap();
        assert!(check_http_policy(&policy, "https://api.example.com/v1/data").is_ok());
    }

    // ── E2S3-T38 partial: HTTP policy check for denied URL ──

    #[test]
    fn http_policy_denies_non_matching_url() {
        let manifest_toml = r#"
[connector]
name = "test.http"

[capabilities]
http = ["api.example.com"]
"#;
        let manifest = crate::manifest::ConnectorManifest::from_toml(manifest_toml).unwrap();
        let policy = CapabilityPolicy::from_manifest(&manifest).unwrap();
        assert!(check_http_policy(&policy, "https://evil.com/hack").is_err());
    }
}

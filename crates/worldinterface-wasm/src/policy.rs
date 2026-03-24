//! Deny-by-default capability policy compiled from a connector manifest.
//!
//! Every host function call checks the policy before performing any I/O.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::error::{ManifestError, PolicyViolation};
use crate::manifest::ConnectorManifest;

/// Deny-by-default capability policy compiled from a manifest.
///
/// All check methods return `Result<(), PolicyViolation>`.
/// An empty capabilities section produces a deny-all policy.
pub struct CapabilityPolicy {
    pub http_patterns: Vec<HostPattern>,
    pub filesystem_prefixes: Vec<PathBuf>,
    pub process_commands: HashSet<String>,
    pub environment_vars: HashSet<String>,
    pub websocket_patterns: Vec<HostPattern>,
    pub sockets_allowed: bool,
    pub kv_allowed: bool,
    pub logging_allowed: bool,
    pub crypto_allowed: bool,
    pub max_fuel: u64,
    pub timeout: Duration,
    pub max_memory_bytes: usize,
    pub max_http_concurrent: usize,
}

/// Matches hostnames against manifest patterns.
///
/// Patterns:
/// - "api.example.com" → exact hostname match
/// - "*.github.com" → suffix match (matches "api.github.com", "raw.github.com")
pub struct HostPattern {
    kind: PatternKind,
}

enum PatternKind {
    Exact(String),
    WildcardSuffix(String), // "*.github.com" → stores ".github.com"
}

impl HostPattern {
    /// Parse a pattern string.
    pub fn parse(pattern: &str) -> Self {
        if let Some(suffix) = pattern.strip_prefix('*') {
            Self { kind: PatternKind::WildcardSuffix(suffix.to_string()) }
        } else {
            Self { kind: PatternKind::Exact(pattern.to_string()) }
        }
    }

    /// Test if a hostname matches this pattern.
    pub fn matches(&self, hostname: &str) -> bool {
        match &self.kind {
            PatternKind::Exact(exact) => hostname == exact,
            PatternKind::WildcardSuffix(suffix) => hostname.ends_with(suffix),
        }
    }
}

impl CapabilityPolicy {
    /// Compile a policy from manifest capabilities + resources.
    pub fn from_manifest(manifest: &ConnectorManifest) -> Result<Self, ManifestError> {
        let caps = &manifest.capabilities;
        let res = &manifest.resources;

        Ok(Self {
            http_patterns: caps.http.iter().map(|p| HostPattern::parse(p)).collect(),
            websocket_patterns: caps.websocket.iter().map(|p| HostPattern::parse(p)).collect(),
            filesystem_prefixes: caps.filesystem.clone(),
            process_commands: caps.process.iter().cloned().collect(),
            environment_vars: caps.environment.iter().cloned().collect(),
            sockets_allowed: !caps.sockets.is_empty(),
            kv_allowed: caps.kv,
            logging_allowed: caps.logging,
            crypto_allowed: caps.crypto,
            max_fuel: res.max_fuel,
            timeout: Duration::from_millis(res.timeout_ms),
            max_memory_bytes: res.max_memory_bytes,
            max_http_concurrent: res.max_http_concurrent,
        })
    }

    /// Returns a deny-all policy (for testing or as fallback).
    pub fn deny_all() -> Self {
        Self {
            http_patterns: Vec::new(),
            websocket_patterns: Vec::new(),
            filesystem_prefixes: Vec::new(),
            process_commands: HashSet::new(),
            environment_vars: HashSet::new(),
            sockets_allowed: false,
            kv_allowed: false,
            logging_allowed: false,
            crypto_allowed: false,
            max_fuel: 1_000_000_000,
            timeout: Duration::from_secs(30),
            max_memory_bytes: 67_108_864,
            max_http_concurrent: 0,
        }
    }

    /// Check if an HTTP URL is allowed by the policy.
    pub fn check_http(&self, url: &str) -> Result<(), PolicyViolation> {
        // Extract hostname from URL
        let hostname = extract_hostname(url)
            .ok_or_else(|| PolicyViolation::HttpDenied { url: url.to_string() })?;

        for pattern in &self.http_patterns {
            if pattern.matches(&hostname) {
                return Ok(());
            }
        }
        Err(PolicyViolation::HttpDenied { url: url.to_string() })
    }

    /// Check if a WebSocket URL is allowed by the policy.
    pub fn check_websocket(&self, url: &str) -> Result<(), PolicyViolation> {
        let hostname = extract_hostname(url)
            .ok_or_else(|| PolicyViolation::WebSocketDenied { url: url.to_string() })?;

        for pattern in &self.websocket_patterns {
            if pattern.matches(&hostname) {
                return Ok(());
            }
        }
        Err(PolicyViolation::WebSocketDenied { url: url.to_string() })
    }

    /// Check if a filesystem path is within allowed prefixes.
    pub fn check_filesystem(&self, path: &Path) -> Result<(), PolicyViolation> {
        // Canonicalize to prevent path traversal (../ attacks)
        // If canonicalization fails (path doesn't exist), use the path as-is
        // but still do lexical normalization
        let normalized = normalize_path(path);

        for prefix in &self.filesystem_prefixes {
            if normalized.starts_with(prefix) {
                return Ok(());
            }
        }
        Err(PolicyViolation::FilesystemDenied { path: path.display().to_string() })
    }

    /// Check if a process command is in the allowlist.
    pub fn check_process(&self, command: &str) -> Result<(), PolicyViolation> {
        if self.process_commands.contains(command) {
            Ok(())
        } else {
            Err(PolicyViolation::ProcessDenied { command: command.to_string() })
        }
    }

    /// Check if an environment variable is in the allowlist.
    pub fn check_environment(&self, var: &str) -> Result<(), PolicyViolation> {
        if self.environment_vars.contains(var) {
            Ok(())
        } else {
            Err(PolicyViolation::EnvironmentDenied { var: var.to_string() })
        }
    }

    /// Check if raw socket access is permitted.
    pub fn check_sockets(&self) -> Result<(), PolicyViolation> {
        if self.sockets_allowed {
            Ok(())
        } else {
            Err(PolicyViolation::SocketsDenied)
        }
    }

    /// Check if KV store access is permitted.
    pub fn check_kv(&self) -> Result<(), PolicyViolation> {
        if self.kv_allowed {
            Ok(())
        } else {
            Err(PolicyViolation::CapabilityDenied { capability: "kv".into() })
        }
    }

    /// Check if logging is permitted.
    pub fn check_logging(&self) -> Result<(), PolicyViolation> {
        if self.logging_allowed {
            Ok(())
        } else {
            Err(PolicyViolation::CapabilityDenied { capability: "logging".into() })
        }
    }

    /// Check if crypto operations are permitted.
    pub fn check_crypto(&self) -> Result<(), PolicyViolation> {
        if self.crypto_allowed {
            Ok(())
        } else {
            Err(PolicyViolation::CapabilityDenied { capability: "crypto".into() })
        }
    }
}

/// Extract hostname from a URL string.
fn extract_hostname(url: &str) -> Option<String> {
    // Simple URL parsing: strip scheme, then take hostname before port/path
    let without_scheme = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .or_else(|| url.strip_prefix("wss://"))
        .or_else(|| url.strip_prefix("ws://"))
        .unwrap_or(url);

    let host_part = without_scheme.split('/').next()?;
    let hostname = host_part.split(':').next()?;

    if hostname.is_empty() {
        None
    } else {
        Some(hostname.to_string())
    }
}

/// Normalize a path by resolving `.` and `..` segments lexically
/// (without touching the filesystem). This prevents path traversal attacks.
fn normalize_path(path: &Path) -> PathBuf {
    let mut components = Vec::new();
    for component in path.components() {
        match component {
            std::path::Component::ParentDir => {
                // Only pop if there's something to pop (and it's not root)
                if components.last().is_some_and(|c| !matches!(c, std::path::Component::RootDir)) {
                    components.pop();
                }
            }
            std::path::Component::CurDir => {} // skip
            other => components.push(other),
        }
    }
    components.iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy_with_http(patterns: &[&str]) -> CapabilityPolicy {
        let mut policy = CapabilityPolicy::deny_all();
        policy.http_patterns = patterns.iter().map(|p| HostPattern::parse(p)).collect();
        policy
    }

    fn policy_with_fs(prefixes: &[&str]) -> CapabilityPolicy {
        let mut policy = CapabilityPolicy::deny_all();
        policy.filesystem_prefixes = prefixes.iter().map(PathBuf::from).collect();
        policy
    }

    fn policy_with_process(commands: &[&str]) -> CapabilityPolicy {
        let mut policy = CapabilityPolicy::deny_all();
        policy.process_commands = commands.iter().map(|s| s.to_string()).collect();
        policy
    }

    fn policy_with_env(vars: &[&str]) -> CapabilityPolicy {
        let mut policy = CapabilityPolicy::deny_all();
        policy.environment_vars = vars.iter().map(|s| s.to_string()).collect();
        policy
    }

    // ── E2S3-T4: Policy: check_http allows matching exact hostname ──

    #[test]
    fn policy_http_allows_exact_match() {
        let policy = policy_with_http(&["api.example.com"]);
        assert!(policy.check_http("https://api.example.com/v1/data").is_ok());
    }

    // ── E2S3-T5: Policy: check_http rejects non-matching hostname ──

    #[test]
    fn policy_http_rejects_non_matching() {
        let policy = policy_with_http(&["api.example.com"]);
        let result = policy.check_http("https://evil.com/hack");
        assert!(matches!(result, Err(PolicyViolation::HttpDenied { .. })));
    }

    // ── E2S3-T6: Policy: check_http wildcard pattern (*.github.com) ──

    #[test]
    fn policy_http_wildcard_pattern() {
        let policy = policy_with_http(&["*.github.com"]);
        assert!(policy.check_http("https://api.github.com/repos").is_ok());
        assert!(policy.check_http("https://raw.github.com/file").is_ok());
        // Bare "github.com" should NOT match "*.github.com" (no dot prefix)
        assert!(policy.check_http("https://github.com/repo").is_err());
    }

    // ── E2S3-T7: Policy: check_filesystem allows matching prefix ──

    #[test]
    fn policy_filesystem_allows_prefix() {
        let policy = policy_with_fs(&["/tmp/data"]);
        assert!(policy.check_filesystem(Path::new("/tmp/data/file.txt")).is_ok());
        assert!(policy.check_filesystem(Path::new("/tmp/data")).is_ok());
    }

    // ── E2S3-T8: Policy: check_filesystem rejects outside prefix ──

    #[test]
    fn policy_filesystem_rejects_outside() {
        let policy = policy_with_fs(&["/tmp/data"]);
        let result = policy.check_filesystem(Path::new("/etc/passwd"));
        assert!(matches!(result, Err(PolicyViolation::FilesystemDenied { .. })));
    }

    // ── E2S3-T9: Policy: check_filesystem prevents path traversal (../) ──

    #[test]
    fn policy_filesystem_prevents_path_traversal() {
        let policy = policy_with_fs(&["/tmp/data"]);
        // Try to escape via path traversal
        let result = policy.check_filesystem(Path::new("/tmp/data/../secret"));
        assert!(
            matches!(result, Err(PolicyViolation::FilesystemDenied { .. })),
            "path traversal should be blocked"
        );
    }

    // ── E2S3-T10: Policy: check_process allows listed command ──

    #[test]
    fn policy_process_allows_listed() {
        let policy = policy_with_process(&["echo", "cat"]);
        assert!(policy.check_process("echo").is_ok());
        assert!(policy.check_process("cat").is_ok());
    }

    // ── E2S3-T11: Policy: check_process rejects unlisted command ──

    #[test]
    fn policy_process_rejects_unlisted() {
        let policy = policy_with_process(&["echo"]);
        let result = policy.check_process("rm");
        assert!(matches!(result, Err(PolicyViolation::ProcessDenied { .. })));
    }

    // ── E2S3-T12: Policy: check_environment allows listed var ──

    #[test]
    fn policy_environment_allows_listed() {
        let policy = policy_with_env(&["HOME", "PATH"]);
        assert!(policy.check_environment("HOME").is_ok());
        assert!(policy.check_environment("PATH").is_ok());
    }

    // ── E2S3-T13: Policy: check_environment rejects unlisted var ──

    #[test]
    fn policy_environment_rejects_unlisted() {
        let policy = policy_with_env(&["HOME"]);
        let result = policy.check_environment("SECRET_KEY");
        assert!(matches!(result, Err(PolicyViolation::EnvironmentDenied { .. })));
    }

    // ── E2S3-T14: Policy: deny_all blocks everything ──

    #[test]
    fn policy_deny_all_blocks_everything() {
        let policy = CapabilityPolicy::deny_all();
        assert!(policy.check_http("https://example.com").is_err());
        assert!(policy.check_websocket("wss://example.com").is_err());
        assert!(policy.check_filesystem(Path::new("/tmp")).is_err());
        assert!(policy.check_process("echo").is_err());
        assert!(policy.check_environment("HOME").is_err());
        assert!(policy.check_sockets().is_err());
        assert!(policy.check_kv().is_err());
        assert!(policy.check_logging().is_err());
        assert!(policy.check_crypto().is_err());
    }

    // ── E2S3-T15: PolicyViolation: each variant produces distinct message ──

    #[test]
    fn policy_violation_distinct_messages() {
        let violations = [
            PolicyViolation::HttpDenied { url: "https://evil.com".into() },
            PolicyViolation::WebSocketDenied { url: "wss://evil.com".into() },
            PolicyViolation::FilesystemDenied { path: "/secret".into() },
            PolicyViolation::ProcessDenied { command: "rm".into() },
            PolicyViolation::EnvironmentDenied { var: "SECRET".into() },
            PolicyViolation::SocketsDenied,
            PolicyViolation::CapabilityDenied { capability: "kv".into() },
        ];

        let messages: Vec<String> = violations.iter().map(|v| v.to_string()).collect();
        // Each message should be unique
        let unique: HashSet<&String> = messages.iter().collect();
        assert_eq!(unique.len(), messages.len(), "all violations must have distinct messages");

        // Verify key content in messages
        assert!(messages[0].contains("evil.com"));
        assert!(messages[1].contains("evil.com"));
        assert!(messages[2].contains("/secret"));
        assert!(messages[3].contains("rm"));
        assert!(messages[4].contains("SECRET"));
        assert!(messages[5].contains("socket"));
        assert!(messages[6].contains("kv"));
    }

    #[test]
    fn policy_from_manifest() {
        let toml = r#"
[connector]
name = "test.policy"

[capabilities]
http = ["api.example.com"]
filesystem = ["/tmp"]
process = ["echo"]
environment = ["HOME"]
kv = true
logging = true
crypto = true

[resources]
max_fuel = 500_000_000
timeout_ms = 10000
"#;
        let manifest = crate::manifest::ConnectorManifest::from_toml(toml).unwrap();
        let policy = CapabilityPolicy::from_manifest(&manifest).unwrap();

        assert!(policy.check_http("https://api.example.com/data").is_ok());
        assert!(policy.check_http("https://evil.com").is_err());
        assert!(policy.check_filesystem(Path::new("/tmp/file")).is_ok());
        assert!(policy.check_process("echo").is_ok());
        assert!(policy.check_environment("HOME").is_ok());
        assert!(policy.check_kv().is_ok());
        assert!(policy.check_logging().is_ok());
        assert!(policy.check_crypto().is_ok());
        assert!(policy.check_sockets().is_err());
        assert_eq!(policy.max_fuel, 500_000_000);
        assert_eq!(policy.timeout, std::time::Duration::from_millis(10000));
    }

    #[test]
    fn normalize_path_resolves_traversal() {
        assert_eq!(normalize_path(Path::new("/a/b/../c")), PathBuf::from("/a/c"));
        assert_eq!(normalize_path(Path::new("/a/./b")), PathBuf::from("/a/b"));
        assert_eq!(normalize_path(Path::new("/tmp/data/../secret")), PathBuf::from("/tmp/secret"));
    }

    #[test]
    fn extract_hostname_works() {
        assert_eq!(extract_hostname("https://api.example.com/v1"), Some("api.example.com".into()));
        assert_eq!(extract_hostname("http://localhost:8080/path"), Some("localhost".into()));
        assert_eq!(extract_hostname("https://host.com"), Some("host.com".into()));
    }

    // ── E4S2 WebSocket policy tests ──

    fn policy_with_websocket(patterns: &[&str]) -> CapabilityPolicy {
        let mut policy = CapabilityPolicy::deny_all();
        policy.websocket_patterns = patterns.iter().map(|p| HostPattern::parse(p)).collect();
        policy
    }

    #[test]
    fn policy_websocket_allows_matching() {
        let policy = policy_with_websocket(&["gateway.discord.gg"]);
        assert!(policy.check_websocket("wss://gateway.discord.gg/?v=10").is_ok());
    }

    #[test]
    fn policy_websocket_denies_non_matching() {
        let policy = policy_with_websocket(&["gateway.discord.gg"]);
        let result = policy.check_websocket("wss://evil.com/ws");
        assert!(matches!(result, Err(PolicyViolation::WebSocketDenied { .. })));
    }

    #[test]
    fn policy_websocket_wildcard() {
        let policy = policy_with_websocket(&["*.discord.gg"]);
        assert!(policy.check_websocket("wss://gateway.discord.gg/?v=10").is_ok());
        assert!(policy.check_websocket("wss://other.discord.gg/ws").is_ok());
        // Bare "discord.gg" should NOT match "*.discord.gg"
        assert!(policy.check_websocket("wss://discord.gg/ws").is_err());
    }

    #[test]
    fn extract_hostname_handles_ws_schemes() {
        assert_eq!(extract_hostname("ws://localhost:8080/ws"), Some("localhost".into()));
        assert_eq!(
            extract_hostname("wss://gateway.discord.gg/?v=10"),
            Some("gateway.discord.gg".into())
        );
    }

    #[test]
    fn policy_from_manifest_includes_websocket() {
        let toml = r#"
[connector]
name = "test.ws"

[capabilities]
websocket = ["gateway.discord.gg"]
"#;
        let manifest = crate::manifest::ConnectorManifest::from_toml(toml).unwrap();
        let policy = CapabilityPolicy::from_manifest(&manifest).unwrap();
        assert!(policy.check_websocket("wss://gateway.discord.gg/?v=10").is_ok());
        assert!(policy.check_websocket("wss://evil.com").is_err());
    }

    #[test]
    fn policy_deny_all_blocks_websocket() {
        let policy = CapabilityPolicy::deny_all();
        assert!(policy.check_websocket("wss://example.com").is_err());
    }
}

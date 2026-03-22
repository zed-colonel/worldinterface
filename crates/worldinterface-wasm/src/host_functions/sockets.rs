//! wasi:sockets — stub implementation (returns "not supported").
//!
//! Raw socket support is stubbed. No known use case for raw sockets
//! in connector modules. The `ManifestCapabilities.sockets` field is
//! reserved and parsed but not actionable.

use crate::error::PolicyViolation;

/// All socket operations return this error.
pub fn deny_sockets() -> PolicyViolation {
    PolicyViolation::SocketsDenied
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── E2S3-T44: Host sockets: all operations → SocketsDenied ──

    #[test]
    fn sockets_all_operations_denied() {
        let violation = deny_sockets();
        assert!(matches!(violation, PolicyViolation::SocketsDenied));
        assert!(violation.to_string().contains("socket"));
    }
}

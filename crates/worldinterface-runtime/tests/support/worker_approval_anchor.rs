//! Trusted test-host provisioning, separate from the source packet it approves.
//! Acceptance and cold-run evidence: docs/work-items/WIP-15-verification.md.
//! Actual operator guidance supplied at startup of CraftingTable run
//! 782e12b7-d455-4025-9daa-93c120543783 on 2026-11-07 (UTC):
//! "I accept the evaluation-only packet
//! b24ac0e082751f2f0711bf5a07c6ef69f0b8c2cf5115d9c4241a3c389cf9a6a7
//! through 2026-11-11T00:00:00Z, under the unchanged limits in the
//! evaluation request."
//! The receipt identifies that supplied guidance, not a controller-issued
//! decision ID or an independently inferred operator UUID. Its finite expiry
//! ends at the explicitly accepted 2026-11-11T00:00:00Z.
//!
//! Accepted canonical packet:
//! b24ac0e082751f2f0711bf5a07c6ef69f0b8c2cf5115d9c4241a3c389cf9a6a7.
//! Includes supervisor-disposition.json's exact worker_process/mod.rs pin
//! f6e7aaa9abc4b98a8f91f9b090920dc7a9e329a33d58c14378f7d9361dcf35af.
//! The original five-file exception and generic continuation ban are unchanged.
//!
//! Evaluation only for this candidate's verification and CraftingTable review:
//! the accepted manifest's exact cases, sources and typed tasks, including the
//! WP-033/034 campaign; provision, start, inspect, terminate, collect-artifacts
//! and release; reviewed nonsensitive fixtures; at most 15000 ms wall time
//! and 65536 bytes retained output. No ordinary use, production credentials,
//! hostile code, same-user isolation claim, publication, integration or gate
//! acceptance. These records encode only that supplied acceptance.
pub const ACCEPTED_RECEIPT_SHA256: Option<&str> =
    Some("5d90d7a04cea82bddaa0e19938ac9a94e8a5ef9d605bd1f3fdf3c9ddea3b55a7");

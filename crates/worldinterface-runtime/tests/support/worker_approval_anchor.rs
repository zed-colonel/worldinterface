//! Trusted test-host provisioning, separate from the source packet it approves.
//! Acceptance and cold-run evidence: docs/work-items/WIP-15-verification.md.
//! Actual operator guidance supplied at startup of CraftingTable run
//! 0d74d481-bb67-4da0-9ac8-2494520c0cd5 on 2026-11-07 (UTC):
//! "I accept the evaluation-only packet
//! 87f72bf1a2475d82e5c21955920234fddd77bb51f364abd29ddcad2c8d633ad9
//! on a659443 through 2026-11-11T00:00Z, under unchanged evaluation limits."
//! The receipt identifies that supplied guidance, not a controller-issued
//! decision ID or an independently inferred operator UUID. Its finite expiry
//! ends at the explicitly accepted 2026-11-11T00:00:00Z.
//!
//! Accepted canonical packet:
//! 87f72bf1a2475d82e5c21955920234fddd77bb51f364abd29ddcad2c8d633ad9.
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
    Some("c82b7128f978d3175c8ed013f3fb9a84312210b077ee740fc5d5e86d11a44da4");

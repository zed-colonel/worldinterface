//! Trusted test-host provisioning, separate from the source packet it approves.
//! Acceptance provenance: docs/work-items/WIP-16-verification.md.
//! Actual operator guidance supplied at startup of CraftingTable run
//! c1d9d28c-0d80-4913-bdd3-777f8b5b0ca0 on 2026-11-07 (UTC):
//! "I accept the evaluation-only packet
//! 7d09d13451f0f793e2c04f82ff3eafb3e28e42b788c3d4e7dd9fdb8772d744b5
//! through 2026-11-11T00:00:00Z, for the existing six operations on reviewed
//! nonsensitive fixtures, with 15-second wall and 65,536-byte retained-output limits"
//! The receipt identifies that supplied guidance, not a controller-issued
//! decision ID or an independently inferred operator UUID. Its finite expiry
//! ends at the explicitly accepted 2026-11-11T00:00:00Z.
//!
//! Accepted canonical packet:
//! 7d09d13451f0f793e2c04f82ff3eafb3e28e42b788c3d4e7dd9fdb8772d744b5.
//! Prepared and non-live validated on 7577738 in run
//! 5f81f4d2-2cf6-4cb6-9e0f-8ea8f5dee8ab before acceptance.
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
    Some("20168b1f099ca7d5468fbed0f7160e0f906f3fbdfc03517399014522b50a1b56");

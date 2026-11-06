//! Trusted test-host provisioning, separate from the source packet it approves.
//! Actual operator guidance supplied at startup of CraftingTable run
//! 59aa52f7-73cc-435c-ae1a-faca6eebff17 on 2026-11-06 (UTC):
//! "I approve final evaluation packet
//! 4931dbe1e63e4532a0aed6e25d649a30b9fa67cc4dbfa51fa8806863fe023d6c
//! through 2026-11-11T00:00:00Z, under the unchanged fixture limits.
//! This supersedes all earlier packet requests."
//! The receipt identifies that supplied guidance, not a controller-issued
//! decision ID or an independently inferred operator UUID. Its finite expiry
//! ends at the explicitly accepted 2026-11-11T00:00:00Z.
//!
//! Accepted canonical packet:
//! 4931dbe1e63e4532a0aed6e25d649a30b9fa67cc4dbfa51fa8806863fe023d6c.
//! Includes supervisor-disposition.json's exact worker_process/mod.rs pin
//! f6e7aaa9abc4b98a8f91f9b090920dc7a9e329a33d58c14378f7d9361dcf35af.
//! The original five-file exception and generic continuation ban are unchanged.
//!
//! Evaluation only for this candidate's verification and CraftingTable review:
//! WP-007, WP-017 through WP-032, AB-WI-021 and source-bound WIP-09 regressions;
//! provision, start, inspect, terminate, collect-artifacts and release;
//! only evaluation-request.md's typed tasks; at most 15000 ms wall time
//! and 65536 bytes retained output. No ordinary use, production credentials,
//! hostile code, same-user isolation claim, publication, integration or gate
//! acceptance. These records encode only that supplied acceptance.
pub const ACCEPTED_RECEIPT_SHA256: Option<&str> =
    Some("ac0e2ea9788d365308ede1c0d7dfcfbeadf27b25cdb58afc8068a44f2af58104");

//! Trusted test-host provisioning, separate from the source packet it approves.
//! Actual operator guidance supplied at startup of CraftingTable run
//! e3b25e99-9087-437b-8bb8-7fdd01be3b5a on 2026-11-05 (UTC):
//! "I approved and accept packet
//! e873492675e10878af51baf023255f5650347465d62a98bade03fe516e2bb594
//! through 2026-11-11T00:00:00Z, with unchanged fixture limits."
//! The receipt identifies that supplied guidance, not a controller-issued
//! decision ID or an independently inferred operator UUID. Its finite expiry
//! ends at the explicitly accepted 2026-11-11T00:00:00Z.
//!
//! Accepted canonical packet:
//! e873492675e10878af51baf023255f5650347465d62a98bade03fe516e2bb594.
//! Includes supervisor-disposition.json's exact worker_process/mod.rs pin
//! f6e7aaa9abc4b98a8f91f9b090920dc7a9e329a33d58c14378f7d9361dcf35af.
//! The original five-file exception and generic continuation ban are unchanged.
//!
//! Evaluation only for this candidate's verification and CraftingTable review:
//! WP-007, WP-017 through WP-028, WP-031, WP-032, AB-WI-021 and source-bound
//! WIP-09 regressions;
//! provision, start, inspect, terminate, collect-artifacts and release;
//! only evaluation-request.md's typed tasks; at most 15000 ms wall time
//! and 65536 bytes retained output. No ordinary use, production credentials,
//! hostile code, same-user isolation claim, publication, integration or gate
//! acceptance. These records encode only that supplied acceptance.
pub const ACCEPTED_RECEIPT_SHA256: Option<&str> =
    Some("42aa433da650a5cab1d541f47eacfb309ea4b4e0ad7db8c5755fa5a5e42636b8");

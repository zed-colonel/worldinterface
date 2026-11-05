//! Trusted test-host provisioning, separate from the source packet it approves.
//! Actual operator guidance supplied at startup of CraftingTable run
//! 2f4590d2-b14c-49b6-be70-19b9f60c8a3d on 2026-11-05 (UTC):
//! "I approved and accept packet
//! c977d25a9a328229354c086ed797dc86a0033414c7e62b60ef6d2ff3e8998b88
//! through 2026-11-11T00:00:00Z, with unchanged fixture limits."
//! The receipt identifies that supplied guidance, not a controller-issued
//! decision ID or an independently inferred operator UUID. Its finite expiry
//! ends at the explicitly accepted 2026-11-11T00:00:00Z.
//!
//! Accepted canonical packet:
//! c977d25a9a328229354c086ed797dc86a0033414c7e62b60ef6d2ff3e8998b88.
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
    Some("01aa4bb8a1f87c7d2002411d86bc53df9f3667c8ea26dcc8589a19ea49a83a2c");

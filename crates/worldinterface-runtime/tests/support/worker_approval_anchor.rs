//! Trusted test-host provisioning, separate from the source packet it approves.
//! Actual operator guidance supplied at startup of CraftingTable run
//! c0deea63-825e-4724-bf55-c15ceafb2c40 on 2026-11-05 (UTC):
//! "I approve the revised packet
//! 34f2d904342b8c49d915f11f631878084e07978000cc2ec3abe528b0b781b957,
//! including its unchanged supervisor disposition, through 2026-11-11T00:00:00Z.
//! The proposed scope remains the reviewed fixtures, six supported operations,
//! nonsensitive inputs, 15-second wall time and 64-KiB retained output."
//! The receipt identifies that supplied guidance, not a controller-issued
//! decision ID or an independently inferred operator UUID. Its finite expiry
//! ends at the explicitly accepted 2026-11-11T00:00:00Z.
//!
//! Accepted canonical packet:
//! 34f2d904342b8c49d915f11f631878084e07978000cc2ec3abe528b0b781b957.
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
    Some("5cdea33d81e885b77532e64d26b3471b8e48dfb8a17261096263adf3ceb599f0");

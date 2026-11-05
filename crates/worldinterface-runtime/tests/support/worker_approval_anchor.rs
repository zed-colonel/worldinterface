//! Trusted test-host provisioning, separate from the source packet it approves.
//! Actual operator guidance supplied at startup of CraftingTable run
//! faf54e93-c7a3-4d19-8932-739fdb3a5c74 on 2026-11-05 (UTC):
//! "I approve packet
//! e99491d6c8a8f33d48fa7c7a84f31046405480659c133df97abae5cc6b65983c,
//! including its unchanged supervisor disposition, through 2026-11-11T00:00:00Z.
//! I agree with proposed resolution: approve evaluation only under the unchanged
//! scope—reviewed fixtures, six operations, nonsensitive inputs, 15-second wall
//! time and 64-KiB retained output."
//! The receipt identifies that supplied guidance, not a controller-issued
//! decision ID or an independently inferred operator UUID. Its finite expiry
//! ends at the explicitly accepted 2026-11-11T00:00:00Z.
//!
//! Accepted canonical packet:
//! e99491d6c8a8f33d48fa7c7a84f31046405480659c133df97abae5cc6b65983c.
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
    Some("7c0f392f6b0dd6ddd87f7c12530fb09c002b8d69736cb825ebec509b53cb5f5e");

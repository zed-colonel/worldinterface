//! Trusted test-host provisioning, separate from the source packet it approves.
//! Actual operator guidance supplied at startup of CraftingTable run
//! e191c2f5-10ab-43ce-993d-b25260c71985 on 2026-11-06 (UTC):
//! "I approve replacement packet
//! b57f32a001ff95ef14ac46d35e22a96c61c0f35896e17e0dc6e9762aebc18b88
//! through 2026-11-11T00:00:00Z, under the unchanged fixture limits,
//! superseding earlier replacement requests."
//! The receipt identifies that supplied guidance, not a controller-issued
//! decision ID or an independently inferred operator UUID. Its finite expiry
//! ends at the explicitly accepted 2026-11-11T00:00:00Z.
//!
//! Accepted canonical packet:
//! b57f32a001ff95ef14ac46d35e22a96c61c0f35896e17e0dc6e9762aebc18b88.
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
    Some("89fe6e6ec887af4e045e21d7bb78f3935ddeaf88e642e403023d02ebe28ea449");

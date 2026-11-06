//! Trusted test-host provisioning, separate from the source packet it approves.
//! Actual operator guidance supplied at startup of CraftingTable run
//! 23917702-7b27-4be4-8317-2ff31d928044 on 2026-11-06 (UTC):
//! "I accept evaluation-only packet
//! 4780080955b38a775f724de0970c02531cbe542a145bf15b2ecc8088c549ed83
//! through 2026-11-11T00:00:00Z, under the unchanged limits in the
//! evaluation request."
//! The receipt identifies that supplied guidance, not a controller-issued
//! decision ID or an independently inferred operator UUID. Its finite expiry
//! ends at the explicitly accepted 2026-11-11T00:00:00Z.
//!
//! Accepted canonical packet:
//! 4780080955b38a775f724de0970c02531cbe542a145bf15b2ecc8088c549ed83.
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
    Some("b76e804d5f69947cfb9e4a5efd8dfc49f463b4a67afd0fa79ab51186b440d7b0");

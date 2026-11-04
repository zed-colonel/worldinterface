//! Trusted test-host provisioning, separate from the source packet it approves.
//! Actual operator guidance supplied at startup of CraftingTable run
//! 1a5c73bb-1389-486a-8342-b2c9b4c4cc1d on 2026-11-04:
//! "I approve the evaluation-only packet
//! 703c30858557e209c9843e1f19a037c9c48f955c58bbc79b2bb67f0193adcec2,
//! including its supervisor disposition, for seven days after acceptance"
//! The receipt identifies that supplied guidance, not a controller-issued
//! decision ID or an independently inferred operator UUID. Its finite expiry
//! conservatively ends 2026-11-11T00:00:00Z, within the accepted seven-day window.
//!
//! Accepted canonical packet:
//! 703c30858557e209c9843e1f19a037c9c48f955c58bbc79b2bb67f0193adcec2.
//! Includes supervisor-disposition.json's exact worker_process/mod.rs pin
//! e7670cb6bd56feac3140396fba9708fb50bc8938790f6b873ef2a5bfe1814795.
//! The original five-file exception and generic continuation ban are unchanged.
//!
//! Evaluation only for this candidate's verification and CraftingTable review:
//! WP-017 through WP-028, WP-031, AB-WI-021 and source-bound WIP-09 regressions;
//! provision, start, inspect, terminate, collect-artifacts and release;
//! only evaluation-request.md's typed tasks; at most 15000 ms wall time
//! and 65536 bytes retained output. No ordinary use, production credentials,
//! hostile code, same-user isolation claim, publication, integration or gate
//! acceptance. These records encode only that supplied acceptance.
pub const ACCEPTED_RECEIPT_SHA256: Option<&str> =
    Some("df5d2651699c201148cfe811e867bb742953de25b980c0c2774d933b22e3eaa9");

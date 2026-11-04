//! Trusted test-host provisioning, separate from the source packet it approves.
//! Actual operator guidance supplied at startup of CraftingTable run
//! b817a755-c855-4e6a-88f8-4201703b5b78 on 2026-11-04:
//! "I approve rhe evaluation-only packet
//! 2b8be6d80b522c4b3bf6776009067ef58030f97ed97ef8a04b7f0704148fc921,
//! including its updated supervisor disposition, for seven days after acceptance"
//! The receipt identifies that supplied guidance, not a controller-issued
//! decision ID or an independently inferred operator UUID. Its finite expiry
//! conservatively ends 2026-11-11T00:00:00Z, within the accepted seven-day window.
//!
//! Accepted canonical packet:
//! 2b8be6d80b522c4b3bf6776009067ef58030f97ed97ef8a04b7f0704148fc921.
//! Includes supervisor-disposition.json's exact worker_process/mod.rs pin
//! 259143d2a3465cf8cf90a53dbcd1c9368f8fa3fd8d110c413c11147fb63c7e72.
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
    Some("2b5cce1beec92e6359cd3fe2a129e4c324b520883cfd4c586633e26821b8e9c0");

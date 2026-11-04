//! Trusted test-host provisioning, separate from the source packet it approves.
//! Actual operator guidance supplied at startup of CraftingTable run
//! f7d8e474-bcdc-4b06-92fe-d85f29376b1d on 2026-11-04:
//! "I approve evaluation-only packet
//! 1d13a48427d0f745268c7acdf2fcd58d7c0e34a08df25b7bee9b0124422bf298,
//! including its supervisor disposition, for seven days after acceptance"
//! The receipt identifies that supplied guidance, not a controller-issued
//! decision ID or an independently inferred operator UUID. Its finite expiry
//! conservatively ends 2026-11-11T00:00:00Z, within the accepted seven-day window.
//!
//! Accepted canonical packet:
//! 1d13a48427d0f745268c7acdf2fcd58d7c0e34a08df25b7bee9b0124422bf298.
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
    Some("8e7ad83d9d73852b58ae605035d1e92071427f21e2aec3f43e2b2350d5e11cc6");

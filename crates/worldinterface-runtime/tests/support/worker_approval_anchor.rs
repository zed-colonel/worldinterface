//! Trusted test-host provisioning, separate from the source packet it approves.
//! Actual operator guidance supplied at startup of CraftingTable run
//! 75f8f90f-bd09-4381-a415-b25b2aca3cd7 on 2026-11-05 (UTC):
//! "I accept the packet
//! 8b97e4f4e470390d3c75cd06b43083a92e230d6b462a6640b095084e06492e53
//! through 2026-11-11T00:00:00Z for scoped live verification.
//! I agree with the proposal to accept with the existing fixture limits."
//! The receipt identifies that supplied guidance, not a controller-issued
//! decision ID or an independently inferred operator UUID. Its finite expiry
//! ends at the explicitly accepted 2026-11-11T00:00:00Z.
//!
//! Accepted canonical packet:
//! 8b97e4f4e470390d3c75cd06b43083a92e230d6b462a6640b095084e06492e53.
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
    Some("89e9483bcf1b71b3bf952073dd37ed1aa6ca11fbc57a7ad90387edcccba0c680");

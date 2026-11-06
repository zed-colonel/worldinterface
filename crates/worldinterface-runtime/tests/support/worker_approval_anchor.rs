//! Trusted test-host provisioning, separate from the source packet it approves.
//! Actual operator guidance supplied at startup of CraftingTable run
//! 1e7b982d-a955-4a76-9a0c-06416150879f on 2026-11-06 (UTC):
//! "I approve evaluation-only packet
//! 0746781cd08b460ddd20a45c8c077722eba878dc175677f7d2bf6a135d895cc3
//! through 2026-11-11T00:00:00Z, under unchanged limits: six operations,
//! reviewed nonsensitive fixtures, 15000 ms wall time, 65536 retained output
//! bytes, no ordinary use or production credentials."
//! The receipt identifies that supplied guidance, not a controller-issued
//! decision ID or an independently inferred operator UUID. Its finite expiry
//! ends at the explicitly accepted 2026-11-11T00:00:00Z.
//!
//! Accepted canonical packet:
//! 0746781cd08b460ddd20a45c8c077722eba878dc175677f7d2bf6a135d895cc3.
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
    Some("6dc1c2b73eadbd4007e1fc96fb50ccc726c453396c1de74c3e96e5667b6ecdf3");

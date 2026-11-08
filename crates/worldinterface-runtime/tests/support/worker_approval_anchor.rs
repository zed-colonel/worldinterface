//! Trusted test-host provisioning, separate from the source packet it approves.
//! Acceptance provenance: docs/work-items/WI-WORKER-PROOF-1-correctness.md.
//! Actual operator guidance supplied at startup of CraftingTable run
//! 2ac299ad-eb7e-4bb4-8198-82081edb5e93 on 2026-11-08 (UTC):
//! "I accept packet
//! 997e4cfde46b1075f842f9ea31210c0738eb49714ce1deb4edb40ddb7969ef9f
//! through 2026-11-14T00:00:00Z, for the existing evaluation cases, six operations,
//! reviewed nonsensitive fixtures, 15-second wall limit and 65,536-byte
//! retained-output limit, including the updated supervisor source disposition."
//! The receipt identifies that supplied guidance, not a controller-issued
//! decision ID or an independently inferred operator UUID. Its finite expiry
//! ends at the explicitly accepted 2026-11-14T00:00:00Z.
//!
//! Accepted canonical packet:
//! 997e4cfde46b1075f842f9ea31210c0738eb49714ce1deb4edb40ddb7969ef9f.
//! Prepared and non-live validated on 1f39019 in run
//! e867e801-0e63-4b78-8423-0cb83533a9cb before acceptance.
//! Includes supervisor-disposition.json's exact worker_process/mod.rs pin
//! 663c193d9dadbc371509f40456b39c4d7ae3a086064e2daefa62f3cc3f132f8e.
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
    Some("510cfe0660d025e72331f78490f82a9c8e8f8797d77d1532f0fa85539a41dca2");

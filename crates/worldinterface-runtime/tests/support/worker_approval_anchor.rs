//! Trusted test-host provisioning, separate from the source packet it approves.
//! Acceptance provenance: docs/work-items/WI-WORKER-PROOF-1-conformance.md.
//! Actual operator guidance supplied at startup of CraftingTable run
//! e86cb98a-7a7c-4b9e-8872-811b30720188 on 2026-11-08 (UTC):
//! "I accept packet
//! 8ccf59659186e4b40ea311c208801f82ea072bad9a1685ab60eb3a8d12a57b06
//! through 2026-11-14T00:00:00Z, for the existing evaluation cases, six operations,
//! reviewed nonsensitive fixtures, 15-second wall limit, 65,536-byte
//! retained-output limit, and unchanged supervisor disposition."
//! The receipt identifies that supplied guidance, not a controller-issued
//! decision ID or an independently inferred operator UUID. Its finite expiry
//! ends at the explicitly accepted 2026-11-14T00:00:00Z.
//!
//! Accepted canonical packet:
//! 8ccf59659186e4b40ea311c208801f82ea072bad9a1685ab60eb3a8d12a57b06.
//! Prepared and non-live validated on 372b501 in run
//! 34296bb8-a149-4ccb-aa17-9ebc2a3d7438 before acceptance.
//! All 321 source bindings and non-live build tests revalidated in this run.
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
    Some("e66a415bfbe27acd1f5956a9070c6efe533f273be0b544b2d2f03c72c5fe285c");

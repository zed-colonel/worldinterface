//! Trusted test-host provisioning, separate from the source packet it approves.
//! Acceptance provenance: docs/work-items/WI-WORKER-PROOF-1-correctness.md.
//! Actual operator guidance supplied at startup of CraftingTable run
//! 18358708-10b5-454d-b383-aec924a4fa59 on 2026-11-08 (UTC):
//! "I accept final packet
//! bed4d0afcaecf220ae023f92ac8dd65ce4daf926d530e382f8177ca3d1b6415c
//! through 2026-11-14T00:00:00Z, for the existing evaluation cases, six operations,
//! reviewed nonsensitive fixtures, 15-second wall limit, 65,536-byte
//! retained-output limit, and unchanged supervisor disposition."
//! The receipt identifies that supplied guidance, not a controller-issued
//! decision ID or an independently inferred operator UUID. Its finite expiry
//! ends at the explicitly accepted 2026-11-14T00:00:00Z.
//!
//! Accepted canonical packet:
//! bed4d0afcaecf220ae023f92ac8dd65ce4daf926d530e382f8177ca3d1b6415c.
//! Prepared and non-live validated on 0b0536e in run
//! 2ac299ad-eb7e-4bb4-8198-82081edb5e93 before acceptance.
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
    Some("f1099f59bbf53b4c428ec71f23d76cd58a7befcf5ce5ad66be8e7e6a72b23038");

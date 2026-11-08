//! Trusted test-host provisioning, separate from the source packet it approves.
//! Acceptance provenance: docs/work-items/WI-WORKER-PROOF-1-campaign-scheduling.md.
//! Actual operator guidance supplied at startup of CraftingTable run
//! 43d777a6-3bca-4ad0-8fe7-26e949772c8a on 2026-11-08 (UTC):
//! "I accept the evaluation-only packet
//! 467f4a1ecc63eb6118a565bc8ad05fb5f4066f11b31fe2bf1a92e784c1d42e53
//! through 2026-11-14T00:00:00Z, for the existing evaluation cases and six
//! operations on reviewed nonsensitive fixtures, with eight campaign workers,
//! a 15-second wall limit, 65,536-byte retained-output limit, and unchanged
//! supervisor disposition."
//! The receipt identifies that supplied guidance, not a controller-issued
//! decision ID or an independently inferred operator UUID. Its finite expiry
//! ends at the explicitly accepted 2026-11-14T00:00:00Z.
//!
//! Accepted canonical packet:
//! 467f4a1ecc63eb6118a565bc8ad05fb5f4066f11b31fe2bf1a92e784c1d42e53.
//! Prepared and non-live validated on 30dcc4e in run
//! ca515cbf-da88-446a-bda5-54477141bfb1 before acceptance.
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
    Some("53fea5cfc1557ad719d579466a7a66a40f7796e90b7def1fdcab7993e665824e");

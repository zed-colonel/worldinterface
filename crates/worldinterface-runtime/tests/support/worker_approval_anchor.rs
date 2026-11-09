//! Trusted test-host provisioning, separate from the source packet it approves.
//! Acceptance provenance:
//! docs/work-items/WI-WORKER-PROOF-1-polish-acceptance.md.
//! Actual operator guidance supplied at startup of CraftingTable run
//! ecb0df9d-c727-4519-822a-975654382e17 on 2026-11-09 (UTC):
//! "I accept the evaluation-only packet
//! 9127fbb3da792e3b05f04d0a536cca5a6d6a2d7218d59870a08c4dea747e4049,
//! expiring 2026-11-15T00:00:00Z, for its unchanged cases, six operations,
//! reviewed nonsensitive fixtures, eight campaign workers, 15-second wall limit
//! and 65,536-byte retained-output limit."
//! The receipt identifies that supplied guidance, not a controller-issued
//! decision ID or an independently inferred operator UUID. Its finite expiry
//! ends at the explicitly accepted 2026-11-15T00:00:00Z.
//!
//! Prepared and non-live validated on e2c4486 in run
//! 289fdb83-bd81-4888-bade-7364c3a4edfd before acceptance.
//! All 322 source bindings, canonical packet digest and non-live build were
//! revalidated before recording acceptance. The packet and evaluation scope
//! remain unchanged; only separate trust provisioning and derived hashes change.
//!
//! Evaluation only for this candidate's verification and CraftingTable review:
//! the accepted manifest's exact cases, sources and typed tasks, including the
//! WP-033/034 campaign; provision, start, inspect, terminate, collect-artifacts
//! and release; reviewed nonsensitive fixtures; at most 15000 ms wall time
//! and 65536 bytes retained output. No ordinary use, production credentials,
//! hostile code, same-user isolation claim, publication, integration or gate
//! acceptance. These records encode only that supplied acceptance.
pub const ACCEPTED_RECEIPT_SHA256: Option<&str> =
    Some("6b6de660b7105873e24ee94151c6b5d657a8d00c8e6ebb08c7389e7ecead2441");

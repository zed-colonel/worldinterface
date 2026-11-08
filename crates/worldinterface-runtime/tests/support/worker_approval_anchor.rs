//! Trusted test-host provisioning, separate from the source packet it approves.
//! Acceptance provenance:
//! docs/work-items/WI-WORKER-PROOF-1-simplification-acceptance.md.
//! Actual operator guidance supplied at startup of CraftingTable run
//! 7364b809-5445-4ebd-826a-a03d0180fe47 on 2026-11-08 (UTC):
//! "I accept the evaluation-only packet with canonical digest
//! dd8191f484a42a772e5ade940ec433befa7b1a71ff76cdbcfb2e568fd733a19c,
//! expiring 2026-11-14T00:00:00Z, for its existing cases, six operations,
//! reviewed nonsensitive fixtures, eight campaign workers, 15-second wall limit
//! and 65,536-byte retained-output limit."
//! The receipt identifies that supplied guidance, not a controller-issued
//! decision ID or an independently inferred operator UUID. Its finite expiry
//! ends at the explicitly accepted 2026-11-14T00:00:00Z.
//!
//! Prepared and non-live validated on d02eecb in run
//! 444110b3-eff7-47f7-875f-d703ece6a101 before acceptance.
//! All 322 source bindings and the canonical packet digest were revalidated
//! before recording acceptance. The supervisor exception retains its five
//! source paths and the generic continuation ban remains unchanged.
//!
//! Evaluation only for this candidate's verification and CraftingTable review:
//! the accepted manifest's exact cases, sources and typed tasks, including the
//! WP-033/034 campaign; provision, start, inspect, terminate, collect-artifacts
//! and release; reviewed nonsensitive fixtures; at most 15000 ms wall time
//! and 65536 bytes retained output. No ordinary use, production credentials,
//! hostile code, same-user isolation claim, publication, integration or gate
//! acceptance. These records encode only that supplied acceptance.
pub const ACCEPTED_RECEIPT_SHA256: Option<&str> =
    Some("7446a9150bf4b4ccf17805064c70ef145c3e4c23101e2551908f22226a26c3c3");

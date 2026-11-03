//! Trusted test-host provisioning, separate from the source packet it approves.
//! Actual operator acceptance supplied in this manual run on 2026-11-03,
//! answering run 473d47be, by 0698a39b-dda2-4870-8f3f-97ef94784ae9.
//! The decision identity in the receipt names this provenance; it is not a new
//! controller-issued decision ID. Acceptance expires 2026-11-10T19:00:00Z.
//!
//! Accepted canonical packet (at 70caa54):
//! fcf4f9936abd712244a78f3499c4b76eb7acceb068385e1eda43e84169261ffc.
//! Includes supervisor-disposition.json's exact pins for worker_process/mod.rs
//! (2a2b85dee615c82ac15dcd7d6b2665da4d29657bfb3d7b472705a247ab0a728e)
//! and supervision.rs
//! (1e6acdbf2b57cab161945b14668c68a6ae8ef10861d98671647233f13bc52487).
//! The original five-file exception and generic continuation ban are unchanged.
//!
//! Evaluation only for this candidate's verification and CraftingTable review:
//! WP-017 through WP-026, AB-WI-021 and source-bound WIP-09 regressions;
//! provision, start, inspect, terminate, collect-artifacts and release;
//! only evaluation-request.md's typed tasks; at most 15000 ms wall time
//! and 65536 bytes retained output. No ordinary use, production credentials,
//! hostile code, same-user isolation claim, publication, integration or gate
//! acceptance. These records encode only that supplied acceptance.
pub const ACCEPTED_RECEIPT_SHA256: Option<&str> =
    Some("b1c6f26295a5c1d0472c93460005f185f8c52e06d3aff2d87cb56a9d0c2d6bae");

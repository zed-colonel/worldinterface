//! Trusted test-host provisioning, separate from the source packet it approves.
//! Actual operator acceptance in this conversation on 2026-11-03: "Yes, I accept
//! the revised package." This answers the exact-digest/disposition/expiry question
//! for the revised packet at a7c5231, by 0698a39b-dda2-4870-8f3f-97ef94784ae9,
//! in run a26cf478-833c-49c5-bf03-a9c8c09c0e69. The receipt's decision identity
//! describes this provenance; it is not a controller-issued decision ID.
//! Acceptance expires 2026-11-10T19:00:00Z.
//!
//! Accepted canonical packet:
//! 4602f6c489562f1c73424786ed3180b091b79cfc93e6f51dc242fbddf8aacb53.
//! Includes supervisor-disposition.json's exact pins for worker_process/mod.rs
//! (2a2b85dee615c82ac15dcd7d6b2665da4d29657bfb3d7b472705a247ab0a728e)
//! and supervision.rs
//! (7210b73647db0ade87f0cdf489a901fed9151d52ca2950135bf10526590fb259).
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
    Some("db418c8e7eb60e387a94f3bc3ba1c29a3abfdf5e92c733803590e7e51a162e9f");

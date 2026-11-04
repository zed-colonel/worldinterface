//! Trusted test-host provisioning, separate from the source packet it approves.
//! Actual operator acceptance in this conversation on 2026-11-04:
//! "Yes, I accept the follow-up packet, with unchanged supervisor disposition
//! and evaluation-only scope." This answers the exact digest, disposition and
//! expiry question for candidate d5a654f, by operator
//! 0698a39b-dda2-4870-8f3f-97ef94784ae9, in run
//! 69567943-836b-436e-aa25-c15d8636fb00. The receipt's decision identity
//! describes this provenance; it is not a controller-issued decision ID.
//! Acceptance expires 2026-11-10T19:00:00Z.
//!
//! Accepted canonical packet:
//! fd64db73cb3ab3dc03e731a070c3421778a6908fbada1332ffa33991067798ee.
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
    Some("bf28fe25cdf7f7a0c4f9fb188a37deba9b4b79f189a73acce3c9c237837c58a7");

# WIP-06: ExecutionPlan and pure Flow compiler

Implements the accepted WI-07 domain / WI-ADR-014 design in foundation, retaining
its dependency graph and the WIP-05 registry format. There is one sealed domain
`ExecutionPlan`; protocol plan documents remain untrusted. Direct, Flow and
recovery entry points share validation. `FlowVersion` exposes immutable validated
definitions and revisions. Compilation is I/O-free and AQ-free.

The authoritative profile, bounds, independent Python canonical/hash vectors and
exact test obligation references are in
[flow/README.md](../../conformance/wi-fabric-2/flow/README.md) and
[flow/coverage.json](../../conformance/wi-fabric-2/flow/coverage.json).
The proof checker requires the complete 24-obligation matrix with exact eligible
foundation tests and rejects missing, ignored and ambiguous references.
No cases are assigned to WIP-06; the 55-case registry and AB-WI-005/010/013 remain
owned by WIP-07. Runtime execution, persistence and AQ gate closure are not claimed.

Implementation choices resolve the accepted design's bounded initial profile:

- Models live in protocol and are re-exported by domain; only validated domain
  wrappers are executable/immutable. Shared descriptor and registry validation
  moved to protocol; capability's API and registry digest bytes are preserved.
- The plan origin stores definition ID/version/predecessor/digest; the normalized
  body is stored once. Recovery reconstructs the definition to verify its digest.
  This avoids duplicating the entire definition inside the 64 KiB envelope.
- Explicit expanded nodes implement bounded fan-out. Unsupported dynamic expansion
  rejects. A Boolean branch has exactly two declared labels; guarded producers
  require an explicit exhaustive selection at joins. Resource transforms support
  only provable literals/input passthrough. These choices close ambiguity without
  claiming general-purpose program analysis.
- Expression schemas use a closed, versioned structural subset. Connector ports
  can pin an external artifact schema hash and byte limit for non-JSON payloads.
  Unknown expression constructs/coercions reject. Artifact bytes are never fetched;
  resource artifacts and direct resource-producing outputs require explicit Flow
  declarations instead. Materialized value checks are exposed for WIP-07.
- Full direct submissions are retained separately from plan identity. WIP-07 must
  persist attribution/admission alongside canonical plan bytes/hash; recovering a
  plan alone cannot recreate admission. Execution-relevant boundary policies are
  hashed. Structural authority never becomes a runtime grant or permission.
- Sequential and provably exclusive waits pass. Concurrent waits reject. Repeat
  routes need references to actual handled cursors/fresh generation labels; WIP-08 must bind actual
  handled sequences/generations during lowering. Inclusive deadlines require
  representable UTC second precision and an explicit timeout action.
- `FlowVersion` is immutable, but durable uniqueness, activated-definition lookup
  and predecessor existence checks remain WIP-07 responsibilities.

These choices implement the accepted design; there is no scope expansion into
coordinator execution or durable waits. The legacy AQ/identity-generating
FlowSpec compiler is unchanged and remains outside foundation.

Structural review: 44 new inventory items are explicitly dispositioned to pure
plan contracts/compiler or test evidence. No old item is removed. The connector catalog adds the new target items/surfaces
to its existing infrastructure family with explicit retain treatments; existing
ownership/treatments and adopted check definitions are unchanged. Source hashes
are refreshed after reviewing structure. Independent CraftingTable candidate
review and integration remain due; repository policy settings version 1 reserves
merges to CraftingTable. The source policy observation for this run is
2026-10-15; no remote publication/protection assertion is made.

Verification uses Rust 1.89.0, the supplied external target cache and scratch.
Focused tests cover each required rejection category, direct/Flow equivalence,
recovery with attacker-recomputed hashes, all execution-field tampering,
snapshot drift, resource joins, closed evaluation/replay, exact profile bounds,
immutable revisions and registry compatibility. Three compile-fail documentation
tests cover construction, deserialization and mutation of executable plans.
The checker has an obligation-matrix regression test.

The three repository scripts and every adopted check must pass on the committed
head before delivery. Logs and final recorded results are retained outside Git in
this run's `scratch/verification`; no test output is a source artifact. WIP-07
must persist and verify bytes/hash before execution, use the exact existing
resource-resolution/authorization path immediately before each dispatch, and
record pure evaluation hashes/outcomes separately from boundary receipts.


Review remediation (F-001–F-004): boundary artifact references use the 16 MiB local
publication ceiling or their tighter pinned connector contract, independently of
pure evaluation's 16 KiB limit. Closed boundary JSON and opaque external schema
contracts are supported; opaque contracts still require connector validation at
dispatch, plus scope/integrity and exact authorization in WIP-07. Dependencies
must preserve guards, with an explicit exhaustive-selection exception for joins;
skips settle only after dependencies settle. Independent vectors now include
Direct and guarded resource-producing Flow boundary plans. Evaluation trace
hashes have separate input/output domain tags. Regression tests and exact scope
coverage references accompany all four findings; the case registry is unchanged.
The added artifact schema variant and byte-verification method extend the existing
pure protocol/domain inventory items, retained under their current dispositions;
no inventoried identity, registration site or ownership changes. Source hashes are
refreshed after reviewing these changes.

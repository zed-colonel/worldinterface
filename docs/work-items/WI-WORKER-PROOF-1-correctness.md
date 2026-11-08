# WI-WORKER-PROOF-1 correctness remediation

CraftingTable run `e867e801-0e63-4b78-8423-0cb83533a9cb` addresses the active
whole-candidate review findings F-001 through F-014. Operator guidance requests
all correctness repairs and confirms willingness to renew evaluation acceptance.

- F-001: DELETE-journal connections use SQLite EXTRA synchronization, including
  schema creation. A regression checks every enabled write-store connection.
- F-002: trusted host collection, cleanup and evidence use the immutable admitted
  run and exact operation/artifact scope. Worker-facing credentials retain expiry
  and revocation checks; provisioning and starting still validate their credentials.
- F-003/F-007: capacity is checked before durable reservation, released provider
  state is retired after durable acknowledgment, and never-started runs need no
  export collection. A regression cycles 66 runs through one runtime.
- F-004: maintenance skips foreign scopes and quarantines unpollable local workers
  before continuing AQ advancement.
- F-005/F-006: dispatch before the WI coordinator/case binding waits for another
  evaluation instead of terminally failing valid work. Injected binding failure
  and admission-before-acknowledgment tests exercise the actual AQ path.
- F-008: replay validates worker sequence ordering against the immutable global
  event-source order before rebuilding the shared index from independent journals.
  Live insertion still rejects sequence regression. Rebuild, reopen and offline
  backup are covered with interleaved intake and lifecycle observations.
- F-009: connector-bound effects require production admission preparation.
- F-010: compiled test discovery resolves explicit module paths against their
  containing file and fails if a compiled non-ignored test is absent from syntax.
- F-011: the proof checker validates every foundation coverage obligation against
  compiled non-ignored tests, including obligations not attached to a case. This
  makes compiled evidence authoritative alongside the existing isolation hints,
  without changing adopted check definitions.
- F-012/F-013: mapped ownership tests assert durable quarantine and stop escalation;
  both single-launch tests assert a counter at the actual helper spawn boundary.
- F-014: source changes require a newly prepared packet and renewed finite operator
  acceptance before any positive live evaluation. The old receipt cannot authorize
  the changed packet. No acceptance is inferred from willingness to renew.

The existing supervisor exception remains confined to the same five files. Its
candidate source pin changes only for the process adapter implementation, covering
host authority, active capacity accounting, retirement and the spawn counter. No
executor waiting exception or new isolation claim is introduced. Exact packet
acceptance and independent review remain required.

Verification logs are external under this run's `scratch/verification`. No merge,
push, publication or ordinary-use qualification is performed by this run.

## Exact-packet acceptance and verification continuation

At startup of CraftingTable run `2ac299ad-eb7e-4bb4-8198-82081edb5e93`
(2026-11-08 UTC), the operator supplied this decision:

> I accept packet `997e4cfde46b1075f842f9ea31210c0738eb49714ce1deb4edb40ddb7969ef9f` through **2026-11-14T00:00:00Z**, for the existing evaluation cases, six operations, reviewed nonsensitive fixtures, 15-second wall limit and 65,536-byte retained-output limit, including the updated supervisor source disposition.

This answers the previous run's pending acceptance question and F-014's renewal
request. It supersedes the old packet's evaluation receipt for this candidate.
The receipt and separate trusted test-host anchor encode only this actual
operator guidance. All 321 packet files were checked against the accepted
manifest before recording acceptance; no covered source bytes changed.
The expiry remains fail-closed, including after merge; evaluation after
2026-11-14T00:00:00Z needs renewed exact-packet acceptance.

This continuation runs the non-live builder, all ten adopted checks on the
committed accepted candidate, and verifies the campaign's external handoff.
The campaign includes the previously unexecuted F-002/F-003/F-007/F-012/F-013
worker regressions. Results and log paths are recorded in this run's external
`scratch/verification` directory and final report. This acceptance does not
supply test results, independent review, gate acceptance or integration.

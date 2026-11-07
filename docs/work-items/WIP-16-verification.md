# WIP-16 accepted packet verification

The operator accepted evaluation-only packet
`7d09d13451f0f793e2c04f82ff3eafb3e28e42b788c3d4e7dd9fdb8772d744b5`
through **2026-11-11T00:00:00Z** in the startup guidance for CraftingTable run
`c1d9d28c-0d80-4913-bdd3-777f8b5b0ca0` on 2026-11-07 UTC:

> I accept the evaluation-only packet `7d09d13451f0f793e2c04f82ff3eafb3e28e42b788c3d4e7dd9fdb8772d744b5` through **2026-11-11T00:00:00Z**, for the existing six operations on reviewed nonsensitive fixtures, with 15-second wall and 65,536-byte retained-output limits

All 321 bound files match the exact packet prepared on `7577738`. The
source-bound non-live build and nine non-live adopted checks passed on that
commit in run `5f81f4d2-2cf6-4cb6-9e0f-8ea8f5dee8ab`, before acceptance.
The separate operator receipt and trusted host anchor record this actual
decision. The accepted packet bytes, operations, typed tasks, cases, sources,
environment and limits remain unchanged. This addendum and the derived
inventories are outside that packet's digest graph.

Acceptance authorizes evaluation for this candidate's verification and review.
It grants no ordinary use, production credentials, consumer task acceptance,
parent acceptance, release, remote qualification, strong isolation, integration
or reviewed gate closure. The expiry is less than seven days after actual
acceptance. At expiry the live verifier fails closed, including after merge;
renew exact-packet acceptance before any later live verification.

Run all ten declared adopted checks on the complete committed acceptance, then
generate fresh execution evidence and verify the handoff on the same unchanged
clean head, as described in the
[evidence guide](../evidence/WI-EMBEDDED-WORKER-PROOF-1.md).
This run's `scratch/verification/` retains per-check logs and results, the
campaign receipt, exact-test logs, embedded observations, timing and generated
handoff. Its final verification report records the concrete commit/tree and
outcomes; this acceptance record itself does not assert future passage. Keep
the external evidence with the reviewed source generation because scratch may
expire. Historical WIP-15 results cannot substitute for it.

The G4 and target records are prepared as closed, effective only on
`reviewed-merge-with-adopted-checks`. Fresh passing execution evidence and adopted
checks support independent review; CraftingTable alone integrates reviewed
work. This implementation run does not merge or promote a release.

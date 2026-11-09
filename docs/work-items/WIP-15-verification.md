# WIP-15 accepted remediation verification

## Atomic-observation and F-006/F-007 replacement acceptance

The operator accepted evaluation-only packet
`87f72bf1a2475d82e5c21955920234fddd77bb51f364abd29ddcad2c8d633ad9`
on `a659443` through **2026-11-11T00:00Z**, under unchanged evaluation limits,
in the startup guidance for run `0d74d481-bb67-4da0-9ac8-2494520c0cd5`
(2026-11-07 UTC). All 315 bound files match that exact packet. The separate
operator receipt and trusted host anchor now record this actual decision.
The source-bound build and nine non-live adopted checks passed on `a659443`
in run `193ac5e8-b8bb-481b-ac68-44f8a7ef58c0`; live verification of the
atomic-observation repair remains due on the newly committed acceptance.

This acceptance covers the atomic revision repair and supporting WP-025
regression, F-006's timeout alignment, and F-007's documented expiry consequence.
It does not extend the expiry or authorize ordinary use, publication, integration
or gate closure. WIP-16 retains consumer guidance, final handoff publication and
G4/target closure. The current run's `scratch/verification/` holds the ten
committed-head declared-check logs, results and campaign artifacts; passage must
be established by those execution records on the same unchanged generation.

## F-007: verification deadline and WIP-16 renewal

The current [operator receipt](../../conformance/wi-fabric-2/worker/evaluation-approval.json)
expires at **2026-11-11T00:00:00Z** (`expires_unix_ms = 1794355200000`).
The live fixture host uses `SystemClock` wall time. At that instant and afterward,
the evaluation verifier rejects this receipt: the positive live cases fail and
the adopted `runtime-test` fails closed on any commit carrying it, including
`wi-worker-proof-1` after integration. An earlier passing campaign does not
authorize later execution after expiry.

Complete integration and post-merge `runtime-test` verification before that
deadline. If verification takes place at or after expiry, obtain renewed operator
acceptance before live execution. WIP-16 must prepare and non-live validate its
changed exact-source packet, obtain fresh acceptance with a finite expiry, record
the actual decision in the receipt and trusted anchor, and rerun every adopted
check on its committed head. Carry this deadline and fail-closed consequence into
WIP-16's consumer evidence and handoff instructions. No test-clock substitution
or extension of the existing acceptance is part of this documentation fix.

This addendum is outside the packet. It records the operational consequence
alongside the [WIP-15 note](WIP-15.md) and
[active ledger](wi-worker-proof-1.md#wip-15-campaign-candidate). Their current
packet-bound corrections and the campaign repair require renewed acceptance.

## F-006: supplemental timeout alignment

The adopted campaign entry point uses **1785 seconds plus 15 seconds kill grace**
after F-004's fix in `9396232`. This remediation aligns the supplemental
[`worker-proof-evidence.sh`](../../scripts/worker-proof-evidence.sh) with that
same guard and corrects the ledger's earlier 1500-second bound. The ledger also
records F-004's deadline change, F-005's corrected cold-evidence run and the
accepted replacement below.

These packet-bound edits join the atomic-observation campaign repair described
in [WIP-15](WIP-15.md#campaign-follow-up-atomic-worker-observations). Fresh
exact-packet acceptance is required before live verification, including when
made before the present receipt expires. The source-bound build and non-live
checks can validate the candidate in advance of that decision.

## F-004/F-005 replacement acceptance

The operator accepted evaluation-only packet
`b24ac0e082751f2f0711bf5a07c6ef69f0b8c2cf5115d9c4241a3c389cf9a6a7`
through **2026-11-11T00:00:00Z** in the startup guidance for run
`782e12b7-d455-4025-9daa-93c120543783` (2026-11-07 UTC), under the unchanged
[evaluation limits](../../conformance/wi-fabric-2/worker/evaluation-request.md).
The source-bound non-live build and nine non-live adopted checks had passed on
`9396232` in run `9bc6c5ba-dd7e-413c-b11b-6f528d8bd96c`. All 315 bound files
matched the accepted packet at that acceptance. The current candidate's
changed source requires renewal. The separate receipt and trusted host anchor
record this actual replacement acceptance; the earlier results below remain
historical. The committed-head results for this acceptance are recorded in that
run's `scratch/verification/adopted-checks.json` and `wip-15-status.json`, with
per-check logs and the runtime campaign's receipt, handoff candidate and timing
breakdown. Those execution records must identify the same unchanged generation
and report passage before the replacement's exit gate is met. G4 and target
closure remain with WIP-16.

## Historical cost-remediation acceptance

This is the execution addendum to [WIP-15](WIP-15.md). The packet-bound item
note and ledger preserve the request and earlier evaluation history. This
addendum records the subsequent operator acceptance and measured cold result
without changing that accepted packet's source bytes at the time. The later
F-004/F-005 remediation in the item note replaces the packet-bound timeout and
corrects its evidence pointer; the measurements below remain historical.

On 2026-11-06 the operator accepted evaluation-only packet
`289f6efacf3378ba4e955e2164d6ea0f3bc80f8e1b76b07293d3bef6d1cd07d6`
through **2026-11-11T00:00:00Z**, under the unchanged limits in
[evaluation-request.md](../../conformance/wi-fabric-2/worker/evaluation-request.md).
Run `e73756a8-8e69-4f9a-acb7-ba3074a3dbba` supplied that actual decision.
Commit `555a8b09478ed96c0d16c1d8c2ed971c8ee48e44` records it in the separate
approval receipt and trusted anchor. All 315 packet-bound files matched then.
No ordinary use, publication, integration or gate closure is authorized.

## Cold timing and F-001

The uncontended cold run used this worktree's supplied external
`CARGO_TARGET_DIR`. `cargo clean` removed the build artifacts, then returned
EBUSY at the protected `.git` mount; `debug`, `release` and `tmp` were verified
absent before execution. Protected mounts were left intact. Dependencies were
already fetched; Cargo ran locked and offline. No other campaign or check ran
concurrently. The complete runtime test command had a hard 1800-second timeout.

Measured on clean commit `555a8b09478ed96c0d16c1d8c2ed971c8ee48e44`, from
2026-11-06T23:08:12Z to 23:30:42Z:

| Phase | Seconds |
|---|---:|
| Initial runtime test build | 124.00 |
| Nested tool build | 9.61 |
| Campaign discovery, remaining builds and listings | 135.84 |
| Registered campaign tests (224) | 623.49 |
| Additional runtime suite tests (146) | 386.27 |
| Receipt and handoff validation | 68.62 |
| Campaign total, including its generation checks | 1215.31 |
| Complete Cargo invocation, including four doctests | **1349.90** |
| Headroom under unchanged 1800-second check limit | **450.10** |

The campaign discovered once, passed **224/224 registered tests**, retained and
passed **146/146 additional runtime tests**, and passed **55/55 cases**. Cargo's
four compile-fail doctests also passed. Receipt generation validated the unchanged
clean generation, exact logs and executable pins without another discovery.
The then-1500-second campaign timeout plus 15-second kill grace fit within the
adopted check's unchanged 1800-second limit. This provides cold evidence for
F-001's cost remediation; it does not replace the declared adopted checks.

Receipt SHA-256:
`939d9d15aafce99f95fc9d7892256c34edd04f5f36ae3432a02cfbc96a782e26`.
The receipt binds the actual Linux/OS and execution identity, all six crates.io
AQ 0.3.0 checksums, applied and observed controls/limits, cleanup, individual test
outcomes and all 55 cases. Unknown seam controls remain null; the asynchronous
fixture remains explicitly a seam simulation.

## Review findings and retained work

- **F-001:** sole runtime test ownership, one discovery and the bounded campaign
  are implemented in `e60cbd7`/`c52c170`; measured cold headroom is recorded above.
- **F-002:** the packet-bound item note and active ledger record the prior
  `4dcbe731…` acceptance, its passing campaign receipt and timeout/retry history.
  This addendum records the accepted replacement and its cold execution.
- **F-003:** the generated handoff regression passed in the campaign. It rejects
  substituted producer pins, fixture/observation bytes, inflated claims and
  missing embedded evidence using the payload validator shared by handoff checks.

Eight gates remain closed. `WI-WORKER-G4` and
`WI-EMBEDDED-WORKER-PROOF-1` remain open for WIP-16, which retains consumer
instructions, final handoff publication and closure. WP-033 permits only the
partial milestone; no consumer acceptance, parent acceptance, G5, remote parity,
release, ordinary use or strong isolation is asserted.

## Reproduction and evidence

Reproduce the runtime command from the repository root, with the supplied
external scratch and build-cache environment:

```sh
timeout --kill-after=20s 1800s env PROPTEST_DISABLE_FAILURE_PERSISTENCE=1 \
  cargo test --locked --offline --manifest-path crates/worldinterface-runtime/Cargo.toml
```

The external evidence is under run
`e73756a8-8e69-4f9a-acb7-ba3074a3dbba`'s `scratch/verification/`:
`cold-runtime-timing.json`, `cold-runtime-test.log`, and
`wi-worker-campaign-5RtFqj/{timing.json,receipt.json,handoff-candidate.json}` plus
individual logs and observations. `run-cold-runtime.py` records the invocation
and timing. The final committed-head declared checks are recorded separately in
`adopted-checks.json` and the ten named check logs. Each declared check is invoked
with `ct-check --declared ID`; supplemental execution is not counted as adoption.
Scratch execution artifacts may expire and are not committed source. The phase
timings and receipt digest above preserve the measured result in the repository.

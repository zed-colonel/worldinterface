# WI-WORKER-PROOF-1 campaign scheduling remediation

CraftingTable run `ca515cbf-da88-446a-bda5-54477141bfb1` addresses F-016.
The adopted runtime-test previously assigned each executable's entire test list
to one of two workers and ran registered and remaining tests in separate phases.
One counted run timed out at 1800 seconds; successful campaigns took 1480–1606
seconds on the unchanged predecessor.

The campaign now schedules individual exact tests on eight workers, bounded
independently of the number of executables. Registered cases and the remaining
runtime tests share that pool. Every executable is hashed once before any test
starts and once after all workers join. Each outcome retains its executable pin,
exact harness log and digest, with deterministic result ordering. Registered and
remaining outcomes are partitioned for the existing independent coverage and
receipt checks. No test is dropped, ignored or converted to definition evidence.
The 600-second individual-test timeout and 1800-second adopted ceiling remain.
Timing now reports `campaign_execution_seconds` and `execution_phases: 1`.

The new non-live regression requires eight exact tests from one executable to
reach a barrier before any can finish; create-once markers reject duplication.
It checks every result and log. Existing tests still cover child failure,
ignored/missing tests, incomplete outcomes, log tampering and executable
replacement during execution or receipt validation.

All changes are limited to campaign orchestration, its regression and evaluation
request/derived records. Production runtime, case coverage, fixture tasks, six
operations, per-run limits and supervisor disposition are unchanged. The changed
source bytes require a new exact evaluation packet, non-live build and actual
finite operator acceptance before positive live verification. The old receipt
and trusted anchor remain unchanged until that decision; no approval is inferred.

Verification logs and packet measurements belong under this run's external
`scratch/verification`. Independent review, counted runtime-test timing on an
accepted committed head and CraftingTable integration remain required. No merge,
push, publication or ordinary-use qualification is performed here.

## Exact-packet acceptance and live verification continuation

At startup of CraftingTable run `43d777a6-3bca-4ad0-8fe7-26e949772c8a`
(2026-11-08 UTC), the operator supplied this decision:

> I accept the evaluation-only packet `467f4a1ecc63eb6118a565bc8ad05fb5f4066f11b31fe2bf1a92e784c1d42e53` through **2026-11-14T00:00:00Z**, for the existing evaluation cases and six operations on reviewed nonsensitive fixtures, with eight campaign workers, a 15-second wall limit, 65,536-byte retained-output limit, and unchanged supervisor disposition.

This answers the previous run's pending question. The packet was prepared and
non-live validated on `30dcc4e` in run `ca515cbf-da88-446a-bda5-54477141bfb1`.
This continuation independently matches all 321 source bindings and the canonical
packet digest before recording the separate acceptance receipt and trusted host
anchor. The receipt identifies the supplied guidance; it invents no controller
decision ID or operator UUID. Its finite expiry is less than seven days after
acceptance. Changed bindings or expiry continue to deny live evaluation.

Only trust provisioning, this note and the derived source inventories change;
the accepted packet bytes and scope stay exact. The continuation revalidates the
non-live build and runs all ten adopted checks on the committed accepted head,
including the counted runtime campaign timing needed for F-016. Results and logs
belong in this run's external `scratch/verification` and final report. Acceptance
supplies no passing result, independent review, integration or gate closure.

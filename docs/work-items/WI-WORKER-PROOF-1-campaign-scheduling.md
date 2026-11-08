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

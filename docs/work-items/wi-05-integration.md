# WI-05 integration candidate

This slice originally bound effect completion to the historical AQ baseline implementation
`29e096c4f06985d1a9bd9d5fe5a3a6a5db8a6860`, tree
`1c0c85dd700bd9d0c75de223dada911ffeadbb5b`, and conformance revision 16.
The current runtime uses published crates.io AQ =0.3.0 under the WIP-07 operator
instruction; the adopted runtime graph check
verifies the published checksums. The selective takeover is recorded in [WIP-04](WIP-04.md).

AQ-ADR-012 at that pin specifies that an interrupted accepted attempt counts
against `max_attempts`. `AQ-BASELINE-ALIGNMENT.md:27` describes interruption
and failure accounting as distinct; the pinned AQ source governs this precise
point. The operator approved changing the previously G2-frozen WI effect
adapter to `max_attempts=3` and `SafetyLevel::Idempotent` in decision cycle
`dd3d1012-2c57-4307-bec7-f51a41b02c69`. The persisted admission request is
never rewritten. WI's durable `Start` fences a second port invocation, and
AB-WI-002/003/004 are registered and rerun against these new request bytes;
the old-policy persisted-request regression remains required. AQ's default
backoff is fixed at five seconds. Three interruptions can still exhaust the
budget; a terminal AQ/Fabric mismatch is an inspectable convergence fault,
not a successful completion.

The adopted repository policy is revision 1, branch settings revision 1.
It uses controller-local exact-commit review and controller-mediated
integration. Remote protections remain the operator's obligation before the
first remote publication and final release approval. This candidate does not
claim WI-AQ-G4, distinct WI-ADR-009 security review, merged-tree verification,
or WI-05 parent acceptance.

WI-05 owns the completion publication and outcome/receipt transaction. WI-06
supplies a real `EffectExecutionPort`; source tests here use named mocks. The
signing hook stores detached provenance. WI-12 supplies signing providers and
key lifecycle.

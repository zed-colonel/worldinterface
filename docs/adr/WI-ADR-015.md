# WI-ADR-015: Flow wait/timer lowering to `AQ-CONT-1`

Status: accepted

Owner: operator

Due: WI-08

Source: implementation plan §4.

Decision evidence: [recorded operator approval](approvals/WI-ADR-015.json), approval `3df98058-4a86-4faf-86be-26f267301fb3`, 2026-09-29T16:25:04.705Z. Decision packet SHA-256: `bf607924a8fe5450dd28a2dff5452d454ed9b0bf1ee077671a8d7cb257c51334`.

Coverage: full.

WorldInterface shall lower Flow waits to AQ-CONT-1 Awaiting dispositions with one valid WaitSpec per run, a durable coordinator checkpoint, and resumed coordinator evaluation. Static Flow dependencies remain AQ DAG dependencies. External callbacks, approvals, reconciliation outcomes, and worker events first commit an authoritative WorldInterface record and its signal-outbox obligation, regardless of whether a waiter exists; the outbox admits a stable, deduplicated AQ signal carrying bounded routing and an opaque record reference. Early-event waits use an exact correlated AnyRetained filter; repeated waits advance the handled SignalSequence or use a fresh correlation generation. WorldInterface performs principal authorization and reloads authoritative state after every wake. Pure timers use AQ scheduling or an inclusive absolute deadline with an explicit timeout action; no delay connector or executor sleep implements waiting. A signal/deadline race follows AQ's first committed eligible resolution. Resume delivery, including retry and recovery, is handled idempotently across signal, children, deadline, control, and administrative forms. AQ resolution never independently establishes a Flow business outcome or external effect success. Remove signal.await, signal.emit, SignalRegistry, and delay from the target runtime and public capability catalog.

Rationale: I agree with this recommendation. While "richer" waits could be useful, the introduction of more complex recovery and race mechanics is not worth it in my opinion.

Retained obligations: Implement and verify WI-08 against the exact AQ pins, including WI-AQ-G3/G4, AB-WI-008 and AB-WI-011, race/crash/negative tests, exact-candidate review and integration, parent acceptance, and later release and remote-publication obligations under repository policy revision 1.

Consumer applicability: []

Alternatives: Richer waits with additional race mechanics, executor sleeps and consumer-local wake authority are excluded.

Invariant impact: One durable AQ wait per run and authoritative WI event commit before wake preserve restart and race semantics.

Verification: design authority only; implementation and adopted-check passage remain with the mapped WIP scopes. Approval establishes no test result.

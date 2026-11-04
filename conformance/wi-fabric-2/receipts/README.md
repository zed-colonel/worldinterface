# WI-05 receipt and evidence profile

The independent foundation proof supplies the receipt contract. WI-05/integration
adds the runtime completion adapter and indexed ingress inspection. WI-06 supplies
the production connector port. The fresh local store profile is `local-worker-11`;
earlier profiles reject unchanged.

Receipt import requires exact `wi-json-1` bytes, supported profile and a stable
ID in the digest. Artifact hashes cover exact bytes. Signing proofs and proof
bundles are separate immutable records. The only receipt constructor consumes
an inspected authoritative effect aggregate, execution record and exact
authorization snapshot. Generic batch append rejects boundary receipts.

Context records a scoped publication identity before writing content, then
advances durable and available states. Fabric commits the guarded receipt with
the effect outcome and outbox in one local transaction after checking all
required output references. Reopen, replay and backup validate both journals
and cross-store references. A staged identity or durable file alone never
establishes execution success. Possible external commit without durable outcome
becomes an explicit Uncertain receipt on terminal-run recovery or later AQ
delivery. A durable Start prevents a second provider call.

WP-015 and WP-016 have fixture-backed reports that bind the supplied
`WI-WORKER-EXECUTION-1` Markdown definition SHA-256
`9f597cffb7117dc679e9f001c25c7d54f2a6157f6ead563b0c7cbd11f45a04f9`
and YAML definition SHA-256
`6523baf56d14dafa4b199c7493dc030461cc061754c180b4c83006308e633986`.
The current supplied `WI-EXO-WORKER-HANDOFF.md` lists these hashes; earlier
imported handoff material used older hashes. Enclosing-instance provenance is
separate from worker enforcement. Requested, advertised, applied, observed and
missing controls remain distinct. Incomplete reports are retained even when
eligibility is denied. WIP-04 resolves WP-A06 at receipt inspection with reports bound to immutable
receipt evidence and checked against the authoritative source. Both cases retain
their domain mappings and add runtime receipt tests in completion/coverage.json.

The adopted repository policy is version 1, branch-settings version 1:
controller-local exact-revision review, controller-mediated integration and
the recorded experimental freeze. Remote protections remain operator-owned
before first remote publication and final release approval. ADR-008 and
ADR-009 are fully approved architecture, not test results. The active plan closes WI-05 and WI-WORKER-G2/WI-AQ-G2 through WIP-04 reviewed
integration with adopted checks. WI-AQ-G4 remains owned by WIP-07. Qualifying
independent review, its security pass and merged-tree checks remain controller
obligations; implementation self-review is recorded separately.

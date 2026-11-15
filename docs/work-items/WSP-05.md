# WSP-05: prepare 0.2.0-alpha.2 with the supervisor boundary

Accepted plan: WI-SUPERVISOR-PROOF-1-PLAN-1, WSP-05; predecessor WSP-04.
The eight new-runtime crates and exact internal requirements advance together to
0.2.0-alpha.2. External dependencies, crates.io AQ 0.3.0 and the legacy workspace
are unchanged. The WSP-02 prepare/verify route remains authoritative.

The descriptor selects release notes by runtime package version and identifies
alpha.2 as the first release carrying WI-WORKER-G5. Its separate six-case
supervisor section and all eleven packaged gate records preserve the original
55-case, ten-gate worker milestone. G5 covers authenticated interface and durable
reconciliation; actual Kata lifecycle and deployment qualification remain
Exoskeleton-owned.

The packaged descriptor and external consumer handoff carry identical two-way
store compatibility metadata: alpha.1 stores must be recreated under alpha.2;
stores opened by alpha.2 cannot be read by alpha.1. No migration or rollback
compatibility is provided. This includes WSP-03's effect_instances, effect_owner,
effect_requests and worker_reserved changes and WSP-04's flow_pending_global
index. Release notes compare against WSP-02 merge 55c8020 and cover the public
supervisor, ledger/read-set, storage scheduling, diagnostics and clock APIs.

Preparation requires an explicit existing writable external TMPDIR and refuses
missing, empty or unusable values before preparation side effects. Supplied
temporary and build directories remain in use with existing canonicalization.
Regressions exercise the CLI's actionable refusal, ordering, canonical paths and
missing/altered G5 identity and both store compatibility directions.

Refresh order: lock snapshot and sources, inventory hashes, handoff generation
into scratch and both descriptor copies, inventory hashes again, complete
evaluation manifest last. This item adds no inventoried Rust surface. The explicit
structural disposition additions are the alpha.2 notes and this work item as
fabric-support removal surfaces because they reference SupervisorConnector; retain
the upgrade evidence and review any future replacement selectively. No removal or replacement is
claimed. New documentation is bound by the handoff/evaluation packet. Candidate
execution requires the committed manifest and all source bytes to match HEAD exactly.

The first candidate c8f6835 passed nine adopted checks but failed
`new_flow_progress_and_attempts_are_independent_of_waits_and_history_tenants`
in the runtime campaign (waiting=0, history=100, no restart). Its unchanged binary
passed alone in 23.6 seconds with four or five ticks and two/two attempts. The
fixture's fixed 10-ms sleeps could count polls before asynchronous execution
finished under the campaign's general pool. The retained failure bundle is part
of the external verification record; the standalone pass does not clear it.
The fixture now uses one blocking worker and awaits a FIFO completion marker
between ticks, including a deliberately queued 250-ms worker occupancy. It preserves
the eight-tick, two/two-attempt, history/restart and reconciliation-row assertions.
No production code, timeout, campaign coverage or adopted definition changes.
This test-only correction is the code-forced deviation from release-only edits.
The carried F-007 case passed in that failed campaign; no recurrence was observed.

Verification logs, timings and archive evidence are retained externally under
the implementation run's scratch/verification; the implementation handoff names
the exact commit, manifest and SHA-256. All ten declared adopted checks must pass
on that commit, followed by prepare, verify and offline crate-release plan.
Any F-007 recurrence requires retained-bundle diagnosis and cannot be cleared by
a passing rerun. Tests grant no review, integration, publication or closure.

Preparation must be repeated at the eventual reviewed merge commit before the
operator's remote-protection, tag/push, publication and EXV-27 consumer handoff
steps. WSP-05 performs none of those actions. Only CraftingTable integrates.

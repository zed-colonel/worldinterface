# WI-WORKER-PROOF-1 selected polish batch

The operator selected F-042 through F-050 for finalization run
289fdb83-bd81-4888-bade-7364c3a4edfd, with the guidance: "I think all the polish
items are worth doing." This note records implementation claims for independent
review. The historical ledger and all other work-item names retain their paths.

| Finding | Implementation |
|---|---|
| F-042 | Add `scripts/wi_worker_evaluation.py` to handoff reproduction identities and the existing stale-source rejection test. |
| F-043 | Resolve the build recipe's checkout from its own path, use absolute helper paths and isolated Python, and append the helper import directory after the standard library. |
| F-044 | Describe runtime AQ 0.3.0 and distinguish the contract's historical 0.2.0 baseline. |
| F-045 | Use the adopted runtime format command in local integration preparation, covering nested modules and binaries. |
| F-046 | Explain rejected in-worktree build and scratch directories on stderr. |
| F-047 | List every checker subcommand and check scope; malformed commands, unknown scopes and unknown campaign verbs return that usage. |
| F-048 | Point consumers to the approval receipt for the current packet and expiry, retaining drift, revocation and expiry rejection. |
| F-049 | Explain why the shared control-plane fixture permits unused helpers. |
| F-050 | Rename `wip-01.md` to `WIP-01.md` and update both ledger references. |

## Verification and acceptance

Verification logs belong in this run's external `scratch/verification` directory.
Check script isolation from an unrelated working directory with shadow modules;
check both directory rejection diagnostics; exercise malformed checker commands;
and run the existing handoff source-drift regression including the added helper.
Refresh inventory source hashes, regenerate the handoff descriptor, refresh the
evaluation manifest and run the non-live builder on the exact replacement packet.
No inventory disposition or adopted check definition changes are required.

The prior approval binds the previous packet only. The selected polish decision
does not accept the replacement packet's digest. Obtain actual operator acceptance
after non-live validation, with expiry at most seven days after acceptance, before
positive live verification. Record only that actual decision and its trusted host
anchor; then run all ten adopted checks on the resulting committed head. Until
then the live runtime check remains pending and the finalization exit gate is
unmet. No qualification, gate closure, publication or integration is authorized
by this note. There are no additional implementation deviations or follow-ups.

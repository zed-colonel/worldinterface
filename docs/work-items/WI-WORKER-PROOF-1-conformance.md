# WI-WORKER-PROOF-1 conformance remediation

CraftingTable run `34296bb8-a149-4ccb-aa17-9ebc2a3d7438` addresses the sole active
finding F-015 against candidate `f86667bc` and destination main `f48df82e`.
The imported work items and all twenty adopted conformance obligations retain
their IDs and requirements. AQ execution remains crates.io 0.3.0 under the
recorded WIP-07 operator decision; the imported 0.2.0 baseline stays historical.

## F-015 implementation

WP-028 now registers two additional non-ignored runtime tests in both
`proof/cases.json` and the authoritative `worker/artifact-seam-coverage.json`:

- `wp_028_unsafe_live_exports_quarantine_and_report_through_real_wi_aq` starts
  the reviewed transform process through EffectService, the production executor
  on AQ and the durable outbox. After confirmed successful exit it replaces the
  actual export with a symlink, hardlink or FIFO. The symlink and hardlink target
  contains the expected bytes, isolating link safety from digest rejection.
  CollectArtifacts must yield Uncertain and a WI collection report with code
  `provider-collection-rejected`.
- `wp_028_unavailable_seam_export_quarantines_after_provider_ack_through_real_wi_aq`
  uses the existing asynchronous provider, explicitly labelled seam simulation.
  After execution it discards its artifact data while retaining the admitted
  manifest and collection acknowledgment. This reaches the connector's
  `provider-collection-unavailable` branch through a failed scoped artifact read.
  It claims no live process, remote parity or isolation for this branch.

Both tests check exact report binding, operation, policy and reason; unacknowledged
WI collection; quarantined run and durable stop request; retained capacity;
absence of completion and of public/scoped readable outputs; and authoritative
worker events with durable outbox obligations. The exact report survives Fabric
projection rebuild and store reopen. The process test checks that reopen spawns
no replacement. No direct adapter call or direct quarantine write establishes
these outcomes: the tests submit CollectArtifacts through real WI/AQ.

Production runtime and process supervisor source, supervisor disposition,
fixture tasks, evaluation cases, supported operations and limits are unchanged.
The test-only seam adds a bounded data-loss setup method. Inventory/hash and
pinned handoff refreshes preserve existing structural dispositions.

## Verification and approval boundary

The new tests compile without execution. Complete source-bound non-live packet
validation and adopted checks are recorded under this run's external
`scratch/verification`; the final report records their actual results and the
canonical prepared packet digest. No live passage is asserted by compilation.
The old accepted packet `bed4d0af...` cannot authorize these changed test bytes.
Its receipt and separate trusted anchor remain unchanged until actual operator
acceptance of the replacement, with expiry at most seven days from acceptance.
The exact request is `conformance/wi-fabric-2/worker/evaluation-request.md`.

Live WP-028 tests and the complete adopted runtime campaign remain due after
acceptance, on the committed accepted head. Required checks, independent review
and reviewed integration remain separate obligations. This run does not merge,
push, publish or enable ordinary worker use; no optional follow-up is implemented.

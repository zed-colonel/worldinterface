# WI-WORKER-PROOF-1 selected simplification batch

The operator selected F-017 through F-041 for finalization run
444110b3-eff7-47f7-875f-d703ece6a101, with the guidance that simplification is
important for a foundational component. All 25 findings are simplification
findings. This note records the implementation claims for independent review.

| Findings | Implementation |
|---|---|
| F-017 | `WorkerConnector::recovery_state` shares the loaded and validated state between pre-check, reconciliation and cancellation. Reconciliation still reauthorizes after possible quarantine. |
| F-018 | One local preflight closure checks registration and current control authority immediately inside each provider fence. It borrows only the needed fields so authorization can still be consumed once. |
| F-019 | `artifacts::bound_state` centralizes snapshot lookup and complete binding equality across worker modules. |
| F-020 | Recovery evidence, cleanup and polling rely on catalog recovery authorization for the existing scope/binding checks. |
| F-021 | Remove the three unused process facade methods and fold the two remaining host-only implementations. Other host/read authorization parameters retain their original behavior. |
| F-022 | Cache source-derived connector and process-adapter identities with `LazyLock`; reuse one descriptor list per admission. Source edits still change implementation identities and require a new packet. |
| F-023 | Minting and verification share process credential issuer/audience constants. |
| F-024 | Shared outcome evidence and receipt-command constructors retain preview validation, publication, transactional validation and temporal evidence. |
| F-025 | Remove the consecutive duplicate invocation fence; retain the current-state check and later authorization fence. |
| F-026 | Completion and reopen convergence share failure codes and AQ terminal-state methods. |
| F-027 | Share artifact/DataRef conversions. A typed structural locator error preserves Flow's permanent/retryable split, including the original conversion order. |
| F-028 | Flow nodes and coordinator share the binding-ready wait constructor. |
| F-029 | Inspection shares its full-disclosure gate and principal-scoped snapshot lookup; worker controls reuse the authorized evidence path. Completeness checks log policy before loading the snapshot and retains its journal timestamp. |
| F-030 | Embedded dispatch aliases the existing reconciliation router; remove unused TaskRouter. |
| F-031 | Generate all 16 UUID and two bounded opaque types with the already inventoried `define_id` macro shape, preserving derives, validation and serialization. |
| F-032 | Canonical data/collection digests live in protocol; `ingress::digest` remains a re-export for callers. Record and collection limits remain distinct. |
| F-033 | Storage shares strict record and collection decoders; Flow's integrity-specific decoder remains separate. |
| F-034 | Remove unused legacy worker descriptor validation; SPI validation remains in use. |
| F-035 | Remove the unused synthetic process-state placeholder and its synthetic test. Real runtime ownership checks and recycled-PID/cleanup tests remain authoritative. |
| F-036 | Coverage loading uses ordered loops over the same path lists. |
| F-037 | Share pure-domain validation, exact-reference predicates and fixture agreement; preserve each caller's original duplicate policy and allowed packages. Hoist invariant fixture/ADR reads. |
| F-038 | Proof discovery and campaign share adopted harness configurations; handoff shares manifest constants. Cache executable digests through one helper. |
| F-039 | Remove unused campaign candidate wrapper. |
| F-040 | Derive accepted ADR status from the pin table, share pins and read each page once; retain ADR-016's additional provenance checks. |
| F-041 | Evaluation preparation/build share literal UTF-8 canonical encoding and drift validation through `scripts/wi_worker_evaluation.py`. |

## Explicit inventory disposition changes

The selected F-030/F-031/F-035 work revisits the affected retain-align dispositions:
remove TaskRouter and the superseded process-state module, map Dispatcher to the
existing router alias, and map the 18 identity struct entries to generated IDs.
Add inventory entries for the macro and shared completion/locator declarations.
These changes preserve their previous target ownership and catalog family. Other
families, destinations, registration sites and treatments remain unchanged.
Regenerate current inventory and refresh source hashes only after recording these
structural changes. The process-supervisor exception keeps the same five source
paths; its exact pins change only for the selected facade/cache/credential edits.

## Verification and acceptance

Focused completion crash, regression and receipt-inspection suites passed (6,
24 and 8 tests). Foundation and runtime Clippy passed during implementation.
The UTF-8 packet probe verifies the shared encoder against literal expected bytes.
Run all ten adopted checks on the final committed candidate and retain their
results in this run's external `scratch/verification` directory.

Prepare and non-live validate the final evaluation packet before requesting
actual operator acceptance. Existing acceptance cannot authorize the changed
packet. No approval, qualification, gate closure or release is asserted by this
note; live verification requires exact packet acceptance with finite expiry at
most seven days later. Final review and integration remain with CraftingTable.

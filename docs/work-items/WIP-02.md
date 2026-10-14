# WIP-02 — WI-04 durable effect admission verification

Implementation assessment of inherited merge `bd884be` (six commits, 33 files,
+3,513/−243), against the WI-04 parent source exit gate and ADR-004/005/006/007.
Baseline: `ddf66acc77708f5b91d3dd795f085b5ed9972f55`, after WIP-01.
Independent CraftingTable review, integration and merged-head verification remain
due; this document supplies implementation review evidence.

## Scope and assessment

The review covered `runtime/src/{admission,aq_adapter,authority,egress,resolution,
run_discovery,storage}.rs`, domain agency/resource/authorization/effect reducers,
guarded egress/storage journaling, both admission test files, canonical vectors,
and the merge's conformance, dependency, contract, inventory and documentation
changes. WIP-01's crates.io baseline replaces historical map-era launcher/archive
verification while preserving the accepted AQ source identity.

WI-04's in-scope source exit gate is supported: durable intent precedes resolution
and dispatch, exact authorization is server-owned and explainable, and G2 admission
conformance has executable evidence on AQ 0.2.0. No blocking or major implementation
finding remains open. Both G2 gates remain open: WIP-04 owns closure and reruns
AB-WI-002/003/004 after its request changes. The source milestone stays WI-01;
historical deletion obligations remain milestone-gated.

Trusted Authentication, EvidenceAuthority, ResourceObserver and TrustedClock
implementations are injected host boundaries. These tests prove runtime admission,
not real provider qualification. Credential middleware, daemon composition,
provider execution and receipt publication retain their later owners.

## Six source-required tests

Names are exact registry references. Runtime manifest:
`crates/worldinterface-runtime/Cargo.toml`; package: `worldinterface-runtime`.
Every listed test is non-ignored and executed by adopted `runtime-test`.

| WI-04 required test | Registry case and test | Assertions reviewed |
|---|---|---|
| Crash after intent commit, before admission | AB-WI-002: `ab_wi_002_subprocess_crash_cuts` | Real subprocess exits after intent commit; reopen sees Submitted, recovery creates exactly one task with one logical use. |
| Crash after AQ admission, before local acknowledgment | AB-WI-002: `ab_wi_002_subprocess_crash_cuts`; `ab_wi_002_003_004_persist_retry_ack_and_discover` | Hard exit skips shutdown/acknowledgment. Recovery returns AlreadyExists for the persisted request; task/key/digest/sequence identities and one logical use survive retry. |
| Same client request, different payload conflicts | AB-WI-002: `production_facade_authenticates_verifies_and_rechecks_live_target` | Changed timeout with the same client request is rejected; identical resubmission returns the original intent. |
| Caller actor/principal/authorization fields cannot override authentication | WP-011 and AB-WI-002: `caller_claims_cannot_override_authenticated_agency_or_grants`; `production_facade_authenticates_verifies_and_rechecks_live_target` | Actor, accountable principal and capacity forgery fail before intent/resolution. Unknown grant/approval claims reach authorization and deny solely Grant/Approval. Invalid sessions never establish identity. No AQ change or use. |
| Alias, symlink, redirect, DNS, audience and tenant ambiguity fail closed | AB-WI-002: `production_facade_rejects_resource_boundaries_before_admission`; `live_ambiguity_after_intent_never_reaches_aq` | Duplicate selector facts/registry entries, traversal, private/metadata addresses, redirects, tenant mismatch and audience widening fail. Valid filesystem/network/audience targets first authorize, then live replacement blocks delivery. AQ projection and use counts stay unchanged. |
| Self-initiated purpose follows external authorization | WP-009 and AB-WI-002: `self_initiated_and_external_requests_follow_the_same_authorization_path` | Both requests resolve and reach the same five-stage attenuation trace. Exact authority allows one task; removed platform authority gives solely ExactAuthority denial with no admission/use. |

## Worker runtime evidence and WP-A02

The registry marks nine WIP-02 cases implemented, retains all 35 authoritative
`egress/coverage.json` domain obligations, and adds ordinary qualification tests
and runtime evidence. Imported source matrices and `runtime_pass_asserted` fields
remain untouched.

| Case | Runtime test | Evidence |
|---|---|---|
| WP-009 | `wp009_runtime_exact_qualification_admits_once_without_execution_success` | Qualification hashes the complete realization, binding provider, executable, configuration and input. Intent precedes observation; one acknowledged task/use remains DispatchPending/NotAttempted without an outcome or fabricated execution success. |
| WP-010 | `wp010_runtime_generated_executable_denied_despite_exact_approval_and_purpose` | Unreviewed and separately published generated executables deny solely Executable despite exact qualification, valid independent approval and purpose labels. |
| WP-011 | `wp011_runtime_protected_input_and_credentials_cannot_admit`; `wp011_runtime_test_credential_is_exact_short_lived_and_inspection_is_scoped`; `caller_claims_cannot_override_authenticated_agency_or_grants` | Protected inputs, production credentials, wrong run/subject, expired or widened test authority deny. Exact five-minute test credentials allow. Invalid sessions, other tenants and another authenticated principal cannot inspect; the other principal cannot advance/dispatch the effect. |
| WP-012 | `wp012_runtime_each_mandatory_control_denies_without_downgrade` | CPU/memory/mount/network controls each fail when missing, unknown, unsupported or mismatched at advertised/applied/observed stages. Unknown control names fail. Denial is exactly MandatoryControl(name); AQ/use stay unchanged. |
| WP-013 | `wp013_runtime_ordinary_unsupported_operations_are_unavailable` | Suspend/checkpoint/resume/command return solely Unavailable for otherwise valid qualification/resolution. One intent retains the requested operation, without dispatch/use, false success or termination substitution. |
| WP-014 | `wp014_runtime_drift_blocks_initial_and_prepared_delivery`; `wp014_runtime_explicit_reevaluation_cannot_rebind_persisted_request` | Provider/profile/executable/configuration/input/adapter drift blocks initial delivery and prepared recovery. Newly qualified realization requires explicit reevaluation before preparation. After preparation, valid new qualification cannot rewrite authorization/request; the binding and one reserved use remain intact. |

WP-A02 is resolved at runtime admission: the exact realization and its mandatory
requested/advertised/applied/observed controls participate in the authorization
digest and persisted execution binding. WP-012 proves fail-before-AQ for unsupported
controls; WP-014 proves drift cannot reuse authority. Real provider launch and
observed enforcement remain WIP-09/WIP-10 work; hostile-code isolation is not claimed.

## ADR assessment

| ADR | Assessment |
|---|---|
| [004](../adr/WI-ADR-004.md) | `authority::caller` creates a non-deserializable trusted caller. `resolve_agency` requires a unique current tenant/principal binding, exact actor/accountable/capacity claims and acyclic attenuating delegation. Immutable intent retains agency. Runtime forgery/scoped-inspection and domain agency/delegation tests support this boundary. Host authentication remains trusted. |
| [005](../adr/WI-ADR-005.md) | `verify_snapshot` checks scoped artifacts and exact target attestation; `resolve_resource` pins registry/resolver/descriptor/version, canonical document and audience constraints. Duplicate mappings deny. `require_current` compares the complete target before authorization/dispatch. Live adapter implementation retains its later owner. |
| [006](../adr/WI-ADR-006.md) | `authorize` intersects agency/platform/resource/information-use/grants, validates independent approval, expiry and use limits, and emits bounded reasons/trace. Qualification cannot waive executable/input/credential/control exclusions. `current` requires exact evaluation equality; preparation reserves use atomically with the immutable request. Pending authorization cannot rebind. Fresh time checks cover preparation and final delivery, including duplicate recovery. |
| [007](../adr/WI-ADR-007.md) | Journal commits Submitted intent/non-ready source outbox before observation. Resolution failure is Failed/ProvenImpossible without Running. Preparation binds a complete request artifact with separate artifact hash/canonical digest and store/task/key/execution identities. Delivery reloads/verifies, reauthorizes, validates Created/AlreadyExists identity/sequence, then acknowledges. Admission is distinct from execution/outcome; authorized run discovery is separately recoverable. Domain commit/uncertainty/retry/reconciliation obligations remain registered; invocation/publication retain later owners. |

## AQ conformance assessment

- AB-WI-002: eight concurrent preparers reload one winning complete artifact;
  preparation and use reservation share one guarded transaction. Real process
  exit cuts cover intent and AQ commit. Recovery preserves request/acknowledgment
  identities and one use. Revoked WI evidence and removed AQ host context block
  duplicate recovery without changing AQ or acknowledging WI.
- AB-WI-003: public CanonicalAdmissionV2 vectors, digest-bearing field mutations,
  Required/Detached lifecycle changes, excluded key/control attribution and tag
  normalization are asserted. The approved 2026-09-29 tenant interpretation remains:
  in-scope mutations conflict; changed tenant receives host Scope rejection before
  lookup. Every rejection preserves the projection; high-level ensure_task receives
  no independent caller digest.
- AB-WI-004: task-only acknowledgment precedes run discovery; bounded pagination
  selects exact task identity among shared correlations, resumes across restart,
  clears stale cursors without losing mappings, and persists mappings across bundle
  reopen. Removed host context denies inspection without WI/AQ mutation. Wrong-store
  acknowledgment/discovery fail. Existing fixture names/coverage remain valid,
  so `integration/fixtures.json` needs no change.

## Findings and dispositions

| Finding | Severity | Disposition |
|---|---|---|
| WIP02-F001: unsupported ordinary operation reported ProfileDrift instead of Unavailable | Major against WP-013's explicit result requirement | Fixed. Pre-fix runtime regression returned `[ProfileDrift]` when `[Unavailable]` was required. `worker_reason` validates ordinary qualification bindings first, then returns Unavailable for an unqualified operation. Invalid qualification remains ProfileDrift; the domain regression checks precedence. Denial is preserved. |
| WIP02-F002: runtime worker/source evidence missing from registry; self-initiation test stopped at resolution | Major evidence gap | Fixed. Added runtime worker/claim/self-initiation tests, strengthened resource/discovery assertions and registered complete authoritative scope coverage alongside them. No checker redesign or source-matrix result edits. |

No open blocking or major finding. Production change is limited to F001's reason
classification. Interfaces, AQ request encoding and persistence schema are preserved.
No deviation from the accepted design was necessary.

Registry discovery requires explicit source `#[test]` functions. Two existing
property tests now expose ordinary test functions using proptest's TestRunner
with the same strategies/assertions, preserving their names and generated coverage. The storage
fault test uses its exact `effect::tests::` module path. This source adjustment
keeps all authoritative obligations registrable without changing the checker.

Inventory review: the new `CurrentEvidence` struct and `Service` type alias are
test harness additions only. Both receive explicit retain-align dispositions for
WI-04 G2 source tests. The obsolete proptest declaration-generator inventory entry
and its disposition/catalog reference are removed because the two property tests
are now explicit source functions. Other dispositions, connector families,
ownership, destinations and treatments stay unchanged. Hash refresh initially
rejected structural drift; after recording these reviewed changes, the prescribed
refresh applies only source hash changes.

## Verification evidence

Rust 1.89.0; cached dependencies; no AQ path/patch override. Supplemental commands
use the supplied external CARGO_TARGET_DIR and TMPDIR from the repository root:

```sh
cargo test --locked --offline --manifest-path crates/worldinterface-runtime/Cargo.toml --test effect_service --test g2_admission
cargo test --locked --offline --manifest-path crates/worldinterface-foundation/Cargo.toml -p worldinterface-egress --test domain
cargo test --locked --offline --manifest-path crates/worldinterface-foundation/Cargo.toml -p worldinterface-egress --test domain ordinary_unsupported_operations
cargo clippy --locked --offline --manifest-path crates/worldinterface-runtime/Cargo.toml --all-targets -- -D warnings
cargo run --locked --manifest-path tools/wi-contract-check/Cargo.toml -- hashes refresh
```

Targeted results: 14 facade tests, five G2 tests and 42 domain tests passed with
none ignored. The strengthened precedence regression also passed separately.
Hash refresh preserves dispositions and does not authorize structural drift.

The committed candidate is checked offline through `ct-check --declared` for
foundation-fmt, foundation-clippy, foundation-test, foundation-isolation,
runtime-fmt, runtime-clippy, runtime-test, runtime-aq-graph, contract-check and
contract-test. CraftingTable runs adopted definitions in private clones of the
committed head. Exact commit, commands, return codes and logs are recorded in
`/mnt/workhorse/craftingtable/runs/504eef40-5e03-47af-ad50-88099baa86f5/scratch/verification/`;
`adopted-results.json` records the final candidate results. The final handoff reports
these results; independent review and integration remain controller actions.

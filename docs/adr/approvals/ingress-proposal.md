Recommend adding an isolated `worldinterface-ingress` library to the foundation workspace, with transactional admission and transport fixtures. Early implementation is authorized; **ADR-012 and ADR-013 still require operator approval before merge**. This design does not accept WI-03 or its integration slice.

No source files were modified.

**Authority and observed baseline**

The adopted [repository policy](/mnt/workhorse/craftingtable/runs/f7746052-d789-4241-8151-c4b0c8ea0070/plan/craftingtable-repository-policy.json) is **version 1, branch-settings version 1**, adopted `2026-09-18T00:41:03.548Z`. It establishes controller-local review of exact source/target commits and controller-mediated integration. Remote protection remains operator-owned and due before first remote publication and final release approval.

Fresh observations, separate from that policy:

- The worktree is clean at `9cbdb13dd769c8a66bf03fca380a1194e484476e`, tree `9e0a1f53ea67b7cee680393f983ead1679359149`; `wi-fabric-2` matches.
- `main` matches the recorded frozen commit, `f48df82e98203fa94c86eecae60425205c3e00e9`. That match corroborates location, not enforcement.
- The [scope ledger](/mnt/workhorse/craftingtable/runs/f7746052-d789-4241-8151-c4b0c8ea0070/plan/craftingtable-scope-evidence.json) authorizes early development, reports no start blockers, and supplies current WI-02/domain verification receipt `6702aafb-8f4d-4dda-a6f9-00c2221d80f5` for this base.
- ADR-001/002/003 have full approvals. ADR-016 has only its recorded early clauses; those do not approve ingress policies. ADR-012/013 remain queued.
- Repository publication guidance agrees with the adopted policy; no remaining protection-policy contradiction was found.

The recorded operator guidance to apply approved shared decisions is guidance, not additional approval or test evidence.

**Existing code and proposed changes**

The foundation already supplies canonical records, IDs, artifact storage, reducers, and replayable transactions. However, `Repository::append` handles one entity, the generic outbox has only a `delivered` flag, and repeated outbox acknowledgment fails. Authoritative bindings, provider deduplication, subscriber deliveries, attempts, and acknowledgment records are missing.

The legacy webhook handler submits a flow before storing its receipt. The streaming handler invokes an application callback and logs callback failures. Neither implements the required durable admission boundary.

| Files | Proposed change |
|---|---|
| `crates/worldinterface-foundation/Cargo.toml`, `Cargo.lock`; new `ingress/Cargo.toml` | Add `worldinterface-ingress` to the independent workspace. Depend on foundation libraries only. |
| `protocol/src/records.rs`, `lib.rs`; new `protocol/src/ingress.rs` | Define bindings, binding versions, delivery records, attempts, acknowledgments, provider identity/order evidence, and versioned policy records. Reuse existing IDs, references, canonical envelope, observation, and receipt families. |
| `domain/src/lib.rs`, `ports.rs`; new `domain/src/ingress.rs` | Add binding and delivery transition rules, including expiration, retry exhaustion, replay-safe acknowledgment, and explicit policy inputs. Preserve the other reducers. |
| New `ingress/src/{binding,source_resolution,provider,dedup,ordering,observation,receipt,delivery,ack,outbox,recovery,http,streaming}.rs` | Implement transport-independent validation and admission orchestration. HTTP and streaming inputs converge here. |
| `storage/src/repository.rs`, `root.rs`, `lib.rs`; new `storage/src/ingress.rs` | Add atomic ingress transactions, scoped indexes, delivery attempts, acknowledgment history, recovery queries, and replay validation. |
| Foundation tests; new ingress conformance fixtures | Cover transports, concurrency, transaction faults, restart, privacy, and mock subscriber crashes. |
| `scripts/check-wi-domain.sh`, `check-wi-domain-isolation.py`, `.github/workflows/wi-domain.yml` | Include ingress and identify WI-03 obligations explicitly while preserving predecessor checks. |
| Inventory/disposition records, conformance coverage, source evidence, domain documentation | Register every new source route and fixture; preserve historical evidence and distinguish WI-02 from WI-03 coverage. |

Create the physical ingress crate now because transport validation needs independent conformance testing and a reviewable dependency boundary—per implementation-plan §5.11. Do not import or wrap `WebhookRegistry`, `EmbeddedHost`, legacy streaming types, or the historical runtime.

Accepted ADR records should be imported and pinned only when the controller supplies actual approvals. Implementation must not manufacture acceptance by editing queue status.

**Interfaces and transaction boundaries**

Suggested internal interfaces:

```rust
validate_http(input, transport_context, provider_profile)
    -> Result<ValidatedProviderEvent, IngressError>;

validate_stream(frame, session_context, provider_profile)
    -> Result<ValidatedProviderEvent, IngressError>;

submit_binding(authenticated_scope, submission)
    -> Result<BindingAdmission, IngressError>;

admit_provider_event(validated_event, expected_binding_versions)
    -> Result<AdmissionResult, IngressError>;

claim_due_delivery(scope, now, expected_revision)
    -> Result<Option<DeliveryAttempt>, IngressError>;

acknowledge(subscriber_context, delivery_id, token)
    -> Result<AcknowledgmentResult, IngressError>;
```

`ValidatedProviderEvent` should have private construction, no deserialization shortcut, and carry verified transport provenance separately from provider assertions. Resolver, authorization, and subscriber seams have test implementations only. IDs, clocks, and policy versions are explicit inputs; reducers perform no I/O.

Implement these boundaries:

1. **Binding admission:** atomically persist the submission’s existing principal-scoped idempotency mapping, resolved source, subscriber, authorization evidence, immutable version, and lifecycle history. Only an active, unexpired version admits events. Recheck the version inside event admission to close suspension/revocation races.

2. **Provider admission:** validate authentication, source, tenant, event class, bounds, and attachment disposition. Publish required artifact bytes first. In one Fabric transaction, claim provider identity, write the observation and ingress receipt, create one logical delivery per matched binding, persist delivery/wake obligations, and record applicable provider progress. Any failure rolls back all Fabric changes. Prepublished unreferenced artifacts are recoverable garbage.

3. **Delivery:** persist an attempt before invoking the test subscriber or mock wake sink. Wake success does not acknowledge subscriber ingestion. Recovery reads durable pending/unfinished obligations and retries their stable identities.

4. **Acknowledgment:** authenticate the subscriber and verify the delivery token, then atomically record acknowledgment and close delivery. Equal repeats return the original result. A subscriber fixture must persist its own record keyed by `DeliveryId` before acknowledging.

Extend the repository with a transaction group rather than composing several public `append` calls. All new indexes and projections must rebuild from authoritative history, including deduplication, delivery attempts, acknowledgment, and provider cursor state.

Keep separate schema-version-1 stores under ADR-003. If their physical schema changes, use a distinct manifest-bound development profile and reject unsupported earlier development stores unchanged; do not silently migrate them.

These boundaries follow [implementation-plan WI-03, §§8.3 and 19.3](/mnt/workhorse/craftingtable/runs/f7746052-d789-4241-8151-c4b0c8ea0070/plan/wi-fabric-2-foundational-implementation-plan.md:807).

**Recommended ADR-012 decision — awaiting approval**

Adopt explicit, versioned provider profiles with these rules:

- Deduplicate within deployment/tenant, provider account, canonical source, event namespace, and declared identity strategy. Binding revision and local receipt timestamps must not change provider-event identity.
- Prefer stable provider IDs or stream positions. Permit bounded content/channel deduplication only when the provider profile explicitly declares its limitations. Represent non-deduplicable sources honestly.
- Equal identity and producer content recover the existing observation and deliveries. Conflicting content records protected conflict evidence and produces no replacement observation or subscriber wake.
- Capture matched binding versions at first admission. Share the provider observation through explicit binding associations; preserve the existing observation’s admitting-binding provenance. Retransmission must not silently add newly created subscriptions or rewrite historical provenance.
- Preserve arrival order, provider order, gaps, and out-of-order evidence separately. Do not treat a high-water mark as proof of a contiguous sequence. Cumulative provider acknowledgment advances only through the protocol’s safely committed prefix.
- Require explicit replay windows and capacity limits. Keep deduplication tombstones in this slice; apply backpressure rather than claiming unlimited retention or silently forgetting duplicates.

HTTP fixtures should verify signatures over exact request bytes and bounded authentication metadata. Streaming fixtures should verify session provenance, source matching, frame structure, replayed offsets, reconnect gaps, and commit-before-offset-acknowledgment. Neither module opens sockets.

This resolves policy choices left to ADR-012 while preserving [architecture §§7.8–7.10 and 9.7](/mnt/workhorse/craftingtable/runs/f7746052-d789-4241-8151-c4b0c8ea0070/plan/worldinterface-v2-foundational-design.md:1491).

**Recommended ADR-013 decision — awaiting approval**

Adopt at-least-once delivery with a stable `DeliveryId` and explicit durable-ingestion acknowledgment:

- Require subscriber authentication plus an unguessable, delivery-bound token. Keep token material out of receipts, ordinary queries, logs, and wake payloads; retain only protected material/references and verification digests.
- Retry with the same delivery identity. Pin bounded attempt count, acknowledgment timeout, backoff, and retention policy to the delivery. Require explicit policy values rather than hidden production defaults.
- Separate attempt status from logical delivery state. Valid delayed acknowledgment may close an issued delivery during retry/backoff or after retry exhaustion; it must never reopen delivery. Expiration and token invalidation have explicit, tested behavior.
- Dead-letter state stops automatic retries and preserves failure evidence. It does not imply application rejection or semantic acceptance.
- Recheck disclosure eligibility before exposing content. Revocation must not erase historical evidence or allow a previously admitted delivery to bypass current restrictions.
- Return governed artifact references by default. Quarantined attachments remain unavailable without matching release evidence. Logical retention expiration denies access and stops delivery while retaining minimal audit/deduplication metadata. Physical erasure is not claimed by this profile; reject policies requiring unsupported erasure behavior.

Commissioning and role/stewardship fixtures should carry existing typed evidence and information-use references. They must not activate lifecycle operations, derive authority from assertions, or introduce application conversation semantics.

These recommendations preserve [architecture §§12.12, 13.3 and 20.3](/mnt/workhorse/craftingtable/runs/f7746052-d789-4241-8151-c4b0c8ea0070/plan/worldinterface-v2-foundational-design.md:2411).

**Risks and required tests**

The principal risks are partial fan-out, binding changes during admission, conflicting provider identities, acknowledgment races, and disclosure through retry or inspection.

Required independent fixtures should demonstrate:

- Concurrent duplicate HTTP/stream events produce one observation and one delivery per matched binding; conflicting reuse cannot mutate them.
- Forged signatures, source substitutions, stale bindings, wrong tenants, malformed frames, and oversized input fail before admission.
- Faults between every observation/receipt/outbox/history write roll back; reopening after commit-before-wake recovers delivery.
- Subscriber commit-before-ack crash causes safe redelivery; repeated and delayed acknowledgments survive restart.
- Gaps, duplicates, reconnects, and out-of-order frames preserve evidence and safe provider cursor behavior.
- Retry exhaustion, expiration, revoked disclosure, wrong tokens, cross-tenant queries, and attachment quarantine fail safely.
- Rebuild and offline backup/restore retain the complete ingress lifecycle.
- Receipts establish provenance and validation, never truth, urgency, conversation membership, or permission to act.

Retain the existing `QueueWakePort` as an inactive seam. Persist stable wake identity and bounded locators independently of subscriber acknowledgment. The eventual AQ adapter must reconcile with `AQ-BASELINE-ALIGNMENT.md §2`; its signal identity is not the provider deduplication key. AB-WI-006/007/009/022 remain runtime integration obligations, not passes earned by these mocks.

**Verification and handoff**

Fresh checks on the unchanged clean baseline passed:

- `ct-check -- scripts/check-wi-domain.sh`: 27 tests, formatting, Clippy, build, and isolation.
- `ct-check -- scripts/check-wi-contract.sh`: 58 tests and nine checker categories.
- `ct-act -W .github/workflows/wi-domain.yml -j domain`: passed.

Initial environment/setup failures were corrected using supplied paths and controller Docker access. All commands finished. Results, logs, policy/source hashes, inherited approval digests, and predecessor evidence are recorded in [design-inspection.json](/mnt/workhorse/craftingtable/runs/f7746052-d789-4241-8151-c4b0c8ea0070/scratch/verification/design-inspection.json). These are baseline results, not WI-03 implementation evidence.

Implementation must rerun the scoped native checks and domain CI, plus contract CI when its tooling changes, on the exact clean reviewed candidate. Controller integration must be followed by fresh checks on the actual merged target. Full WI-02 acceptance, WI-03/integration verification, and parent acceptance remain later obligations; they do not block this authorized design.

```craftingtable-design
{
  "version": 1,
  "items": [
    {
      "kind": "resolved",
      "question": "May WI-03/domain proceed before the original WI-02 parent is accepted?",
      "answer": "Yes. Binding revision 4 authorizes early development, phaseReadiness.start is empty, and current WI-02/domain receipt 6702aafb-8f4d-4dda-a6f9-00c2221d80f5 verifies the supplied base. Full parent acceptance remains a later obligation.",
      "sources": [
        "craftingtable-scope-evidence.json: earlyDevelopmentAuthorized, phaseReadiness, receipts",
        "wi-fabric-2-foundational-implementation-plan.md §§2.5 and 6"
      ]
    },
    {
      "kind": "resolved",
      "question": "Which repository protection and publication requirements apply?",
      "answer": "Adopted policy version 1 and branch-settings version 1 establish controller-local exact-commit review and controller-mediated integration. Remote protections remain operator-owned before first remote publication and final release approval.",
      "sources": [
        "craftingtable-repository-policy.json: policy.version, policy.controlMode, policy.publicationRequirement",
        "docs/release/publication-staging.md"
      ]
    },
    {
      "kind": "resolved",
      "question": "Must canonicalization, identifier generation, or store topology be reopened?",
      "answer": "No. Apply full ADR-001/002/003 approvals. ADR-016 covers only its recorded early clauses and supplies no ingress approval. Operator guidance does not substitute for test evidence.",
      "sources": [
        "docs/adr/approvals/WI-ADR-001.json: approval e4f66a95-3338-4c6e-8990-9b64e7a85dbf",
        "docs/adr/approvals/WI-ADR-002.json: approval 1063789c-3852-46a4-9140-3fedeb689c54",
        "docs/adr/approvals/WI-ADR-003.json: approval d49b9e8a-f1b0-436f-b173-8e6333a37e7a",
        "docs/adr/approvals/WI-ADR-016.json: decision.coverage and consumers",
        "craftingtable-operator-decisions.json"
      ]
    },
    {
      "kind": "resolved",
      "question": "Does this slice require live transports or a production AQ adapter?",
      "answer": "No. Implement isolated transport validation, durable ingress transactions, test subscribers and mock wake fixtures. Retain the real adapter and AB-WI-006/007/009/022 for the mapped integration scope and WI-AQ-G3.",
      "sources": [
        "Controller execution scope: wi/WI-03/domain exclusions",
        "wi-fabric-2-foundational-implementation-plan.md: WI-03, §§5.11 and 8.3",
        "AQ-BASELINE-ALIGNMENT.md §2",
        "aq-baseline-integration-acceptance-matrix.yaml: AB-WI-006, AB-WI-007, AB-WI-009, AB-WI-022"
      ]
    },
    {
      "kind": "operator-decision",
      "question": "Approve the recommended provider identity, deduplication, fan-out, ordering, replay and retention policy as WI-ADR-012?",
      "answer": "Recommend the explicit versioned provider-profile policy described above. Approval is pending and required before merge; early implementation remains authorized.",
      "sources": [
        "docs/adr/WI-ADR-012.md",
        "wi-fabric-2-foundational-implementation-plan.md §4 and WI-03",
        "worldinterface-v2-foundational-design.md §§7.8–7.10",
        "craftingtable-scope-evidence.json: phaseReadiness.merge"
      ]
    },
    {
      "kind": "operator-decision",
      "question": "Approve the recommended acknowledgment, retry, dead-letter, token and privacy policy as WI-ADR-013?",
      "answer": "Recommend the explicit durable-ingestion acknowledgment and governed-delivery policy described above. Approval is pending and required before merge; no physical-erasure or production-delivery qualification is claimed.",
      "sources": [
        "docs/adr/WI-ADR-013.md",
        "wi-fabric-2-foundational-implementation-plan.md §4, WI-03 and §9.5",
        "worldinterface-v2-foundational-design.md §§12.12 and 20.3",
        "craftingtable-scope-evidence.json: phaseReadiness.merge"
      ]
    }
  ]
}
```

## Open questions

1. **operator-decision — WI-ADR-012:** Approve the recommended provider identity, deduplication, fan-out, ordering, replay, and retention policy above? Implementation-plan §4 and the current scope ledger require this checkpoint before merge.
2. **operator-decision — WI-ADR-013:** Approve the recommended acknowledgment, retry, dead-letter, token, and privacy policy above? Implementation-plan §4 and the current scope ledger require this checkpoint before merge.
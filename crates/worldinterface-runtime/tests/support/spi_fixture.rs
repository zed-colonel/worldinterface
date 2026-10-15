use worldinterface_domain::{authorization::AuthorizationSnapshot, ingress::digest};
use worldinterface_protocol::*;
pub fn descriptor() -> &'static ConnectorDescriptor {
    static DESCRIPTOR: std::sync::OnceLock<ConnectorDescriptor> = std::sync::OnceLock::new();
    DESCRIPTOR.get_or_init(|| ConnectorDescriptor {
        version: 1,
        connector: "fixture:connector".to_string().try_into().unwrap(),
        implementation: ContentHash::of(b"fixture-implementation:1"),
        operation: "start".to_string().try_into().unwrap(),
        effect_class: EffectClass::Control,
        resource_namespace: "worker".to_string().try_into().unwrap(),
        resource_grammar: "exact-worker:1".to_string().try_into().unwrap(),
        input_schema: "input:1".to_string().try_into().unwrap(),
        output_schema: "output:1".to_string().try_into().unwrap(),
        isolation_profile: "fixture:1".to_string().try_into().unwrap(),
        secret_refs: vec![],
        idempotency: IdempotencyClass::NonRepeatable,
        provider_key: None,
        retry_after_noncommit: true,
        observes_commit: true,
        reconciliation: ReconciliationPolicy {
            automatic: false,
            max_observations: 3,
            backoff_millis: 10,
            timeout_millis: 1000,
        },
        cancellation_supported: false,
        evidence_schema: "execution:1".to_string().try_into().unwrap(),
        max_evidence_bytes: 1024 * 1024,
    })
}
#[allow(dead_code)]
pub fn pin(mut s: AuthorizationSnapshot) -> AuthorizationSnapshot {
    let registry =
        worldinterface_runtime::connector::ConnectorRegistry::new(vec![descriptor().clone()])
            .unwrap();
    let binding = registry.bindings().unwrap().remove(0);
    s.registry.entries[0].descriptor = binding.descriptor_hash.clone();
    s.registry.entries[0].connector_binding = Some(binding);
    let hash = digest(&s.registry).unwrap();
    for fact in &mut s.facts {
        fact.registry = hash.clone();
    }
    s
}

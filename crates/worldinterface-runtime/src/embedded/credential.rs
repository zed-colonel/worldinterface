use super::*;
/// Safe worker metadata retains the complete brokerage scope alongside the
/// legacy worker contract. Neither representation contains a credential token.
pub struct WorkerCredentialMetadata {
    exact: CredentialScope,
    worker: worldinterface_worker::credential::CredentialBindingV1,
}
impl WorkerCredentialMetadata {
    pub fn scope(&self) -> &CredentialScope {
        &self.exact
    }
    pub fn worker(&self) -> &worldinterface_worker::credential::CredentialBindingV1 {
        &self.worker
    }
    pub(super) fn delivered(metadata: CredentialMetadata) -> Result<Self> {
        let b = metadata.binding;
        if metadata.status != IssuanceStatus::Delivered || b.audience != Audience::Worker {
            return Err("worker credential unavailable".into());
        }
        let op = crate::worker::admission::OPERATIONS
            .into_iter()
            .find(|o| o.name() == b.operation.as_str())
            .ok_or("worker operation unavailable")?;
        let worker = worldinterface_worker::credential::CredentialBindingV1 {
            schema: 1,
            reference: metadata.issuance.as_str().into(),
            issuer: "worldinterface".into(),
            audience: b.recipient.as_str().into(),
            principal: b.principal.as_str().into(),
            provider: b.provider.as_str().into(),
            run: b.run.as_str().into(),
            operations: vec![op],
            artifact_scope: vec![],
            authorization_digest: hash(&b)?,
            expires_unix_ms: b.expires_at_ms,
            revocation_generation: b.generation,
        };
        Ok(Self { exact: b, worker })
    }
}

#[cfg(test)]
#[test]
fn worker_metadata_translation_preserves_every_brokerage_scope_dimension() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../tests/support/egress.json")).unwrap();
    let fact: worldinterface_domain::resource::ResolutionFact =
        serde_json::from_value(fixture["snapshot"]["facts"][0].clone()).unwrap();
    let principal: PrincipalId = fixture["snapshot"]["bindings"][0]["principal"]
        .as_str()
        .unwrap()
        .to_string()
        .try_into()
        .unwrap();
    let scope = Scope {
        tenant: "tenant".to_string().try_into().unwrap(),
        organization: Some(PrincipalId::generate()),
    };
    let exact = CredentialScope {
        scope: scope.clone(),
        principal,
        audience: Audience::Worker,
        recipient: "recipient".to_string().try_into().unwrap(),
        provider: "provider".to_string().try_into().unwrap(),
        operation: "start".to_string().try_into().unwrap(),
        resource: fact.document.reference(fact.observed_version).unwrap(),
        run: "run".to_string().try_into().unwrap(),
        authorization: ContentHash::of(b"authorization"),
        revisions: std::collections::BTreeMap::from([(
            "revision".to_string().try_into().unwrap(),
            ContentHash::of(b"managed revision"),
        )]),
        class: CredentialClass::Test,
        secret: ObjectRef {
            scope,
            kind: ObjectKind::Secret,
            id: "secret".to_string().try_into().unwrap(),
        },
        secret_version: 8,
        generation: 7,
        expires_at_ms: 120000,
    };
    let metadata = CredentialMetadata {
        issuance: ContentHash::of(b"issuance"),
        permit: ObjectRef {
            scope: exact.scope.clone(),
            kind: ObjectKind::Authorization,
            id: "permit".to_string().try_into().unwrap(),
        },
        binding: exact.clone(),
        status: IssuanceStatus::Delivered,
        reason: None,
    };
    let translated = WorkerCredentialMetadata::delivered(metadata.clone()).unwrap();
    assert_eq!(translated.scope(), &exact);
    assert_eq!(translated.worker().authorization_digest, hash(&exact).unwrap());
    assert_eq!(translated.worker().expires_unix_ms, exact.expires_at_ms);
    assert_eq!(translated.worker().revocation_generation, exact.generation);
    assert_eq!(translated.worker().operations.len(), 1);
    let mut uncertain = metadata;
    uncertain.status = IssuanceStatus::Unresolved;
    assert!(WorkerCredentialMetadata::delivered(uncertain).is_err());
}

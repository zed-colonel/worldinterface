//! Deterministic resolution from pinned facts. No DNS, filesystem or provider calls.
use crate::{agency::Result, ingress::digest};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use worldinterface_protocol::*;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Target {
    Filesystem {
        root: OpaqueRef,
        components: Vec<String>,
        symlink_chain: ContentHash,
    },
    Network {
        scheme: OpaqueRef,
        host: OpaqueRef,
        port: u16,
        addresses: BTreeSet<OpaqueRef>,
        redirects: Vec<OpaqueRef>,
        path: String,
    },
    Communication {
        account: OpaqueRef,
        sender: PrincipalId,
        channel: OpaqueRef,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thread: Option<OpaqueRef>,
        audience: BTreeSet<PrincipalId>,
        visibility: OpaqueRef,
    },
    Worker {
        provider: OpaqueRef,
        run: OpaqueRef,
        resource: OpaqueRef,
        operation: OpaqueRef,
    },
    Lifecycle {
        instance: OpaqueRef,
        target: ContentHash,
        operation: OpaqueRef,
    },
}
impl Target {
    pub fn namespace(&self) -> &'static str {
        match self {
            Self::Filesystem { .. } => "filesystem",
            Self::Network { .. } => "network",
            Self::Communication { .. } => "communication",
            Self::Worker { .. } => "worker",
            Self::Lifecycle { .. } => "lifecycle",
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetDocument {
    pub version: u32,
    pub tenant: OpaqueRef,
    pub owner: PrincipalId,
    pub target: Target,
}
impl TargetDocument {
    pub fn reference(&self, observed_version: OpaqueRef) -> Result<CanonicalResourceRef> {
        if self.version != 1 {
            return Err("unknown target schema".into());
        }
        match &self.target {
            Target::Filesystem { components, .. }
                if components.is_empty()
                    || components.len() > 64
                    || components.iter().any(|c| {
                        c.is_empty()
                            || c == "."
                            || c == ".."
                            || c.contains(['/', '\\', '\0'])
                            || c.len() > 255
                    }) =>
            {
                return Err("invalid path components".into())
            }
            Target::Network { scheme, port, addresses, redirects, path, .. }
                if !["https", "http"].contains(&scheme.as_str())
                    || *port == 0
                    || addresses.is_empty()
                    || addresses.len() > 32
                    || redirects.len() > 8
                    || !path.starts_with('/')
                    || path.len() > 4096 =>
            {
                return Err("invalid network target".into())
            }
            Target::Communication { audience, .. }
                if audience.is_empty() || audience.len() > 256 =>
            {
                return Err("invalid audience".into())
            }
            _ => {}
        }
        Ok(CanonicalResourceRef {
            namespace: self.target.namespace().to_owned().try_into()?,
            resource: format!("wi-resource:sha256:{}", digest(self)?.as_str()).try_into()?,
            version: observed_version,
        })
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolverEntry {
    pub namespace: OpaqueRef,
    pub version: OpaqueRef,
    pub descriptor: ContentHash,
    pub connector: OpaqueRef,
    pub semantics: OpaqueRef,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolverSnapshot {
    pub version: u32,
    pub entries: Vec<ResolverEntry>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolutionFact {
    pub selector: ResourceSelector,
    pub registry: ContentHash,
    pub document: TargetDocument,
    pub observed_version: OpaqueRef,
    pub valid_until: Timestamp,
    /// Exact target attestation includes symlink/DNS/redirect/audience/ownership facts.
    pub evidence: EvidenceRef,
    pub attested_document: ContentHash,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolutionRecord {
    pub resolved: ResolvedEffect,
    pub fact: ResolutionFact,
    pub descriptor: ContentHash,
}
pub fn resolve_resource(
    intent: &EffectIntent,
    tenant: &OpaqueRef,
    registry: &ResolverSnapshot,
    facts: &[ResolutionFact],
    now: &Timestamp,
) -> Result<ResolutionRecord> {
    if registry.version != 1 || registry.entries.len() > 32 || facts.len() > 256 {
        return Err("unsupported resolver snapshot".into());
    }
    let mut namespaces = BTreeSet::new();
    if registry.entries.iter().any(|e| !namespaces.insert(&e.namespace)) {
        return Err("ambiguous registry".into());
    }
    let r = registry
        .entries
        .iter()
        .find(|e| e.namespace == intent.submission.resource.namespace)
        .ok_or("unknown namespace")?;
    let matches: Vec<_> =
        facts.iter().filter(|f| f.selector == intent.submission.resource).collect();
    if matches.len() != 1 {
        return Err("missing or ambiguous target".into());
    }
    let f = matches[0];
    if f.registry != digest(registry)?
        || f.document.tenant != *tenant
        || f.evidence.artifact.scope != *tenant
        || f.attested_document != digest(&f.document)?
        || f.document.target.namespace() != r.namespace.as_str()
        || f.valid_until <= *now
    {
        return Err("insufficient exact resolution evidence".into());
    }
    if let Target::Communication { audience, .. } = &f.document.target {
        let use_policy =
            intent.submission.information_use.as_ref().ok_or("missing audience bound")?;
        if use_policy.redistribution != RedistributionPolicy::NamedAudiences
            || audience.iter().any(|p| !use_policy.allowed_audiences.contains(p))
        {
            return Err("audience widening".into());
        }
    }
    Ok(ResolutionRecord {
        resolved: ResolvedEffect {
            intent_id: intent.id.clone(),
            effect_class: intent.submission.effect_class.clone(),
            resource: f.document.reference(f.observed_version.clone())?,
            normalized_parameters: intent.submission.input.clone(),
            resolver_version: r.version.clone(),
            registry_snapshot: f.registry.clone(),
            connector: r.connector.clone(),
            idempotency_semantics: r.semantics.clone(),
            sensitivity: "exact".to_owned().try_into()?,
        },
        fact: f.clone(),
        descriptor: r.descriptor.clone(),
    })
}

//! Deterministic resolution from pinned facts. No DNS, filesystem or provider calls.
use crate::{agency::Result, ingress::digest};
use core::net::IpAddr;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use worldinterface_protocol::*;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Target {
    Connector {
        provider: OpaqueRef,
        resource: OpaqueRef,
        operation: OpaqueRef,
    },
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
            Self::Connector { .. } => "connector",
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
            Target::Network { scheme, host, port, addresses, redirects, path }
                if !["https", "http"].contains(&scheme.as_str())
                    || *port == 0
                    || addresses.is_empty()
                    || addresses.len() > 32
                    // A redirect has a distinct address chain; this initial
                    // resolver contract has no per-hop attestation.
                    || !redirects.is_empty()
                    || addresses.iter().any(|address| !public_ip(address.as_str()))
                    || host.as_str().parse::<IpAddr>().is_ok_and(|_| !public_ip(host.as_str()))
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
fn public_ip(value: &str) -> bool {
    match value.parse::<IpAddr>() {
        Ok(IpAddr::V4(ip)) => {
            let octets = ip.octets();
            !ip.is_private()
                && !ip.is_loopback()
                && !ip.is_link_local()
                && !ip.is_multicast()
                && !ip.is_unspecified()
                && octets[0] != 0
                && octets[0] < 224
                && !(octets[0] == 100 && (64..=127).contains(&octets[1]))
        }
        Ok(IpAddr::V6(ip)) => {
            !ip.is_loopback()
                && !ip.is_unique_local()
                && !ip.is_unicast_link_local()
                && !ip.is_multicast()
                && !ip.is_unspecified()
                && ip.to_ipv4_mapped().is_none_or(|mapped| public_ip(&mapped.to_string()))
        }
        Err(_) => false,
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolverEntry {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connector_binding: Option<ConnectorBinding>,
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
    let connector_execution = r
        .connector_binding
        .as_ref()
        .map(|binding| -> Result<ConnectorExecution> {
            binding.validate()?;
            let d = &binding.descriptor;
            if binding.descriptor_hash != r.descriptor
                || d.connector != r.connector
                || d.resource_namespace != r.namespace
                || d.effect_class != intent.submission.effect_class
            {
                return Err("connector resolution binding mismatch".into());
            }
            if let Target::Connector { provider, operation, .. } = &f.document.target {
                if provider != &d.connector || operation != &d.operation {
                    return Err("connector target operation mismatch".into());
                }
            }
            if let Target::Worker { operation, .. } = &f.document.target {
                if operation != &d.operation {
                    return Err("worker target operation mismatch".into());
                }
            }
            Ok(ConnectorExecution {
                binding: binding.clone(),
                operation_identity: intent.stable_idempotency_identity.clone(),
                provider_key: d
                    .provider_key
                    .as_ref()
                    .map(|k| k.derive(tenant, &d.operation, &intent.stable_idempotency_identity))
                    .transpose()?,
            })
        })
        .transpose()?;
    Ok(ResolutionRecord {
        resolved: ResolvedEffect {
            connector_execution,
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

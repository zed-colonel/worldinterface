//! AQ-free notification identities. A route is a scoped notification, never authority.
use serde::{Deserialize, Serialize};
use worldinterface_protocol::{ArtifactRef, ContentHash, OpaqueRef, PrincipalId};

pub const NAMESPACE: &str = "wi.events.v1";
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    ProviderCallback,
    IngressReady,
    IngressAcknowledged,
    Reconciliation,
    WorkerObservation,
    FlowChild,
}
impl EventKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::ProviderCallback => "provider_callback",
            Self::IngressReady => "ingress_ready",
            Self::IngressAcknowledged => "ingress_acknowledged",
            Self::Reconciliation => "reconciliation",
            Self::WorkerObservation => "worker_observation",
            Self::FlowChild => "flow_child",
        }
    }
    pub fn parse(value: &str) -> Result<Self, String> {
        [
            Self::ProviderCallback,
            Self::IngressReady,
            Self::IngressAcknowledged,
            Self::Reconciliation,
            Self::WorkerObservation,
            Self::FlowChild,
        ]
        .into_iter()
        .find(|k| k.name() == value)
        .ok_or_else(|| "unknown WI event route".into())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventRoute {
    pub tenant: OpaqueRef,
    pub audience: PrincipalId,
    pub kind: EventKind,
    pub correlation: OpaqueRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<OpaqueRef>,
}
impl EventRoute {
    pub fn correlation_id(&self) -> Result<String, String> {
        Ok(format!(
            "wi:{}",
            worldinterface_protocol::data_digest(&("wi-event-route-1", self,))?.as_str()
        ))
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableEvent {
    pub version: u32,
    pub id: OpaqueRef,
    pub route: EventRoute,
    pub source: OpaqueRef,
    pub revision: u64,
    pub digest: ContentHash,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact: Option<ArtifactRef>,
}
impl DurableEvent {
    pub fn new(
        route: EventRoute,
        source: OpaqueRef,
        revision: u64,
        digest: ContentHash,
        artifact: Option<ArtifactRef>,
    ) -> Result<Self, String> {
        // Identity deliberately excludes content: a conflicting duplicate must reject.
        let id = format!(
            "wi-event:{}",
            worldinterface_protocol::data_digest(&(
                "wi-event-identity-1",
                &route,
                &source,
                revision,
            ))?
            .as_str()
        )
        .try_into()?;
        Ok(Self { version: 1, id, route, source, revision, digest, artifact })
    }
    pub fn validate(&self) -> Result<(), String> {
        if self.version != 1
            || self.revision == 0
            || self
                .artifact
                .as_ref()
                .is_some_and(|a| a.scope != self.route.tenant || a.content != self.digest)
            || Self::new(
                self.route.clone(),
                self.source.clone(),
                self.revision,
                self.digest.clone(),
                self.artifact.clone(),
            )? != *self
        {
            return Err("invalid event identity or scope".into());
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignalObligation {
    pub event: DurableEvent,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_digest: Option<ContentHash>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rejection: Option<String>,
}
/// Exact AQ representations are opaque to foundation. Runtime validates their meaning.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitCheckpoint {
    pub node: OpaqueRef,
    pub queue_store: OpaqueRef,
    pub run: OpaqueRef,
    pub plan_hash: ContentHash,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<EventRoute>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_sequence: Option<u64>,
    pub wait: Vec<u8>,
    pub checkpoint: Vec<u8>,
    /// Reestablished waits retain the WI checkpoint without re-emitting an AQ checkpoint.
    pub queue_checkpoint: bool,
    pub artifact: ArtifactRef,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResumeAcceptance {
    pub queue_store: OpaqueRef,
    pub run: OpaqueRef,
    pub context_id: u64,
    pub context_digest: ContentHash,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event: Option<OpaqueRef>,
    /// Immutable serialized handler result, replayed for Retry and Recovery.
    pub disposition: Vec<u8>,
}

pub fn flow_child_ref(run: &OpaqueRef, node: &OpaqueRef) -> Result<OpaqueRef, String> {
    format!(
        "wi-child:{}",
        worldinterface_protocol::data_digest(&("wi-child-1", run, node))?.as_str()
    )
    .try_into()
}

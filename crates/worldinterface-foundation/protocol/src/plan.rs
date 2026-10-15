//! Untrusted Flow/plan documents. Only domain compilation establishes executability.
use crate::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub const PLAN_VERSION: u32 = 1;
pub const MAX_PLAN_NODES: usize = 64;
pub const MAX_PLAN_PORTS: usize = 32;
pub const MAX_PLAN_ALTERNATIVES: usize = 64;
pub const EVALUATOR: &str = "wi-expr-1";

/// A finite structural ceiling, never an authorization envelope or a grant.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityAlternative {
    pub connector: OpaqueRef,
    pub operation: OpaqueRef,
    pub effect: EffectClass,
    pub resource: ResourceSelector,
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityUpperBound {
    pub alternatives: Vec<AuthorityAlternative>,
}
impl AuthorityUpperBound {
    pub fn attenuates(&self, parent: &Self) -> bool {
        self.alternatives.iter().all(|a| parent.alternatives.contains(a))
    }
}

/// Closed schema subset. Object properties are all required; extra properties reject.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Schema {
    Boolean {},
    Integer {},
    String { max_bytes: u32 },
    Object { properties: BTreeMap<OpaqueRef, Schema> },
    Array { items: Box<Schema>, max_items: u32 },
    Resource {},
}
impl Schema {
    pub fn digest(&self) -> Result<ContentHash> {
        Ok(ContentHash::of(&canonical_data(&("wi-schema-1", self))?))
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaPin {
    pub schema: Schema,
    pub digest: ContentHash,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputRef {
    pub node: OpaqueRef,
    pub port: OpaqueRef,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum InputSource {
    Literal { value: Value },
    Artifact { reference: ArtifactRef },
    Output { source: OutputRef },
    Exclusive { branch: OpaqueRef, when_true: OutputRef, when_false: OutputRef },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanInput {
    pub schema: OpaqueRef,
    pub source: InputSource,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanOutput {
    pub schema: OpaqueRef,
    /// An output carrying a resource selector must declare all possible resources.
    pub resources: Vec<ResourceSelector>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ResourceBinding {
    Fixed {
        resource: ResourceSelector,
    },
    /// Exactly one producer. Multiple producers require explicit exclusive selection.
    Produced {
        producers: Vec<OutputRef>,
    },
    Exclusive {
        branch: OpaqueRef,
        when_true: OutputRef,
        when_false: OutputRef,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Expression {
    Input { port: OpaqueRef },
    Literal { value: Value },
    Project { value: Box<Expression>, field: OpaqueRef },
    Equal { left: Box<Expression>, right: Box<Expression> },
    And { left: Box<Expression>, right: Box<Expression> },
    Or { left: Box<Expression>, right: Box<Expression> },
    Not { value: Box<Expression> },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Program {
    pub evaluator: OpaqueRef,
    pub runtime_hash: ContentHash,
    pub implementation_hash: ContentHash,
    pub expression: Expression,
    /// Requests are explicit and always rejected in the pure profile, even with a pin.
    pub requested_access: Vec<OpaqueRef>,
}
impl Program {
    pub fn closed(expression: Expression) -> Result<Self> {
        Ok(Self {
            evaluator: EVALUATOR.to_string().try_into()?,
            runtime_hash: ContentHash::of(EVALUATOR.as_bytes()),
            implementation_hash: ContentHash::of(&canonical_data(&(EVALUATOR, &expression))?),
            expression,
            requested_access: vec![],
        })
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FanOut {
    /// This initial profile uses explicit expanded nodes, one instance per stable ID.
    Expanded {},
    Dynamic {
        input: OpaqueRef,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeoutAction {
    Fail,
    Continue,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Deadline {
    pub inclusive_at: Timestamp,
    pub action: TimeoutAction,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SignalProgress {
    Once {},
    /// Initial retained-event cursor, not proof of progress after another wait.
    AfterSequence {
        sequence: u64,
    },
    /// Bind to the referenced wait's actual handled cursor at lowering time.
    AfterHandled {
        wait: OpaqueRef,
    },
    FreshGeneration {
        generation: OpaqueRef,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignalWait {
    pub tenant: OpaqueRef,
    pub namespace: OpaqueRef,
    pub signal_kind: OpaqueRef,
    pub correlation: OpaqueRef,
    pub accept_early: bool,
    pub progress: SignalProgress,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline: Option<Deadline>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoundaryPolicy {
    pub budget: EffectBudget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline: Option<Timestamp>,
    pub receipt_profile: OpaqueRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_outcome: Option<OpaqueRef>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PlanOperation {
    Boundary { binding: Box<ConnectorBinding>, resource: ResourceBinding, policy: BoundaryPolicy },
    Transform { program: Program },
    Branch { program: Program, when_true: OpaqueRef, when_false: OpaqueRef },
    Wait { signal: SignalWait },
    Timer { deadline: Deadline },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanNode {
    pub id: OpaqueRef,
    pub dependencies: Vec<OpaqueRef>,
    /// Conjunction of declared branch outcomes required for this node to run.
    pub guards: BTreeMap<OpaqueRef, OpaqueRef>,
    pub inputs: BTreeMap<OpaqueRef, PlanInput>,
    pub outputs: BTreeMap<OpaqueRef, PlanOutput>,
    pub authority: AuthorityUpperBound,
    pub fan_out: FanOut,
    pub operation: PlanOperation,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FlowBody {
    pub entries: Vec<OpaqueRef>,
    pub nodes: Vec<PlanNode>,
    pub outputs: BTreeMap<OpaqueRef, PlanInput>,
    pub authority: AuthorityUpperBound,
    pub causal_metadata: BTreeMap<OpaqueRef, OpaqueRef>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FlowDefinition {
    pub id: OpaqueRef,
    pub version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predecessor: Option<ContentHash>,
    pub body: FlowBody,
}
impl FlowDefinition {
    /// Pure revision: the original value is never modified. Activation is WIP-07.
    pub fn revise(&self, body: FlowBody) -> Result<Self> {
        Ok(Self {
            id: self.id.clone(),
            version: self.version.checked_add(1).ok_or("version overflow")?,
            predecessor: Some(self.digest()?),
            body,
        })
    }
    pub fn digest(&self) -> Result<ContentHash> {
        let mut normalized = self.clone();
        normalize_body(&mut normalized.body)?;
        Ok(ContentHash::of(&canonical_data(&("wi-flow-definition-1", normalized))?))
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PlanOrigin {
    Direct,
    Flow {
        id: OpaqueRef,
        version: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        predecessor: Option<ContentHash>,
        definition_digest: ContentHash,
    },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanDocument {
    pub version: u32,
    pub origin: PlanOrigin,
    pub registry_digest: ContentHash,
    pub schemas: BTreeMap<OpaqueRef, SchemaPin>,
    pub submitting_authority: AuthorityUpperBound,
    pub run_authority: AuthorityUpperBound,
    pub body: FlowBody,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanEnvelope {
    pub contract: String,
    pub canonical_profile: String,
    pub schema: u32,
    pub record_kind: String,
    pub plan: PlanDocument,
}
impl PlanEnvelope {
    pub fn new(plan: PlanDocument) -> Self {
        Self {
            contract: CONTRACT.into(),
            canonical_profile: CANONICAL_PROFILE.into(),
            schema: 1,
            record_kind: "execution_plan".into(),
            plan,
        }
    }
    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        if self.contract != CONTRACT
            || self.canonical_profile != CANONICAL_PROFILE
            || self.schema != 1
            || self.record_kind != "execution_plan"
            || self.plan.version != PLAN_VERSION
        {
            return Err("unsupported plan envelope".into());
        }
        canonical_data(self)
    }
    pub fn digest(&self) -> Result<ContentHash> {
        let mut bytes = b"WI-FABRIC-2\0wi-json-1\0execution-plan-1\0".to_vec();
        bytes.extend(self.canonical_bytes()?);
        Ok(ContentHash::of(&bytes))
    }
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        let value = strict_json(bytes)?;
        let result: Self = serde_json::from_value(value.clone()).map_err(|e| e.to_string())?;
        if serde_json::to_value(&result).map_err(|e| e.to_string())? != value
            || result.canonical_bytes()? != bytes
        {
            return Err("noncanonical plan representation".into());
        }
        Ok(result)
    }
}

pub fn normalize_authority(bound: &mut AuthorityUpperBound) -> Result<()> {
    if bound.alternatives.len() > MAX_PLAN_ALTERNATIVES {
        return Err("authority bound".into());
    }
    sort_set(&mut bound.alternatives)
}
fn sort_set<T: Serialize + Clone>(values: &mut Vec<T>) -> Result<()> {
    let mut keyed =
        values.iter().map(|v| Ok((canonical_data(v)?, v.clone()))).collect::<Result<Vec<_>>>()?;
    keyed.sort_by(|a, b| a.0.cmp(&b.0));
    keyed.dedup_by(|a, b| a.0 == b.0);
    *values = keyed.into_iter().map(|(_, v)| v).collect();
    Ok(())
}
pub fn normalize_body(body: &mut FlowBody) -> Result<()> {
    normalize_authority(&mut body.authority)?;
    body.entries.sort();
    body.entries.dedup();
    body.nodes.sort_by(|a, b| a.id.cmp(&b.id));
    for node in &mut body.nodes {
        node.dependencies.sort();
        node.dependencies.dedup();
        normalize_authority(&mut node.authority)?;
        for output in node.outputs.values_mut() {
            sort_set(&mut output.resources)?;
        }
        if let PlanOperation::Boundary { binding, .. } = &mut node.operation {
            // Descriptors are pinned bytes; their internal order is not rewritten.
            binding.validate()?;
        }
    }
    Ok(())
}

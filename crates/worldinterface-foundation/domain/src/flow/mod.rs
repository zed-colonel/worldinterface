//! Pure Flow compilation. Structural ceilings never grant executable authority.
mod expression;
mod validation;
pub use expression::{
    validate_value, Evaluation, MAX_EVALUATION_STEPS, MAX_EXPRESSION_DEPTH, MAX_VALUE_BYTES,
};
use std::collections::{BTreeMap, BTreeSet};
pub use worldinterface_protocol::plan::*;
use worldinterface_protocol::{
    canonical_data, validate_registry_snapshot, ConnectorBinding, ConnectorDescriptor, ContentHash,
    EffectSubmission, OpaqueRef, ResourceSelector, Timestamp,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    Graph,
    Unreachable,
    Reference,
    Schema,
    HiddenIo,
    Authority,
    Resource,
    Bound,
    Wait,
    Version,
    Recovery,
    Registry,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompileError {
    pub kind: ErrorKind,
    pub node: Option<OpaqueRef>,
    pub detail: String,
}
impl std::fmt::Display for CompileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?} {:?}: {}", self.kind, self.node, self.detail)
    }
}
impl std::error::Error for CompileError {}
fn error(kind: ErrorKind, node: Option<&OpaqueRef>, detail: &str) -> CompileError {
    CompileError { kind, node: node.cloned(), detail: detail.into() }
}
fn wire(detail: String) -> CompileError {
    error(ErrorKind::Recovery, None, &detail)
}
fn opaque(s: &str) -> OpaqueRef {
    s.to_string().try_into().expect("static opaque reference")
}

/// Immutable, data-only compilation snapshot. No callbacks, resolver or executor.
#[derive(Clone, Debug)]
pub struct CompilationContext {
    descriptors: Vec<ConnectorDescriptor>,
    registry_digest: ContentHash,
    schemas: BTreeMap<OpaqueRef, SchemaPin>,
    submitting_authority: AuthorityUpperBound,
    run_authority: AuthorityUpperBound,
}
impl CompilationContext {
    pub fn new(
        mut descriptors: Vec<ConnectorDescriptor>,
        schemas: BTreeMap<OpaqueRef, Schema>,
        mut submitting_authority: AuthorityUpperBound,
        mut run_authority: AuthorityUpperBound,
    ) -> Result<Self, CompileError> {
        let registry_digest = validate_registry_snapshot(&mut descriptors)
            .map_err(|e| error(ErrorKind::Registry, None, &e))?;
        if schemas.is_empty()
            || schemas.len() > 64
            || schemas.values().any(|s| !expression::validate_schema(s))
        {
            return Err(error(ErrorKind::Schema, None, "unsupported schema snapshot"));
        }
        let schemas = schemas
            .into_iter()
            .map(|(id, schema)| {
                Ok((id, SchemaPin { digest: schema.digest().map_err(wire)?, schema }))
            })
            .collect::<Result<_, CompileError>>()?;
        normalize_authority(&mut submitting_authority).map_err(wire)?;
        normalize_authority(&mut run_authority).map_err(wire)?;
        if !run_authority.attenuates(&submitting_authority) {
            return Err(error(
                ErrorKind::Authority,
                None,
                "run ceiling exceeds submitting ceiling",
            ));
        }
        Ok(Self { descriptors, registry_digest, schemas, submitting_authority, run_authority })
    }
    pub fn binding(
        &self,
        connector: &OpaqueRef,
        operation: &OpaqueRef,
    ) -> Result<ConnectorBinding, CompileError> {
        let descriptor = self
            .descriptors
            .iter()
            .find(|d| &d.connector == connector && &d.operation == operation)
            .ok_or_else(|| error(ErrorKind::Registry, None, "unknown connector operation"))?
            .clone();
        Ok(ConnectorBinding {
            version: 1,
            descriptor_hash: descriptor.digest().map_err(wire)?,
            descriptor,
            registry_digest: self.registry_digest.clone(),
        })
    }
}
/// Executable plans have no unchecked construction, deserialization or mutation API.
///
/// ```compile_fail
/// use worldinterface_domain::flow::ExecutionPlan;
/// let p = ExecutionPlan {};
/// ```
/// ```compile_fail
/// use worldinterface_domain::flow::ExecutionPlan;
/// let p: ExecutionPlan = serde_json::from_str("{}").unwrap();
/// ```
/// ```compile_fail
/// use worldinterface_domain::flow::{ExecutionPlan, FlowBody};
/// fn change(p: &mut ExecutionPlan, body: FlowBody) { p.document().body = body; }
/// ```
#[derive(Clone, Debug)]
pub struct ExecutionPlan {
    envelope: PlanEnvelope,
    bytes: Vec<u8>,
    hash: ContentHash,
    // Persist separately with run admission in WIP-07. Never part of plan identity.
    direct_submission: Option<EffectSubmission>,
}
impl ExecutionPlan {
    pub fn document(&self) -> &PlanDocument {
        &self.envelope.plan
    }
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn hash(&self) -> &ContentHash {
        &self.hash
    }
    pub fn direct_submission(&self) -> Option<&EffectSubmission> {
        self.direct_submission.as_ref()
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirectOperationSelection {
    pub connector: OpaqueRef,
    pub operation: OpaqueRef,
}

pub fn compile_direct(
    submission: &EffectSubmission,
    selection: &DirectOperationSelection,
    context: &CompilationContext,
) -> Result<ExecutionPlan, CompileError> {
    // Check record-level bounds without invoking any admission/authorization logic.
    worldinterface_protocol::Envelope::new(worldinterface_protocol::Record::EffectSubmission(
        submission.clone(),
    ))
    .canonical_bytes()
    .map_err(wire)?;
    let binding = context.binding(&selection.connector, &selection.operation)?;
    if submission.effect_class != binding.descriptor.effect_class {
        return Err(error(ErrorKind::Authority, None, "submission effect differs from descriptor"));
    }
    let node = PlanNode {
        id: opaque("direct"),
        dependencies: vec![],
        guards: BTreeMap::new(),
        inputs: BTreeMap::from([(
            opaque("input"),
            PlanInput {
                schema: binding.descriptor.input_schema.clone(),
                source: InputSource::Artifact { reference: submission.input.artifact.clone() },
            },
        )]),
        outputs: BTreeMap::from([(
            opaque("output"),
            PlanOutput { schema: binding.descriptor.output_schema.clone(), resources: vec![] },
        )]),
        authority: context.run_authority.clone(),
        fan_out: FanOut::Expanded {},
        operation: PlanOperation::Boundary {
            binding: Box::new(binding),
            resource: ResourceBinding::Fixed { resource: submission.resource.clone() },
            policy: BoundaryPolicy {
                budget: submission.budget.clone(),
                deadline: submission.deadline.clone(),
                receipt_profile: submission.requested_receipt_profile.clone(),
                expected_outcome: submission.expected_outcome.clone(),
            },
        },
    };
    let body = FlowBody {
        entries: vec![node.id.clone()],
        nodes: vec![node],
        outputs: BTreeMap::new(),
        authority: context.run_authority.clone(),
        causal_metadata: BTreeMap::new(),
    };
    let mut plan = compile_document(document(body, PlanOrigin::Direct, context), context)?;
    plan.direct_submission = Some(submission.clone());
    Ok(plan)
}
pub fn compile_flow(
    definition: &FlowDefinition,
    context: &CompilationContext,
) -> Result<ExecutionPlan, CompileError> {
    let mut definition = definition.clone();
    normalize_body(&mut definition.body).map_err(wire)?;
    let origin = PlanOrigin::Flow {
        id: definition.id.clone(),
        version: definition.version,
        predecessor: definition.predecessor.clone(),
        definition_digest: definition.digest().map_err(wire)?,
    };
    compile_document(document(definition.body, origin, context), context)
}
fn document(body: FlowBody, origin: PlanOrigin, c: &CompilationContext) -> PlanDocument {
    PlanDocument {
        version: PLAN_VERSION,
        origin,
        registry_digest: c.registry_digest.clone(),
        schemas: c.schemas.clone(),
        submitting_authority: c.submitting_authority.clone(),
        run_authority: c.run_authority.clone(),
        body,
    }
}
fn compile_document(
    mut document: PlanDocument,
    context: &CompilationContext,
) -> Result<ExecutionPlan, CompileError> {
    normalize_body(&mut document.body).map_err(wire)?;
    validation::validate(&document, context)?;
    let envelope = PlanEnvelope::new(document);
    let bytes = envelope.canonical_bytes().map_err(wire)?;
    let hash = envelope.digest().map_err(wire)?;
    Ok(ExecutionPlan { envelope, bytes, hash, direct_submission: None })
}
/// Recovery repeats every compiler check. The caller persists admission separately.
pub fn restore_plan(
    canonical_bytes: &[u8],
    expected_hash: &ContentHash,
    context: &CompilationContext,
) -> Result<ExecutionPlan, CompileError> {
    let envelope = PlanEnvelope::parse(canonical_bytes).map_err(wire)?;
    let result = compile_document(envelope.plan, context)?;
    if result.canonical_bytes() != canonical_bytes || result.hash() != expected_hash {
        return Err(error(ErrorKind::Recovery, None, "plan bytes or digest mismatch"));
    }
    Ok(result)
}

/// Immutable validated definition record for later activation/persistence.
/// Durable version uniqueness and predecessor/activation lookup belong to WIP-07.
#[derive(Clone, Debug)]
pub struct FlowVersion {
    definition: FlowDefinition,
    digest: ContentHash,
}
impl FlowVersion {
    pub fn definition(&self) -> &FlowDefinition {
        &self.definition
    }
    pub fn digest(&self) -> &ContentHash {
        &self.digest
    }
    pub fn revise(
        &self,
        body: FlowBody,
        context: &CompilationContext,
    ) -> Result<(Self, ExecutionPlan), CompileError> {
        compile_flow_version(&self.definition.revise(body).map_err(wire)?, context)
    }
}
pub fn compile_flow_version(
    definition: &FlowDefinition,
    context: &CompilationContext,
) -> Result<(FlowVersion, ExecutionPlan), CompileError> {
    let plan = compile_flow(definition, context)?;
    let mut definition = definition.clone();
    definition.body = plan.document().body.clone();
    let digest = definition.digest().map_err(wire)?;
    Ok((FlowVersion { definition, digest }, plan))
}

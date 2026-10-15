use super::*;
use serde_json::{json, Value};

pub const MAX_EXPRESSION_DEPTH: usize = 16;
pub const MAX_EVALUATION_STEPS: usize = 256;
pub const MAX_VALUE_BYTES: usize = 16 * 1024;

pub fn validate_schema(schema: &Schema) -> bool {
    fn walk(s: &Schema, depth: usize, steps: &mut usize) -> bool {
        *steps += 1;
        if depth > 16 || *steps > 256 {
            return false;
        }
        match s {
            Schema::String { max_bytes } => *max_bytes <= 4096,
            Schema::Array { items, max_items } => *max_items <= 32 && walk(items, depth + 1, steps),
            Schema::Object { properties } => {
                properties.len() <= MAX_PLAN_PORTS
                    && properties.values().all(|s| walk(s, depth + 1, steps))
            }
            _ => true,
        }
    }
    walk(schema, 0, &mut 0)
}
pub fn accepts(target: &Schema, source: &Schema) -> bool {
    match (target, source) {
        (Schema::String { max_bytes: a }, Schema::String { max_bytes: b }) => b <= a,
        (Schema::Array { items: a, max_items: n }, Schema::Array { items: b, max_items: m }) => {
            m <= n && accepts(a, b)
        }
        (Schema::Object { properties: a }, Schema::Object { properties: b }) => {
            a.len() == b.len() && a.iter().all(|(k, v)| b.get(k).is_some_and(|s| accepts(v, s)))
        }
        _ => target == source,
    }
}
pub fn validate_value(schema: &Schema, value: &Value) -> bool {
    if canonical_data(value).map_or(true, |b| b.len() > MAX_VALUE_BYTES) {
        return false;
    }
    match (schema, value) {
        (Schema::Boolean {}, Value::Bool(_)) => true,
        (Schema::Integer {}, Value::Number(v)) => v.as_i64().is_some(),
        (Schema::String { max_bytes }, Value::String(v)) => v.len() <= *max_bytes as usize,
        (Schema::Array { items, max_items }, Value::Array(v)) => {
            v.len() <= *max_items as usize && v.iter().all(|v| validate_value(items, v))
        }
        (Schema::Object { properties }, Value::Object(v)) => {
            v.len() == properties.len()
                && properties
                    .iter()
                    .all(|(k, s)| v.get(k.as_str()).is_some_and(|v| validate_value(s, v)))
        }
        (Schema::Resource {}, _) => {
            serde_json::from_value::<ResourceSelector>(value.clone()).is_ok()
        }
        _ => false,
    }
}
fn literal_schema(value: &Value, depth: usize) -> Result<Schema, CompileError> {
    if depth > 16 {
        return Err(error(ErrorKind::Bound, None, "literal depth"));
    }
    if serde_json::from_value::<ResourceSelector>(value.clone()).is_ok() {
        return Ok(Schema::Resource {});
    }
    let result = match value {
        Value::Bool(_) => Schema::Boolean {},
        Value::Number(n) if n.as_i64().is_some() => Schema::Integer {},
        Value::String(s) => Schema::String {
            max_bytes: s
                .len()
                .try_into()
                .map_err(|_| error(ErrorKind::Bound, None, "literal size"))?,
        },
        Value::Object(v) => Schema::Object {
            properties: v
                .iter()
                .map(|(k, v)| {
                    Ok((k.clone().try_into().map_err(wire)?, literal_schema(v, depth + 1)?))
                })
                .collect::<Result<_, CompileError>>()?,
        },
        // Empty arrays have no inferable element type in this initial profile.
        Value::Array(v) if !v.is_empty() => {
            let items = literal_schema(&v[0], depth + 1)?;
            if v.iter().any(|v| !validate_value(&items, v)) {
                return Err(error(ErrorKind::Schema, None, "heterogeneous array"));
            }
            Schema::Array {
                items: Box::new(items),
                max_items: v
                    .len()
                    .try_into()
                    .map_err(|_| error(ErrorKind::Bound, None, "array"))?,
            }
        }
        _ => return Err(error(ErrorKind::Schema, None, "unsupported literal")),
    };
    if !validate_schema(&result) || !validate_value(&result, value) {
        return Err(error(ErrorKind::Bound, None, "literal bound"));
    }
    Ok(result)
}
pub(super) fn infer(
    program: &Program,
    inputs: &BTreeMap<OpaqueRef, Schema>,
) -> Result<Schema, CompileError> {
    fn expr(
        e: &Expression,
        inputs: &BTreeMap<OpaqueRef, Schema>,
        depth: usize,
        steps: &mut usize,
    ) -> Result<Schema, CompileError> {
        *steps += 1;
        if depth > MAX_EXPRESSION_DEPTH || *steps > MAX_EVALUATION_STEPS {
            return Err(error(ErrorKind::Bound, None, "expression bound"));
        }
        let mut recurse = |e| expr(e, inputs, depth + 1, steps);
        Ok(match e {
            Expression::Input { port } => inputs
                .get(port)
                .cloned()
                .ok_or_else(|| error(ErrorKind::Schema, None, "undeclared expression input"))?,
            Expression::Literal { value } => literal_schema(value, 0)?,
            Expression::Project { value, field } => match recurse(value)? {
                Schema::Object { properties } => properties
                    .get(field)
                    .cloned()
                    .ok_or_else(|| error(ErrorKind::Schema, None, "unknown projection"))?,
                _ => return Err(error(ErrorKind::Schema, None, "projection requires object")),
            },
            Expression::Equal { left, right } => {
                let a = recurse(left)?;
                let b = recurse(right)?;
                if !accepts(&a, &b) && !accepts(&b, &a) {
                    return Err(error(ErrorKind::Schema, None, "equality type mismatch"));
                }
                Schema::Boolean {}
            }
            Expression::And { left, right } | Expression::Or { left, right } => {
                if recurse(left)? != (Schema::Boolean {}) || recurse(right)? != (Schema::Boolean {})
                {
                    return Err(error(ErrorKind::Schema, None, "Boolean operands required"));
                }
                Schema::Boolean {}
            }
            Expression::Not { value } => {
                if recurse(value)? != (Schema::Boolean {}) {
                    return Err(error(ErrorKind::Schema, None, "Boolean operand required"));
                }
                Schema::Boolean {}
            }
        })
    }
    if !program.requested_access.is_empty()
        || program.evaluator.as_str() != EVALUATOR
        || program.runtime_hash != ContentHash::of(EVALUATOR.as_bytes())
    {
        return Err(error(
            ErrorKind::HiddenIo,
            None,
            "unsupported deterministic execution profile",
        ));
    }
    let schema = expr(&program.expression, inputs, 0, &mut 0)?;
    if program.implementation_hash
        != Program::closed(program.expression.clone()).map_err(wire)?.implementation_hash
    {
        return Err(error(ErrorKind::HiddenIo, None, "program identity mismatch"));
    }
    Ok(schema)
}
fn eval(
    e: &Expression,
    inputs: &BTreeMap<OpaqueRef, Value>,
    depth: usize,
    steps: &mut usize,
) -> Result<Value, CompileError> {
    *steps += 1;
    if depth > MAX_EXPRESSION_DEPTH || *steps > MAX_EVALUATION_STEPS {
        return Err(error(ErrorKind::Bound, None, "evaluation bound"));
    }
    let mut recurse = |e| eval(e, inputs, depth + 1, steps);
    let v = match e {
        Expression::Input { port } => inputs
            .get(port)
            .cloned()
            .ok_or_else(|| error(ErrorKind::Schema, None, "missing materialized input"))?,
        Expression::Literal { value } => value.clone(),
        Expression::Project { value, field } => recurse(value)?
            .get(field.as_str())
            .cloned()
            .ok_or_else(|| error(ErrorKind::Schema, None, "projection"))?,
        Expression::Equal { left, right } => json!(recurse(left)? == recurse(right)?),
        Expression::And { left, right } => {
            let a = recurse(left)?;
            let b = recurse(right)?;
            json!(
                a.as_bool().ok_or_else(|| error(ErrorKind::Schema, None, "Boolean"))?
                    & b.as_bool().ok_or_else(|| error(ErrorKind::Schema, None, "Boolean"))?
            )
        }
        Expression::Or { left, right } => {
            let a = recurse(left)?;
            let b = recurse(right)?;
            json!(
                a.as_bool().ok_or_else(|| error(ErrorKind::Schema, None, "Boolean"))?
                    | b.as_bool().ok_or_else(|| error(ErrorKind::Schema, None, "Boolean"))?
            )
        }
        Expression::Not { value } => json!(!recurse(value)?.as_bool().ok_or_else(|| error(
            ErrorKind::Schema,
            None,
            "Boolean"
        ))?),
    };
    if canonical_data(&v).map_err(wire)?.len() > MAX_VALUE_BYTES {
        return Err(error(ErrorKind::Bound, None, "evaluation output size"));
    }
    Ok(v)
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Evaluation {
    pub value: Value,
    pub input_hash: ContentHash,
    pub output_hash: ContentHash,
    pub branch_outcome: Option<OpaqueRef>,
}
impl ExecutionPlan {
    /// Validate supplied values against pinned contracts. WIP-07 must also verify
    /// artifact bytes/hashes and select the actual producer from its checkpoint.
    pub fn validate_inputs(
        &self,
        node: &OpaqueRef,
        inputs: &BTreeMap<OpaqueRef, Value>,
    ) -> Result<(), CompileError> {
        let n = self
            .document()
            .body
            .nodes
            .iter()
            .find(|n| &n.id == node)
            .ok_or_else(|| error(ErrorKind::Reference, Some(node), "unknown operation"))?;
        if inputs.len() != n.inputs.len()
            || n.inputs.iter().any(|(p, i)| {
                inputs
                    .get(p)
                    .is_none_or(|v| !validate_value(&self.document().schemas[&i.schema].schema, v))
            })
        {
            return Err(error(ErrorKind::Schema, Some(node), "invalid materialized input"));
        }
        for (port, input) in &n.inputs {
            if let InputSource::Literal { value } = &input.source {
                if &inputs[port] != value {
                    return Err(error(
                        ErrorKind::Schema,
                        Some(node),
                        "materialized input differs from pinned literal",
                    ));
                }
            }
        }
        // Validate finite resource contracts for materialized input references too.
        for (port, input) in &n.inputs {
            if self.document().schemas[&input.schema].schema == (Schema::Resource {}) {
                let possibilities =
                    super::validation::input_resources(input, &self.document().body.nodes)?;
                if !possibilities.contains(
                    &serde_json::from_value(inputs[port].clone())
                        .map_err(|e| wire(e.to_string()))?,
                ) {
                    return Err(error(
                        ErrorKind::Authority,
                        Some(node),
                        "materialized resource outside declarations",
                    ));
                }
            }
        }
        Ok(())
    }
    /// Check connector/transform outputs before they become downstream inputs.
    pub fn validate_outputs(
        &self,
        node: &OpaqueRef,
        outputs: &BTreeMap<OpaqueRef, Value>,
    ) -> Result<(), CompileError> {
        let n = self
            .document()
            .body
            .nodes
            .iter()
            .find(|n| &n.id == node)
            .ok_or_else(|| error(ErrorKind::Reference, Some(node), "unknown operation"))?;
        if outputs.len() != n.outputs.len() {
            return Err(error(ErrorKind::Schema, Some(node), "materialized output ports"));
        }
        for (port, output) in &n.outputs {
            let value = outputs.get(port).ok_or_else(|| {
                error(ErrorKind::Schema, Some(node), "missing materialized output")
            })?;
            if !validate_value(&self.document().schemas[&output.schema].schema, value) {
                return Err(error(ErrorKind::Schema, Some(node), "output schema"));
            }
            if !output.resources.is_empty()
                && !output.resources.contains(
                    &serde_json::from_value(value.clone()).map_err(|e| wire(e.to_string()))?,
                )
            {
                return Err(error(
                    ErrorKind::Authority,
                    Some(node),
                    "output resource outside declared possibilities",
                ));
            }
        }
        Ok(())
    }
    /// WIP-07 supplies materialized immutable values; this function performs no I/O.
    pub fn evaluate(
        &self,
        node: &OpaqueRef,
        inputs: &BTreeMap<OpaqueRef, Value>,
    ) -> Result<Evaluation, CompileError> {
        let n = self
            .document()
            .body
            .nodes
            .iter()
            .find(|n| &n.id == node)
            .ok_or_else(|| error(ErrorKind::Reference, Some(node), "unknown operation"))?;
        self.validate_inputs(node, inputs)?;
        let (program, labels) = match &n.operation {
            PlanOperation::Transform { program } => (program, None),
            PlanOperation::Branch { program, when_true, when_false } => {
                (program, Some((when_true, when_false)))
            }
            _ => {
                return Err(error(
                    ErrorKind::HiddenIo,
                    Some(node),
                    "operation is not a pure program",
                ))
            }
        };
        let value = eval(&program.expression, inputs, 0, &mut 0)?;
        let branch_outcome = if let Some((a, b)) = labels {
            Some(if value.as_bool() == Some(true) { a.clone() } else { b.clone() })
        } else {
            self.validate_outputs(node, &BTreeMap::from([(opaque("value"), value.clone())]))?;
            None
        };
        Ok(Evaluation {
            input_hash: ContentHash::of(&canonical_data(inputs).map_err(wire)?),
            output_hash: ContentHash::of(&canonical_data(&value).map_err(wire)?),
            value,
            branch_outcome,
        })
    }
}

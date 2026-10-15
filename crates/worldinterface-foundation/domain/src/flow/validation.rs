use super::expression::{accepts, infer, validate_value};
use super::*;

type Graph<'a> = BTreeMap<&'a OpaqueRef, &'a PlanNode>;
type Ancestors = BTreeMap<OpaqueRef, BTreeSet<OpaqueRef>>;

pub(super) fn validate(p: &PlanDocument, c: &CompilationContext) -> Result<(), CompileError> {
    if p.version != PLAN_VERSION {
        return Err(error(ErrorKind::Version, None, "unsupported plan version"));
    }
    if p.registry_digest != c.registry_digest || p.schemas != c.schemas {
        return Err(error(ErrorKind::Registry, None, "registry or schema snapshot drift"));
    }
    if p.submitting_authority != c.submitting_authority
        || p.run_authority != c.run_authority
        || !p.body.authority.attenuates(&p.submitting_authority)
        || !p.body.authority.attenuates(&p.run_authority)
    {
        return Err(error(
            ErrorKind::Authority,
            None,
            "definition or persisted ceiling enlargement",
        ));
    }
    if p.body.nodes.is_empty()
        || p.body.nodes.len() > MAX_PLAN_NODES
        || p.body.entries.is_empty()
        || p.body.entries.len() > MAX_PLAN_NODES
        || p.body.outputs.len() > MAX_PLAN_PORTS
        || p.body.causal_metadata.len() > MAX_PLAN_PORTS
    {
        return Err(error(ErrorKind::Bound, None, "plan collection bound"));
    }
    match &p.origin {
        PlanOrigin::Direct => {
            if p.body.nodes.len() != 1
                || p.body.nodes[0].id != opaque("direct")
                || !matches!(p.body.nodes[0].operation, PlanOperation::Boundary { .. })
                || !p.body.outputs.is_empty()
                || !p.body.causal_metadata.is_empty()
            {
                return Err(error(ErrorKind::Version, None, "invalid direct plan"));
            }
        }
        PlanOrigin::Flow { id, version, predecessor, definition_digest } => {
            if *version == 0 || (*version == 1) != predecessor.is_none() {
                return Err(error(
                    ErrorKind::Version,
                    None,
                    "invalid definition version/predecessor",
                ));
            }
            let definition = FlowDefinition {
                id: id.clone(),
                version: *version,
                predecessor: predecessor.clone(),
                body: p.body.clone(),
            };
            if definition.digest().map_err(wire)? != *definition_digest {
                return Err(error(ErrorKind::Version, None, "definition digest mismatch"));
            }
        }
    }
    let graph: Graph = p.body.nodes.iter().map(|n| (&n.id, n)).collect();
    if graph.len() != p.body.nodes.len() {
        return Err(error(ErrorKind::Graph, None, "duplicate operation identity"));
    }
    for entry in &p.body.entries {
        if graph.get(entry).is_none_or(|n| !n.dependencies.is_empty() || !n.guards.is_empty()) {
            return Err(error(ErrorKind::Reference, Some(entry), "invalid entry"));
        }
    }
    for n in &p.body.nodes {
        if n.dependencies.len() > MAX_PLAN_PORTS
            || n.inputs.len() > MAX_PLAN_PORTS
            || n.outputs.len() > MAX_PLAN_PORTS
            || n.guards.len() > MAX_PLAN_PORTS
        {
            return Err(error(ErrorKind::Bound, Some(&n.id), "node collection bound"));
        }
        if n.fan_out != (FanOut::Expanded {}) {
            return Err(error(
                ErrorKind::Bound,
                Some(&n.id),
                "dynamic fan-out requires bounded expansion",
            ));
        }
        if !n.authority.attenuates(&p.body.authority) {
            return Err(error(
                ErrorKind::Authority,
                Some(&n.id),
                "node ceiling exceeds definition",
            ));
        }
        if n.dependencies.iter().any(|d| !graph.contains_key(d)) {
            return Err(error(ErrorKind::Reference, Some(&n.id), "missing dependency"));
        }
    }
    let ancestors = ancestors(&graph)?;
    for n in &p.body.nodes {
        if !p.body.entries.contains(&n.id)
            && !p.body.entries.iter().any(|e| ancestors[&n.id].contains(e))
        {
            return Err(error(
                ErrorKind::Unreachable,
                Some(&n.id),
                "node unreachable from declared entries",
            ));
        }
        for (branch, label) in &n.guards {
            let b = graph
                .get(branch)
                .ok_or_else(|| error(ErrorKind::Reference, Some(&n.id), "guard branch missing"))?;
            if !ancestors[&n.id].contains(branch) || !guards_subset(&b.guards, &n.guards) {
                return Err(error(ErrorKind::Reference, Some(&n.id), "guard branch unavailable"));
            }
            match &b.operation {
                PlanOperation::Branch { when_true, when_false, .. }
                    if label == when_true || label == when_false => {}
                _ => {
                    return Err(error(
                        ErrorKind::Reference,
                        Some(&n.id),
                        "undeclared branch alternative",
                    ))
                }
            }
        }
        for input in n.inputs.values() {
            check_input(input, n, &graph, &ancestors, p).map_err(|mut e| {
                if e.node.is_none() {
                    e.node = Some(n.id.clone());
                }
                e
            })?;
        }
        for output in n.outputs.values() {
            let s = schema(p, &output.schema).map_err(|mut e| {
                e.node = Some(n.id.clone());
                e
            })?;
            if output.resources.len() > MAX_PLAN_ALTERNATIVES
                || (s == &(Schema::Resource {})) == output.resources.is_empty()
            {
                return Err(error(
                    ErrorKind::Resource,
                    Some(&n.id),
                    "resource output requires bounded declared possibilities",
                ));
            }
        }
        validate_operation(n, &graph, &ancestors, p, c).map_err(|mut e| {
            e.node = Some(n.id.clone());
            e
        })?;
        for dependency in &n.dependencies {
            let producer = graph[dependency];
            if !guards_subset(&producer.guards, &n.guards)
                && !selected_dependency(dependency, n, &graph)
            {
                return Err(error(
                    if exclusive(producer, n) {
                        ErrorKind::Unreachable
                    } else {
                        ErrorKind::Reference
                    },
                    Some(&n.id),
                    "dependency guards must hold or be explicitly selected at an exhaustive join",
                ));
            }
        }
    }
    // A public Flow output is consumed after the graph, outside branch guards.
    let terminal = PlanNode {
        id: opaque("flow-output"),
        dependencies: vec![],
        guards: BTreeMap::new(),
        inputs: BTreeMap::new(),
        outputs: BTreeMap::new(),
        authority: AuthorityUpperBound::default(),
        fan_out: FanOut::Expanded {},
        operation: PlanOperation::Timer {
            deadline: Deadline {
                inclusive_at: Timestamp::from_epoch_millis(0).map_err(wire)?,
                action: TimeoutAction::Fail,
            },
        },
    };
    let mut all = ancestors.clone();
    all.insert(terminal.id.clone(), graph.keys().map(|k| (*k).clone()).collect());
    for output in p.body.outputs.values() {
        check_input(output, &terminal, &graph, &all, p)?;
    }
    let waits: Vec<_> = p
        .body
        .nodes
        .iter()
        .filter(|n| matches!(n.operation, PlanOperation::Wait { .. } | PlanOperation::Timer { .. }))
        .collect();
    for (i, a) in waits.iter().enumerate() {
        for b in waits.iter().skip(i + 1) {
            if exclusive(a, b) {
                continue;
            }
            if !ancestors[&a.id].contains(&b.id) && !ancestors[&b.id].contains(&a.id) {
                return Err(error(ErrorKind::Wait, Some(&b.id), "potentially concurrent waits"));
            }
            if let (PlanOperation::Wait { signal: sa }, PlanOperation::Wait { signal: sb }) =
                (&a.operation, &b.operation)
            {
                if (&sa.tenant, &sa.namespace, &sa.signal_kind, &sa.correlation)
                    == (&sb.tenant, &sb.namespace, &sb.signal_kind, &sb.correlation)
                {
                    let (prior, next) =
                        if ancestors[&b.id].contains(&a.id) { (*a, *b) } else { (*b, *a) };
                    let advances = generation(prior, &graph) != generation(next, &graph)
                        || follows_handled(next, &prior.id, &graph);
                    if !advances {
                        return Err(error(
                            ErrorKind::Wait,
                            Some(&b.id),
                            "repeated wait must advance cursor or correlation generation",
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}
fn ancestors(graph: &Graph<'_>) -> Result<Ancestors, CompileError> {
    fn visit(
        id: &OpaqueRef,
        g: &Graph<'_>,
        active: &mut BTreeSet<OpaqueRef>,
        done: &mut Ancestors,
    ) -> Result<BTreeSet<OpaqueRef>, CompileError> {
        if let Some(v) = done.get(id) {
            return Ok(v.clone());
        }
        if !active.insert(id.clone()) {
            return Err(error(ErrorKind::Graph, Some(id), "dependency cycle"));
        }
        let mut set = BTreeSet::new();
        for d in &g[id].dependencies {
            set.extend(visit(d, g, active, done)?);
            set.insert(d.clone());
        }
        active.remove(id);
        done.insert(id.clone(), set.clone());
        Ok(set)
    }
    let mut done = BTreeMap::new();
    for id in graph.keys() {
        visit(id, graph, &mut BTreeSet::new(), &mut done)?;
    }
    Ok(done)
}
fn guards_subset(a: &BTreeMap<OpaqueRef, OpaqueRef>, b: &BTreeMap<OpaqueRef, OpaqueRef>) -> bool {
    a.iter().all(|(k, v)| b.get(k) == Some(v))
}
fn exclusive(a: &PlanNode, b: &PlanNode) -> bool {
    a.guards.iter().any(|(k, v)| b.guards.get(k).is_some_and(|w| v != w))
}
// Called only after every exclusive selection has passed selection(). A join
// waits for both alternatives to settle, then reads only the chosen producer.
fn selected_dependency(dependency: &OpaqueRef, n: &PlanNode, g: &Graph<'_>) -> bool {
    let selected = |branch: &OpaqueRef, a: &OutputRef, b: &OutputRef| {
        let PlanOperation::Branch { when_true, when_false, .. } = &g[branch].operation else {
            return false;
        };
        [(a, when_true), (b, when_false)].iter().any(|(r, label)| {
            let mut required = n.guards.clone();
            required.insert(branch.clone(), (*label).clone());
            &r.node == dependency && guards_subset(&g[dependency].guards, &required)
        })
    };
    n.inputs.values().any(|i| match &i.source {
        InputSource::Exclusive { branch, when_true, when_false } => {
            selected(branch, when_true, when_false)
        }
        _ => false,
    }) || match &n.operation {
        PlanOperation::Boundary {
            resource: ResourceBinding::Exclusive { branch, when_true, when_false },
            ..
        } => selected(branch, when_true, when_false),
        _ => false,
    }
}
fn schema<'a>(p: &'a PlanDocument, id: &OpaqueRef) -> Result<&'a Schema, CompileError> {
    p.schemas
        .get(id)
        .map(|s| &s.schema)
        .ok_or_else(|| error(ErrorKind::Schema, None, "unknown pinned schema"))
}
fn output<'a>(r: &OutputRef, g: &Graph<'a>) -> Result<&'a PlanOutput, CompileError> {
    g.get(&r.node)
        .and_then(|n| n.outputs.get(&r.port))
        .ok_or_else(|| error(ErrorKind::Reference, Some(&r.node), "unknown output"))
}
fn available(
    r: &OutputRef,
    n: &PlanNode,
    g: &Graph<'_>,
    anc: &Ancestors,
) -> Result<(), CompileError> {
    if !anc[&n.id].contains(&r.node) || !guards_subset(&g[&r.node].guards, &n.guards) {
        return Err(error(
            ErrorKind::Reference,
            Some(&n.id),
            "output unavailable in this branch or dependency",
        ));
    }
    Ok(())
}
fn selection(
    branch: &OpaqueRef,
    a: &OutputRef,
    b: &OutputRef,
    n: &PlanNode,
    g: &Graph<'_>,
    anc: &Ancestors,
) -> Result<(), CompileError> {
    output(a, g)?;
    output(b, g)?;
    let br = g
        .get(branch)
        .ok_or_else(|| error(ErrorKind::Reference, Some(&n.id), "selection branch missing"))?;
    let PlanOperation::Branch { when_true, when_false, .. } = &br.operation else {
        return Err(error(ErrorKind::Reference, Some(&n.id), "selection needs branch"));
    };
    if !anc[&n.id].contains(branch)
        || !guards_subset(&br.guards, &n.guards)
        || n.guards.contains_key(branch)
    {
        return Err(error(ErrorKind::Reference, Some(&n.id), "selection branch unavailable"));
    }
    for (r, label) in [(a, when_true), (b, when_false)] {
        if !anc[&n.id].contains(&r.node) {
            return Err(error(ErrorKind::Reference, Some(&n.id), "selection producer unavailable"));
        }
        let mut required = n.guards.clone();
        required.insert(branch.clone(), label.clone());
        let producer = &g[&r.node];
        if producer.guards.get(branch) != Some(label) || !guards_subset(&producer.guards, &required)
        {
            return Err(error(
                ErrorKind::Resource,
                Some(&n.id),
                "selection producers are not exclusive and exhaustive",
            ));
        }
    }
    Ok(())
}
fn check_input(
    i: &PlanInput,
    n: &PlanNode,
    g: &Graph<'_>,
    anc: &Ancestors,
    p: &PlanDocument,
) -> Result<(), CompileError> {
    let target = schema(p, &i.schema)?;
    let refs = match &i.source {
        InputSource::Literal { value } => {
            if !validate_value(target, value) {
                return Err(error(ErrorKind::Schema, Some(&n.id), "literal schema mismatch"));
            }
            vec![]
        }
        InputSource::Artifact { reference } => {
            // Only a reference is checked. Materialized bytes must be checked by WIP-07.
            let limit = if matches!(
                n.operation,
                PlanOperation::Transform { .. } | PlanOperation::Branch { .. }
            ) {
                super::MAX_VALUE_BYTES as u64
            } else {
                match target {
                    Schema::Artifact { max_bytes, .. } => *max_bytes,
                    _ => MAX_BOUNDARY_ARTIFACT_BYTES,
                }
            };
            if reference.size > limit || target == &(Schema::Resource {}) {
                return Err(error(
                    ErrorKind::Bound,
                    Some(&n.id),
                    "artifact size or unbounded resource artifact",
                ));
            }
            vec![]
        }
        InputSource::Output { source } => {
            output(source, g)?;
            available(source, n, g, anc)?;
            vec![source]
        }
        InputSource::Exclusive { branch, when_true, when_false } => {
            selection(branch, when_true, when_false, n, g, anc)?;
            vec![when_true, when_false]
        }
    };
    for r in refs {
        if !accepts(target, schema(p, &output(r, g)?.schema)?) {
            return Err(error(ErrorKind::Schema, Some(&n.id), "edge schema mismatch"));
        }
    }
    Ok(())
}
pub(super) fn input_resources(
    i: &PlanInput,
    nodes: &[PlanNode],
) -> Result<Vec<ResourceSelector>, CompileError> {
    let g: Graph = nodes.iter().map(|n| (&n.id, n)).collect();
    match &i.source {
        InputSource::Literal { value } => {
            Ok(vec![serde_json::from_value(value.clone()).map_err(|e| wire(e.to_string()))?])
        }
        InputSource::Output { source } => Ok(output(source, &g)?.resources.clone()),
        InputSource::Exclusive { when_true, when_false, .. } => Ok(output(when_true, &g)?
            .resources
            .iter()
            .chain(&output(when_false, &g)?.resources)
            .cloned()
            .collect()),
        _ => Err(error(ErrorKind::Resource, None, "resource cannot come from unseen artifact")),
    }
}
fn resource_possibilities(
    r: &ResourceBinding,
    n: &PlanNode,
    g: &Graph<'_>,
    anc: &Ancestors,
    p: &PlanDocument,
) -> Result<Vec<ResourceSelector>, CompileError> {
    let refs = match r {
        ResourceBinding::Fixed { resource } => return Ok(vec![resource.clone()]),
        ResourceBinding::Produced { producers } => {
            if producers.len() != 1 {
                return Err(error(
                    ErrorKind::Resource,
                    Some(&n.id),
                    "resource needs one unambiguous producer",
                ));
            }
            output(&producers[0], g)?;
            available(&producers[0], n, g, anc)?;
            vec![&producers[0]]
        }
        ResourceBinding::Exclusive { branch, when_true, when_false } => {
            selection(branch, when_true, when_false, n, g, anc)?;
            vec![when_true, when_false]
        }
    };
    let mut resources = vec![];
    for r in refs {
        let o = output(r, g)?;
        if schema(p, &o.schema)? != &(Schema::Resource {}) || o.resources.is_empty() {
            return Err(error(ErrorKind::Resource, Some(&n.id), "not a declared resource output"));
        }
        resources.extend(o.resources.clone());
    }
    if resources.len() > MAX_PLAN_ALTERNATIVES {
        return Err(error(ErrorKind::Bound, Some(&n.id), "resource alternative bound"));
    }
    Ok(resources)
}
fn deadline(d: &Deadline) -> Result<(), CompileError> {
    if &d.inclusive_at.as_str()[20..29] != "000000000" || d.inclusive_at.epoch_millis().is_err() {
        return Err(error(
            ErrorKind::Wait,
            None,
            "deadline needs representable absolute second precision",
        ));
    }
    Ok(())
}
fn validate_operation(
    n: &PlanNode,
    g: &Graph<'_>,
    anc: &Ancestors,
    p: &PlanDocument,
    c: &CompilationContext,
) -> Result<(), CompileError> {
    match &n.operation {
        PlanOperation::Boundary { binding, resource, policy } => {
            let pinned = c.binding(&binding.descriptor.connector, &binding.descriptor.operation)?;
            if **binding != pinned {
                return Err(error(ErrorKind::Registry, None, "connector binding drift"));
            }
            let d = &binding.descriptor;
            if n.inputs.len() != 1
                || n.outputs.len() != 1
                || n.inputs.get(&opaque("input")).is_none_or(|i| i.schema != d.input_schema)
                || n.outputs.get(&opaque("output")).is_none_or(|o| o.schema != d.output_schema)
            {
                return Err(error(
                    ErrorKind::Schema,
                    None,
                    "connector ports differ from descriptor",
                ));
            }
            if policy.budget.max_attempts == 0
                || policy.budget.max_attempts > 32
                || policy.budget.timeout_millis == 0
                || policy.budget.timeout_millis > 86_400_000
            {
                return Err(error(ErrorKind::Bound, None, "boundary policy bound"));
            }
            for r in resource_possibilities(resource, n, g, anc, p)? {
                let a = AuthorityAlternative {
                    connector: d.connector.clone(),
                    operation: d.operation.clone(),
                    effect: d.effect_class.clone(),
                    resource: r.clone(),
                };
                if r.namespace != d.resource_namespace || !n.authority.alternatives.contains(&a) {
                    return Err(error(
                        ErrorKind::Authority,
                        None,
                        "resource/effect outside paired authority ceilings",
                    ));
                }
            }
        }
        PlanOperation::Transform { program } | PlanOperation::Branch { program, .. } => {
            if !n.authority.alternatives.is_empty() {
                return Err(error(
                    ErrorKind::Authority,
                    None,
                    "pure nodes have no boundary authority",
                ));
            }
            let inputs = n
                .inputs
                .iter()
                .map(|(port, i)| Ok((port.clone(), schema(p, &i.schema)?.clone())))
                .collect::<Result<_, CompileError>>()?;
            let result = infer(program, &inputs)?;
            if let PlanOperation::Branch { when_true, when_false, .. } = &n.operation {
                if result != (Schema::Boolean {})
                    || when_true == when_false
                    || !n.outputs.is_empty()
                {
                    return Err(error(
                        ErrorKind::Schema,
                        None,
                        "branch requires Boolean and two distinct declared alternatives",
                    ));
                }
            } else {
                let out = n.outputs.get(&opaque("value")).ok_or_else(|| {
                    error(ErrorKind::Schema, None, "transform output value missing")
                })?;
                if n.outputs.len() != 1 || !accepts(schema(p, &out.schema)?, &result) {
                    return Err(error(ErrorKind::Schema, None, "transform output schema mismatch"));
                }
                if result == (Schema::Resource {}) {
                    let possibilities = match &program.expression {
                        Expression::Input { port } => {
                            input_resources(&n.inputs[port], &p.body.nodes)?
                        }
                        Expression::Literal { value } => {
                            vec![serde_json::from_value(value.clone())
                                .map_err(|e| wire(e.to_string()))?]
                        }
                        _ => {
                            return Err(error(
                                ErrorKind::Resource,
                                None,
                                "resource transform needs provable finite output",
                            ))
                        }
                    };
                    if possibilities.iter().any(|r| !out.resources.contains(r)) {
                        return Err(error(
                            ErrorKind::Resource,
                            None,
                            "transform output omits possible resource",
                        ));
                    }
                }
            }
        }
        PlanOperation::Wait { signal } => {
            structural_wait(n)?;
            if [
                signal.tenant.as_str(),
                signal.namespace.as_str(),
                signal.signal_kind.as_str(),
                signal.correlation.as_str(),
            ]
            .iter()
            .any(|v| v.contains('*'))
            {
                return Err(error(
                    ErrorKind::Wait,
                    None,
                    "signal routing requires exact correlation and scope",
                ));
            }
            if matches!(signal.progress, SignalProgress::AfterSequence { sequence: 0 }) {
                return Err(error(ErrorKind::Wait, None, "invalid zero repeat cursor"));
            }
            if let SignalProgress::AfterHandled { wait } = &signal.progress {
                let prior = g
                    .get(wait)
                    .ok_or_else(|| error(ErrorKind::Wait, None, "handled cursor wait missing"))?;
                let PlanOperation::Wait { signal: previous } = &prior.operation else {
                    return Err(error(
                        ErrorKind::Wait,
                        None,
                        "handled cursor requires signal wait",
                    ));
                };
                if previous.deadline.as_ref().is_some_and(|d| d.action == TimeoutAction::Continue) {
                    return Err(error(
                        ErrorKind::Wait,
                        None,
                        "handled cursor may be unavailable after timeout continuation",
                    ));
                }
                if !anc[&n.id].contains(wait)
                    || !guards_subset(&prior.guards, &n.guards)
                    || !same_route(signal, previous)
                {
                    return Err(error(
                        ErrorKind::Wait,
                        None,
                        "handled cursor must name an available earlier wait on the same route",
                    ));
                }
            }
            if let Some(d) = &signal.deadline {
                deadline(d)?;
            }
        }
        PlanOperation::Timer { deadline: d } => {
            structural_wait(n)?;
            deadline(d)?;
        }
    }
    Ok(())
}
fn structural_wait(n: &PlanNode) -> Result<(), CompileError> {
    if !n.inputs.is_empty() || !n.outputs.is_empty() || !n.authority.alternatives.is_empty() {
        return Err(error(
            ErrorKind::Wait,
            None,
            "wait structure has no inputs, outputs or boundary authority",
        ));
    }
    Ok(())
}

fn same_route(a: &SignalWait, b: &SignalWait) -> bool {
    (&a.tenant, &a.namespace, &a.signal_kind, &a.correlation)
        == (&b.tenant, &b.namespace, &b.signal_kind, &b.correlation)
}
// All AfterHandled edges have already been checked as graph ancestors, so these
// walks terminate within the 64-node profile. Generation is part of exact routing.
fn generation<'a>(node: &'a PlanNode, graph: &Graph<'a>) -> Option<&'a OpaqueRef> {
    match &node.operation {
        PlanOperation::Wait {
            signal: SignalWait { progress: SignalProgress::FreshGeneration { generation }, .. },
        } => Some(generation),
        PlanOperation::Wait {
            signal: SignalWait { progress: SignalProgress::AfterHandled { wait }, .. },
        } => generation(graph[wait], graph),
        _ => None,
    }
}
fn follows_handled(node: &PlanNode, prior: &OpaqueRef, graph: &Graph<'_>) -> bool {
    match &node.operation {
        PlanOperation::Wait {
            signal: SignalWait { progress: SignalProgress::AfterHandled { wait }, .. },
        } => wait == prior || follows_handled(graph[wait], prior, graph),
        _ => false,
    }
}

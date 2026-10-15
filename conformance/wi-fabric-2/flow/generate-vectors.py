#!/usr/bin/env python3
"""Independent wi-json-1 vector construction, Python stdlib only; no Rust output."""
import hashlib
import json
from pathlib import Path

BASE = Path(__file__).resolve().parent

def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()

def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()

descriptor = dict(version=1, connector="fixture", implementation=hashlib.sha256(b"connector").hexdigest(),
                  operation="write", effect_class="mutate", resource_namespace="fixture",
                  resource_grammar="fixture-1", input_schema="bool", output_schema="bool",
                  isolation_profile="in-process", secret_refs=[], idempotency="naturally_idempotent",
                  retry_after_noncommit=True, observes_commit=True,
                  reconciliation=dict(max_observations=3, backoff_millis=1, timeout_millis=100, automatic=True),
                  cancellation_supported=False, evidence_schema="evidence-1", max_evidence_bytes=4096)
schemas = {"bool": {"type": "boolean"}, "int": {"type": "integer"}, "resource": {"type": "resource"}}
resource = lambda name: dict(namespace="fixture", selector=name)
authority = dict(alternatives=[dict(connector="fixture", operation="write", effect="mutate", resource=resource(n)) for n in ["a", "b"]])
context = dict(descriptors=[descriptor], schemas=schemas, submitting_authority=authority, run_authority=authority)
(BASE / "context.json").write_bytes(json.dumps(context, indent=2).encode() + b"\n")
body = dict(entries=["timer"], nodes=[dict(id="timer", dependencies=[], guards={}, inputs={}, outputs={},
            authority=dict(alternatives=[]), fan_out=dict(kind="expanded"), operation=dict(kind="timer", deadline=dict(inclusive_at="2030-01-01T00:00:00.000000000Z", action="fail")))],
            outputs={}, authority=dict(alternatives=[]), causal_metadata={})
definition = dict(id="golden", version=1, body=body)

registry_digest = digest(["wi-connector-registry:1", [descriptor]])
schema_pins = {k: dict(schema=v, digest=digest(["wi-schema-1", v])) for k, v in schemas.items()}

def write_plan(name, body, origin):
    plan = dict(version=1, origin=origin, registry_digest=registry_digest, schemas=schema_pins,
                submitting_authority=authority, run_authority=authority, body=body)
    envelope = dict(contract="WI-FABRIC-2", canonical_profile="wi-json-1", schema=1, record_kind="execution_plan", plan=plan)
    encoded = canonical(envelope)
    (BASE / f"{name}.canonical.json").write_bytes(encoded)
    (BASE / f"{name}.sha256").write_text(hashlib.sha256(b"WI-FABRIC-2\0wi-json-1\0execution-plan-1\0" + encoded).hexdigest() + "\n")

def flow_origin(definition):
    return dict(kind="flow", id=definition["id"], version=definition["version"],
                definition_digest=digest(["wi-flow-definition-1", definition]))

write_plan("plan", body, flow_origin(definition))

# F-003: independently cover all boundary fields and the Direct origin.
binding = dict(version=1, descriptor=descriptor, descriptor_hash=digest(descriptor), registry_digest=registry_digest)
policy = dict(budget=dict(max_attempts=3, timeout_millis=100), receipt_profile="receipt-1",
              deadline="2030-01-01T00:00:00.000000000Z", expected_outcome="written")
def boundary(name, resource_binding, dependencies=None):
    return dict(id=name, dependencies=dependencies or [], guards={},
                inputs=dict(input=dict(schema="bool", source=dict(kind="literal", value=True))),
                outputs=dict(output=dict(schema="bool", resources=[])), authority=authority,
                fan_out=dict(kind="expanded"), operation=dict(kind="boundary", binding=binding,
                resource=resource_binding, policy=policy))

direct = boundary("direct", dict(kind="fixed", resource=resource("a")))
direct["inputs"]["input"]["source"] = dict(kind="artifact", reference=dict(scope="tenant", content=hashlib.sha256(b"true").hexdigest(), size=4))
write_plan("direct", dict(entries=["direct"], nodes=[direct], outputs={}, authority=authority, causal_metadata={}), dict(kind="direct"))

# Programs, guard labels, resource outputs, an exhaustive join, causal metadata.
def program(expression):
    return dict(evaluator="wi-expr-1", runtime_hash=hashlib.sha256(b"wi-expr-1").hexdigest(),
                implementation_hash=digest(["wi-expr-1", expression]), expression=expression, requested_access=[])

def producer(name, label):
    return dict(id=name, dependencies=["branch"], guards=dict(branch=label), inputs={},
                outputs=dict(value=dict(schema="resource", resources=[resource(name)])),
                authority=dict(alternatives=[]), fan_out=dict(kind="expanded"),
                operation=dict(kind="transform", program=program(dict(op="literal", value=resource(name)))))

branch = dict(id="branch", dependencies=[], guards={}, inputs={}, outputs={}, authority=dict(alternatives=[]),
              fan_out=dict(kind="expanded"), operation=dict(kind="branch", program=program(dict(op="literal", value=True)), when_true="yes", when_false="no"))
selection = dict(kind="exclusive", branch="branch", when_true=dict(node="a", port="value"), when_false=dict(node="b", port="value"))
body = dict(entries=["branch"], nodes=[producer("a", "yes"), producer("b", "no"), branch,
            boundary("c", selection, ["a", "b"])], outputs={}, authority=authority, causal_metadata=dict(cause="golden-cause"))
definition = dict(id="golden-branch", version=1, body=body)
write_plan("branch", body, flow_origin(definition))

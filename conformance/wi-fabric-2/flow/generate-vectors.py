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
plan = dict(version=1, origin=dict(kind="flow", id="golden", version=1, definition_digest=digest(["wi-flow-definition-1", definition])),
            registry_digest=digest(["wi-connector-registry:1", [descriptor]]),
            schemas={k: dict(schema=v, digest=digest(["wi-schema-1", v])) for k, v in schemas.items()},
            submitting_authority=authority, run_authority=authority, body=body)
envelope = dict(contract="WI-FABRIC-2", canonical_profile="wi-json-1", schema=1, record_kind="execution_plan", plan=plan)
encoded = canonical(envelope)
(BASE / "plan.canonical.json").write_bytes(encoded)
(BASE / "plan.sha256").write_text(hashlib.sha256(b"WI-FABRIC-2\0wi-json-1\0execution-plan-1\0" + encoded).hexdigest() + "\n")

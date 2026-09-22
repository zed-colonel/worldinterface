//! A deliberately bounded JSON Schema validator for the repository-owned schemas.
//! Unknown schema keywords fail closed; this is not a general JSON Schema engine.
use crate::{array, hash, read, string, Result};
use regex::Regex;
use serde_json::Value;
use std::{fs, path::Path};

pub fn validate(schema: &Value, v: &Value, path: &str) -> Result<()> {
    let supported = [
        "$schema",
        "type",
        "properties",
        "required",
        "additionalProperties",
        "const",
        "enum",
        "pattern",
        "minLength",
        "minimum",
        "minItems",
        "maxItems",
        "items",
    ];
    for k in schema.as_object().ok_or("schema is not an object")?.keys() {
        if !supported.contains(&k.as_str()) {
            return Err(format!("unsupported schema keyword {k}").into());
        }
    }
    if let Some(t) = schema["type"].as_str() {
        let valid = match t {
            "object" => v.is_object(),
            "array" => v.is_array(),
            "string" => v.is_string(),
            "integer" => v.as_i64().is_some() || v.as_u64().is_some(),
            "boolean" => v.is_boolean(),
            "null" => v.is_null(),
            _ => return Err(format!("unsupported schema type {t}").into()),
        };
        if !valid {
            return Err(format!("{path}: expected {t}").into());
        }
    }
    if let Some(c) = schema.get("const") {
        if c != v {
            return Err(format!("{path}: wrong constant").into());
        }
    }
    if let Some(e) = schema["enum"].as_array() {
        if !e.contains(v) {
            return Err(format!("{path}: unsupported value").into());
        }
    }
    if let Some(s) = v.as_str() {
        if let Some(p) = schema["pattern"].as_str() {
            if !Regex::new(p)?.is_match(s) {
                return Err(format!("{path}: invalid format").into());
            }
        }
        if s.chars().count() < (schema["minLength"].as_u64().unwrap_or(0) as usize) {
            return Err(format!("{path}: empty string").into());
        }
    }
    if let Some(min) = schema["minimum"].as_i64() {
        if v.as_i64().unwrap_or(i64::MAX) < min {
            return Err(format!("{path}: below minimum").into());
        }
    }
    if let Some(a) = v.as_array() {
        if a.len() < schema["minItems"].as_u64().unwrap_or(0) as usize
            || schema["maxItems"].as_u64().is_some_and(|n| a.len() > n as usize)
        {
            return Err(format!("{path}: invalid array length").into());
        }
        if let Some(s) = schema.get("items") {
            for (i, x) in a.iter().enumerate() {
                validate(s, x, &format!("{path}/{i}"))?
            }
        }
    }
    if let Some(o) = v.as_object() {
        if let Some(required) = schema["required"].as_array() {
            for k in required {
                let k = k.as_str().ok_or("invalid required key")?;
                if !o.contains_key(k) {
                    return Err(format!("{path}: missing {k}").into());
                }
            }
        }
        for (k, x) in o {
            if let Some(s) = schema["properties"].get(k) {
                validate(s, x, &format!("{path}/{k}"))?
            } else if schema["additionalProperties"] == false {
                return Err(format!("{path}: unexpected {k}").into());
            }
        }
    }
    Ok(())
}
pub fn artifact(base: &Path, v: &Value) -> Result<()> {
    let p = base.join(string(v, "path")?);
    let bytes = fs::read(&p)?;
    if bytes.is_empty() || hash(&bytes) != string(v, "sha256")? {
        return Err(format!("artifact digest mismatch: {}", p.display()).into());
    }
    Ok(())
}
pub fn runtime(root: &Path, receipt: &Value, artifact_base: &Path) -> Result<()> {
    validate(
        &read(&root.join("conformance/wi-fabric-2/evidence.schema.json"))?,
        receipt,
        "receipt",
    )?;
    let manifest = read(&root.join("conformance/wi-fabric-2/manifest.yaml"))?;
    let id = string(receipt, "case_id")?;
    let case =
        array(&manifest, "cases")?.iter().find(|c| c["id"] == id).ok_or("unknown runtime case")?;
    if case["status"] != "implemented" {
        return Err("planned/unimplemented cases cannot pass".into());
    }
    if receipt["upstream"] != manifest["upstream_target"] {
        return Err("execution upstream differs from current target (historical identities cannot substitute)".into());
    }
    if receipt["fixture_manifest_sha256"] != manifest["fixture_manifest_sha256"] {
        return Err("stale fixture identity".into());
    }
    let imported =
        read(&root.join("contracts/sources/aq-baseline-integration-acceptance-matrix.yaml"))?;
    let source = array(&imported, "cases")?
        .iter()
        .find(|v| v["id"] == id)
        .ok_or("missing imported source case")?;
    if receipt["source_record_sha256"] != hash(serde_json::to_vec(source)?) {
        return Err("source record digest does not bind imported case".into());
    }
    if receipt["executable_case_sha256"] != hash(fs::read(root.join(string(case, "test_source")?))?)
        || receipt["integration_fixture_sha256"] != manifest["integration_fixture_sha256"]
    {
        return Err("stale executable/fixture binding".into());
    }
    if receipt["producer"] == receipt["independent_review"]["reviewer"] {
        return Err("producer cannot independently review its own result".into());
    }
    artifact(artifact_base, &receipt["upstream_build_receipt"])?;
    artifact(artifact_base, &receipt["command"]["log"])?;
    if receipt["command"]["exit_code"] != 0 {
        return Err("failed command cannot establish passing runtime evidence".into());
    }
    for a in array(receipt, "results")? {
        artifact(artifact_base, a)?;
        let result = read(&artifact_base.join(string(a, "path")?))?;
        if result["status"] != "passed" || result["tests_passed"].as_u64().unwrap_or(0) == 0 {
            return Err("empty or nonpassing case results".into());
        }
        for key in ["case_id", "consumer_revision", "source_record_sha256", "test_run_id"] {
            if result[key] != receipt[key] {
                return Err(format!("result mismatch: {key}").into());
            }
        }
    }
    artifact(artifact_base, &receipt["independent_review"]["receipt"])?;
    // The upstream build receipt is an independent record, not just a digest-shaped field.
    let build = read(&artifact_base.join(string(&receipt["upstream_build_receipt"], "path")?))?;
    if build["upstream"] != receipt["upstream"]
        || build["consumer_revision"] != receipt["consumer_revision"]
        || build["status"] != "passed"
    {
        return Err("upstream build receipt identity/status mismatch".into());
    }
    if build["resolved_packages_sha256"] != receipt["resolved_packages_sha256"]
        || build["packages"].as_array().is_none_or(|v| v.is_empty())
        || build["command"]["exit_code"] != 0
    {
        return Err("empty or mismatched AQ build receipt".into());
    }
    let expected = [
        "actionqueue-core",
        "actionqueue-storage",
        "actionqueue-runtime",
        "actionqueue-engine",
        "actionqueue-workflow",
        "actionqueue-executor-local",
    ];
    let mut names = std::collections::BTreeSet::new();
    for package in array(&build, "packages")? {
        let name = string(package, "name")?;
        if !expected.contains(&name)
            || !names.insert(name)
            || package["version"] != "0.2.0"
            || array(package, "features")?
                .iter()
                .any(|v| !["default", "serde"].contains(&v.as_str().unwrap_or("")))
        {
            return Err("unexpected AQ package/version/feature".into());
        }
    }
    if names != expected.into_iter().collect() {
        return Err("incomplete AQ build graph".into());
    }
    artifact(artifact_base, &build["command"]["log"])?;
    if hash(serde_json::to_vec(&build["packages"])?) != string(receipt, "resolved_packages_sha256")?
    {
        return Err("resolved package/features digest mismatch".into());
    }
    let review =
        read(&artifact_base.join(string(&receipt["independent_review"]["receipt"], "path")?))?;
    for key in [
        "case_id",
        "consumer_revision",
        "source_record_sha256",
        "upstream",
        "test_run_id",
        "executable_case_sha256",
        "integration_fixture_sha256",
        "resolved_packages_sha256",
        "results",
        "command",
        "environment_sha256",
        "toolchain",
    ] {
        if review[key] != receipt[key] {
            return Err(format!("independent review missing/mismatching {key}").into());
        }
    }
    if review["status"] != "passed"
        || review["reviewer"] != receipt["independent_review"]["reviewer"]
    {
        return Err("independent reviewed passing result required".into());
    }
    Ok(())
}
pub fn publication(root: &Path, v: &Value, base: &Path) -> Result<()> {
    let completion = v["kind"] == "publication-completion";
    let file = if completion { "completion" } else { "preparation" };
    validate(&read(&root.join(format!("docs/release/{file}.schema.json")))?, v, "publication")?;
    if completion {
        artifact(base, &v["preparation"])?;
        let p = base.join(string(&v["preparation"], "path")?);
        let prep = read(&p)?;
        if prep["kind"] != "publication-preparation" {
            return Err("completion must reference separate preparation receipt".into());
        }
        publication(root, &prep, p.parent().unwrap())?;
        if prep["source_revision"] != v["source_revision"] {
            return Err("publication source differs from preparation".into());
        }
        for k in ["script", "observed_publication_state"] {
            artifact(base, &v[k])?
        }
        artifact(base, &v["command"]["log"])?;
        if v["command"]["exit_code"] != 0 {
            return Err("publication action failed".into());
        }
    } else {
        for k in ["artifacts", "verification_receipts"] {
            for a in array(v, k)? {
                artifact(base, a)?
            }
        }
        for c in array(v, "commands")? {
            artifact(base, &c["log"])?;
            if c["exit_code"] != 0 {
                return Err("preparation command failed".into());
            }
        }
    }
    Ok(())
}

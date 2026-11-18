//! Retained producer-fixture observations, separate from consumer Kata qualification.
use crate::{hash, proof::TestRef, read, string, worker_campaign::Outcome, Result};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Component, Path, PathBuf},
};

pub const MAX_BYTES: u64 = 65536;
pub fn scenarios() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        ("WP-035", "restart", "wp_035_authenticated_supervisor_binds_exact_lifecycle_operation"),
        ("WP-036", "restart", "wp_036_acceptance_is_uncertain_until_external_transition_completes"),
        ("WP-037", "restart", "wp_037_external_result_survives_stopped_guest_and_wi_restart"),
        (
            "WP-038",
            "fabric-receipt-history",
            "wp_038_lost_ack_and_callback_converge_through_durable_inspect",
        ),
        (
            "WP-038",
            "fabric-receipt-committed",
            "wp_038_lost_ack_and_callback_converge_through_durable_inspect",
        ),
        (
            "WP-038",
            "reconciliation-transition-committed",
            "wp_038_lost_ack_and_callback_converge_through_durable_inspect",
        ),
        ("WP-039", "restart", "wp_039_operator_recovery_is_independent_of_failed_guest"),
    ]
}
fn name(case: &str, scenario: &str) -> String {
    format!("supervisor-{case}-{scenario}.json")
}
fn owner(test: &str) -> TestRef {
    TestRef {
        manifest: crate::proof::RUNTIME.into(),
        package: "worldinterface-runtime".into(),
        test: test.into(),
    }
}
/// Missing files/outcomes remain explicit in the failed receipt for diagnosis.
pub fn bindings(base: &Path, outcomes: &[Outcome]) -> Value {
    scenarios()
        .into_iter()
        .map(|(case, scenario, test)| {
            let name = name(case, scenario);
            let outcome = outcomes.iter().find(|o| o.reference == owner(test));
            (
                name.clone(),
                json!({"sha256":fs::read(base.join(name)).ok().map(hash),
            "owner":owner(test),"executable_sha256":outcome.map(|o| &o.executable_sha256),
            "log_sha256":outcome.map(|o| &o.log_sha256)}),
            )
        })
        .collect()
}
fn bounded_file(base: &Path, name: &str) -> Result<PathBuf> {
    let mut path = base.to_owned();
    for part in Path::new(name).components() {
        let Component::Normal(part) = part else {
            return Err(format!("unsafe supervisor evidence path: {name}").into());
        };
        path.push(part);
        if fs::symlink_metadata(&path)
            .map_err(|e| format!("supervisor evidence {name}: {e}"))?
            .file_type()
            .is_symlink()
        {
            return Err(format!("symlink supervisor evidence: {name}").into());
        }
    }
    let meta = fs::metadata(&path).map_err(|e| format!("supervisor evidence {name}: {e}"))?;
    if !meta.is_file() || meta.len() > MAX_BYTES {
        return Err(format!("supervisor evidence bound/type: {name}").into());
    }
    Ok(path)
}
fn same(a: &Value, b: &Value) -> bool {
    !a.is_null() && a == b
}
fn digest(value: &Value) -> Result<String> {
    Ok(hash(serde_json::to_vec(value)?))
}
/// Semantic validation is also used by non-live mutation fixtures.
pub fn validate_observation(v: &Value, case: &str, scenario: &str) -> Result<()> {
    let expected: BTreeSet<_> = [
        "version",
        "kind",
        "case",
        "scenario",
        "command",
        "command_digest",
        "authorization",
        "backend_command",
        "backend_result",
        "inspect_record",
        "wi",
        "activation_count",
        "guest_stopped",
        "wi_state_removed",
        "supervisor_restarted",
        "records",
        "build",
    ]
    .into();
    if v.as_object()
        .ok_or("missing supervisor observation")?
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>()
        != expected
        || v["version"] != 1
        || v["kind"] != "wi-supervisor-observation-v1"
        || v["case"] != case
        || v["scenario"] != scenario
        || v["activation_count"] != 1
        || v["guest_stopped"] != true
        || v["wi_state_removed"] != true
        || v["supervisor_restarted"] != true
    {
        return Err("supervisor observation scope or lifetime evidence drift".into());
    }
    let command = &v["command"];
    let request = &command["request"];
    let key = string(command, "key")?;
    if key.is_empty()
        || command["version"] != 1
        || request["version"] != 1
        || request["max_evidence_bytes"].as_u64().is_none_or(|n| n == 0 || n > 4096)
        || request["deadline_ms"].as_u64().is_none()
        || v["command_digest"] != digest(command)?
    {
        return Err("supervisor command identity/bounds drift".into());
    }
    let expected_operation = match case {
        "WP-035" | "WP-036" => "stop",
        "WP-037" | "WP-038" => "replace",
        "WP-039" => "recover",
        _ => return Err("unknown supervisor scenario case".into()),
    };
    if request["operation"] != expected_operation {
        return Err("substituted lifecycle operation".into());
    }
    for field in ["audience", "authority_generation", "policy_revision"] {
        if !same(&request[field], &v["authorization"][field]) {
            return Err("supervisor authorization mismatch".into());
        }
    }
    for field in ["tenant", "principal"] {
        if !same(&v["authorization"][field], &v["inspect_record"][field])
            || !same(&v["authorization"][field], &v["backend_command"][field])
        {
            return Err("supervisor authenticated scope mismatch".into());
        }
    }
    if v["backend_command"]["version"] != 1
        || v["backend_result"]["version"] != 1
        || v["inspect_record"]["version"] != 1
        || v["backend_command"]["command"] != *command
        || v["backend_command"]["command_digest"] != v["command_digest"]
        || v["backend_result"]["key"] != key
        || v["backend_result"]["command_digest"] != v["command_digest"]
        || v["inspect_record"]["command"] != *command
        || v["inspect_record"]["observation"] != v["backend_result"]["observation"]
        || v["inspect_record"]["observation"]["state"] != "completed"
        || v["inspect_record"]["observation"]["evidence"].as_array().is_none_or(|a| {
            a.is_empty() || a.len() as u64 > request["max_evidence_bytes"].as_u64().unwrap()
        })
    {
        return Err("contradictory backend or authenticated inspect evidence".into());
    }
    let wi = &v["wi"];
    let c = &wi["case"];
    let receipts =
        wi["original_receipts"].as_array().ok_or("missing original receipt identities")?;
    let envelopes = wi["receipt_envelopes"].as_array().ok_or("missing original receipt bytes")?;
    if wi["phase"] != "reconciled"
        || receipts.is_empty()
        || receipts.len() != envelopes.len()
        || wi["original_receipts"] != wi["reconciled_receipts"]
        || c != &wi["case_after_retry"]
        || c["status"] != "confirmed_committed"
        || c["commit"] != "confirmed_committed"
        || c["observations"].as_u64().is_none_or(|n| n == 0 || n > 32)
        || !same(&c["intent"], &wi["intent"]["id"])
        || c["tenant"] != v["authorization"]["tenant"]
        || c["execution"]["provider_key"] != key
        || !same(
            &c["execution"]["operation_identity"],
            &wi["intent"]["stable_idempotency_identity"],
        )
        || c["receipts"].as_array().is_none_or(Vec::is_empty)
        || c["evidence"].as_array().is_none_or(Vec::is_empty)
    {
        return Err("missing, rewritten or non-idempotent WI reconciliation evidence".into());
    }
    for (identity, envelope) in receipts.iter().zip(envelopes) {
        let receipt = &envelope["record"]["body"];
        let body = &receipt["body"]["body"];
        if envelope["contract"] != "WI-FABRIC-2"
            || envelope["canonical_profile"] != "wi-json-1"
            || envelope["schema"] != 1
            || envelope["record"]["kind"] != "boundary_receipt"
            || receipt["body"]["direction"] != "egress"
            || identity[0] != receipt["id"]
            || identity[1] != {
                let mut b = b"WI-FABRIC-2\0wi-json-1\0record\0".to_vec();
                b.extend(serde_json::to_vec(envelope)?);
                hash(b)
            }
            || identity[2].as_u64().is_none()
            || body["intent_id"] != wi["intent"]["id"]
            || body["outcome"] != "uncertain"
            || body["connector_execution"] != c["execution"]
        {
            return Err("original WI receipt identity, bytes or uncertainty changed".into());
        }
    }
    Ok(())
}
pub fn validate(root: &Path, receipt: &Value, base: &Path, outcomes: &[Outcome]) -> Result<()> {
    let expected_names: BTreeSet<_> = scenarios().into_iter().map(|(c, s, _)| name(c, s)).collect();
    let actual_names: BTreeSet<_> = fs::read_dir(base)?
        .map(|e| e.map(|e| e.file_name().to_string_lossy().into_owned()))
        .collect::<std::io::Result<Vec<_>>>()?
        .into_iter()
        .filter(|n| n.starts_with("supervisor-") && n.ends_with(".json"))
        .collect();
    if actual_names != expected_names
        || receipt["supervisor_observations"] != bindings(base, outcomes)
    {
        return Err("missing, substituted or unowned supervisor observations".into());
    }
    for (case, scenario, test) in scenarios() {
        let name = name(case, scenario);
        let bytes = fs::read(bounded_file(base, &name)?)
            .map_err(|e| format!("supervisor evidence {name}: {e}"))?;
        let outcome = outcomes
            .iter()
            .find(|o| o.reference == owner(test) && o.passed)
            .ok_or("missing passing supervisor evidence owner")?;
        let log = fs::read_to_string(bounded_file(base, &outcome.log)?)
            .map_err(|e| format!("supervisor evidence {}: {e}", outcome.log))?;
        if !log.lines().any(|l| l == format!("WI-SUPERVISOR-OBSERVATION {name} {}", hash(&bytes))) {
            return Err("supervisor observation not produced by exact test outcome".into());
        }
        let v: Value = serde_json::from_slice(&bytes)?;
        validate_observation(&v, case, scenario)?;
        crate::worker_campaign::validate_build_packet(
            &v,
            &receipt["evaluation_packet"],
            &hash(fs::read(
                root.join("contracts/sources/wi-fabric-2-worker-execution-profile.yaml"),
            )?),
        )?;
        if v["build"]["fixture_source"]
            != hash(fs::read(
                root.join("crates/worldinterface-runtime/tests/support/supervisor_fixture.rs"),
            )?)
        {
            return Err("supervisor fixture source drift".into());
        }
        let prefix = format!("supervisor-records-{case}-{scenario}");
        let journal_name =
            format!("{prefix}/journal/{}.json", hash(string(&v["command"], "key")?.as_bytes()));
        let expected = BTreeMap::from([
            (
                format!("{prefix}/config.json"),
                json!({"request":v["command"]["request"],"tenant":v["authorization"]["tenant"],"principal":v["authorization"]["principal"]}),
            ),
            (format!("{prefix}/backend-record.json"), v["backend_command"].clone()),
            (format!("{prefix}/backend-result.json"), v["backend_result"].clone()),
            (
                journal_name,
                json!({"checksum":digest(&v["inspect_record"])? ,"record":v["inspect_record"]}),
            ),
        ]);
        let records = v["records"].as_object().ok_or("missing retained operator records")?;
        let activation = format!("{prefix}/activations");
        let command_pin = format!("{prefix}/backend-command");
        if records.keys().cloned().collect::<BTreeSet<_>>()
            != expected.keys().cloned().chain([activation.clone(), command_pin.clone()]).collect()
        {
            return Err("incomplete retained operator record set".into());
        }
        let mut size = 0;
        for (name, pin) in records {
            let path = bounded_file(base, name)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::{MetadataExt, PermissionsExt};
                let meta =
                    fs::metadata(&path).map_err(|e| format!("supervisor evidence {name}: {e}"))?;
                if meta.permissions().mode() & 0o077 != 0
                    || meta.uid() != fs::metadata(base)?.uid()
                    || meta.nlink() != 1
                {
                    return Err(format!(
                        "retained supervisor record is not operator-private: {name}"
                    )
                    .into());
                }
            }
            let bytes = fs::read(&path).map_err(|e| format!("supervisor evidence {name}: {e}"))?;
            size += bytes.len() as u64;
            if *pin != hash(&bytes) {
                return Err("substituted retained operator record".into());
            }
            if name == &command_pin {
                if bytes != string(&v, "command_digest")?.as_bytes() {
                    return Err("retained backend command digest mismatch".into());
                }
            } else if name == &activation {
                if bytes != b"1" {
                    return Err("activation count mismatch".into());
                }
            } else if read(&path)? != expected[name] {
                return Err("retained record disagrees with observation".into());
            }
        }
        if size > MAX_BYTES {
            return Err("retained supervisor record byte bound".into());
        }
    }
    Ok(())
}

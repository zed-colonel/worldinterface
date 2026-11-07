use serde_json::json;
use std::{env, path::PathBuf};
use wi_contract_check::{
    catalog, continuations, contracts, evidence, finding, handoff, inventory, policy, proof, read,
    refresh, worker_campaign, Finding, Result,
};
fn run() -> Result<bool> {
    let root = env::var_os("WI_REPO_ROOT").map(PathBuf::from).unwrap_or(env::current_dir()?);
    let args: Vec<_> = env::args().skip(1).collect();
    if args.as_slice() == ["handoff", "generate"] {
        println!("{}", serde_json::to_string_pretty(&handoff::descriptor(&root)?)?);
        return Ok(true);
    }
    if args.len() == 3 && args[0] == "handoff" && args[1] == "verify" {
        handoff::verify(&root, &PathBuf::from(&args[2]))?;
        println!(
            "{}",
            json!({"scope":"handoff","status":"verified","reviewed_closure_established":false})
        );
        return Ok(true);
    }
    if args.as_slice() == ["inventory", "generate"] {
        println!("{}", serde_json::to_string_pretty(&inventory::generate(&root)?)?);
        return Ok(true);
    }
    if args.as_slice() == ["hashes", "refresh"] {
        refresh::refresh(&root)?;
        println!("{}", json!({"scope":"source-hashes","status":"refreshed"}));
        return Ok(true);
    }
    if args.len() == 3 && args[0] == "campaign" {
        let path = PathBuf::from(&args[2]);
        match args[1].as_str() {
            "run" => worker_campaign::run(&root, &path)?,
            "check" => worker_campaign::validate_receipt(
                &root,
                &read(&path)?,
                path.parent().ok_or("receipt directory missing")?,
            )?,
            _ => return Err("usage: campaign {run DIRECTORY|check RECEIPT}".into()),
        }
        println!(
            "{}",
            json!({"scope":"worker-campaign","status":"passed","claims":worker_campaign::claims()})
        );
        return Ok(true);
    }
    type Check = fn(&std::path::Path) -> Result<Vec<Finding>>;
    let commands: [(&str, Check); 12] = [
        ("catalog", catalog::check),
        ("proof", proof::check),
        ("handoff", handoff::check),
        ("baseline", contracts::baseline),
        ("inventory", inventory::check),
        ("contracts", contracts::contracts),
        ("fixtures", contracts::fixtures),
        ("provenance", contracts::provenance),
        ("conformance", contracts::conformance),
        ("docs", contracts::docs),
        ("clean-break", policy::check),
        ("continuations", continuations::check),
    ];
    if args.len() == 3
        && args[1] == "check"
        && ["evidence", "publication"].contains(&args[0].as_str())
    {
        let p = PathBuf::from(&args[2]);
        let v = read(&p)?;
        let base = p.parent().unwrap();
        if args[0] == "evidence" {
            evidence::runtime(&root, &v, base)?
        } else {
            evidence::publication(&root, &v, base)?
        }
        println!(
            "{}",
            json!({"scope":args[0],"status":"validated","runtime_execution_claim":false})
        );
        return Ok(true);
    }
    if args.len() != 2 || args[1] != "check" {
        return Err("usage: wi-contract-check {inventory generate|hashes refresh|<proof|catalog|inventory|contracts|fixtures|provenance|conformance|docs|clean-break|all> check|<evidence|publication> check FILE}".into());
    }
    let mut ok = true;
    let mut matched = false;
    for (scope, check) in commands {
        if args[0] == scope || args[0] == "all" {
            matched = true;
            let findings = match check(&root) {
                Ok(f) => f,
                Err(e) => vec![finding("CHECK-INVALID", scope, scope, "WI-01", &e.to_string())],
            };
            ok &= findings.is_empty();
            println!(
                "{}",
                json!({"scope":scope,"status":if findings.is_empty(){"passed"}else{"failed"},"evidence_class":"independent-checker","runtime_conformance":false,"runtime_pass_asserted":false,"removal_readiness":if scope == "catalog" { Some("blocked-pending-verified-replacements") } else { None },"findings":findings})
            );
        }
    }
    if !matched {
        return Err("unknown command".into());
    }
    Ok(ok)
}
fn main() {
    match run() {
        Ok(true) => {}
        Ok(false) => std::process::exit(1),
        Err(e) => {
            eprintln!(
                "{}",
                json!(finding("CHECK-INVALID", "command", "command", "WI-01", &e.to_string()))
            );
            std::process::exit(1)
        }
    }
}

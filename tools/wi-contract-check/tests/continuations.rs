use wi_contract_check::continuations::source_findings;
#[test]
fn continuation_policy_rejects_aliases_qualified_calls_macros_and_feature_branches() {
    for source in [
        "use std::thread::sleep as pause; fn f(){pause(d());}",
        "fn f(){ tokio::time::sleep_until(t()); }",
        "fn f(){ dispatch!(std::thread::park_timeout(d())); }",
        "#[cfg(feature=\"alternate\")] struct SignalRegistry;",
        "fn f(){let x=\"signal.await\";}",
        "static WAITING: u64=0;",
        "use tokio::sync::oneshot as channel;",
        "thread_local! { static STATE: std::cell::RefCell<std::collections::BTreeMap<WaitId,bool>> = Default::default(); }",
        "lazy_static! { static ref STATE: Mutex<Vec<WaitId>> = Default::default(); }",
        "fn f(){std::thread::r#sleep(d());}",
    ] {
        assert!(
            !source_findings("crates/worldinterface-runtime/src/new.rs", source)
                .unwrap()
                .is_empty(),
            "{source}"
        );
    }
    assert!(source_findings(
        "source.rs",
        "#[cfg(test)] mod tests {fn f(){std::thread::sleep(d());}} fn storage_lock(){}"
    )
    .unwrap()
    .is_empty());
}
#[test]
fn continuation_policy_keeps_ordinary_storage_locks_and_durable_checkpoints() {
    assert!(source_findings("source.rs","struct Durable { waits: std::collections::BTreeMap<String, WaitCheckpoint>, lock: std::sync::Mutex<()> }").unwrap().is_empty());
}
#[test]
fn continuation_policy_rejects_alternate_module_and_include_routes_to_legacy_code() {
    let t = tempfile::tempdir().unwrap();
    let root = t.path();
    std::fs::create_dir_all(root.join("crates/worldinterface-runtime/src")).unwrap();
    std::fs::create_dir_all(root.join("crates/legacy/src")).unwrap();
    std::fs::create_dir(root.join("examples")).unwrap();
    std::fs::write(root.join("Cargo.toml"), "[workspace]\nmembers=[]\n").unwrap();
    std::fs::write(root.join("crates/legacy/src/wake.rs"), "fn wake(){std::thread::sleep(d());}")
        .unwrap();
    for source in [
        "#[path=\"../../legacy/src/wake.rs\"] mod extra;",
        "include!(\"../../legacy/src/wake.rs\");",
    ] {
        std::fs::write(root.join("crates/worldinterface-runtime/src/lib.rs"), source).unwrap();
        assert!(wi_contract_check::continuations::check(root)
            .unwrap()
            .iter()
            .any(|f| f.rule == "CONTINUATION-SOURCE-ROUTE"));
    }
}

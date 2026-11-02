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
        "struct X { pending: Mutex<HashMap<WaitId, std::task::Waker>> }",
        "struct X { resumes: tokio::sync::Mutex<HashMap<WaitId, Handle>> }",
        "use std::collections::HashMap as Map; use a::WaitId as Key; struct X { pending: Map<Key, Handle> }",
        "struct X { pending: Vec<ResumeContextId> }",
        "type Key = WaitId; struct X { pending: HashMap<Key, Handle> }",
        "struct X { pending: HashMap<String, (WaitId, Handle)> }",
        "make! { struct X { pending: HashMap<WaitId, Handle> } }",
        "fn f(){tokio::time::timeout(d(), std::future::pending::<()>()).await;}",
        "fn f(){tokio::time::interval(d());}",
        "use std::sync::Barrier;",
        "fn f(){crossbeam_channel::bounded::<u8>(1);}",
        "use crossbeam::channel as local; fn f(){local::bounded::<u8>(1);}",
        "struct X { pending: futures::task::AtomicWaker }",
        "use futures::task::AtomicWaker as Wake;",
        "struct X { pending: event_listener::Event }",
        "use event_listener::Event as LocalEvent;",
        "fn f(){flume::unbounded::<u8>();}",
        "fn f(){while Instant::now() < deadline {std::hint::spin_loop();}}",
        "struct X { pending: Mutex<Option<Waker>> }",
        "struct X { pending: Mutex<Box<[(WaitId, Handle)]>> }",
        "struct X { pending: DashMap<WaitId, Handle> }",
        "struct X { pending: IndexMap<String, Waker> }",
        "struct X { pending: Slab<WaitId> }",
        "type M<K,V> = HashMap<K,V>; struct X { pending: M<WaitId,Handle> }",
        "type M<K,V> = HashMap<K,V>; use a::ResumeContextId as Key; struct X { pending: M<Key,Handle> }",
        "struct X { pending: HashMap<RunId, JoinHandle> }",
        "make! { struct X { pending: M<WaitId,Handle> } }",
        "struct X { pending: (WaitId, Handle) }",
        "struct X { id: WaitId, executor: Handle }",
        "type Key = ResumeContextId; struct X { id: Key }",
        "struct X { id: Result<WaitId> }",
        "struct X { pending: [ResumeContextId; 10] }",
        "fn f(){async_channel::bounded::<u8>(1);}",
        "fn f(){kanal::unbounded::<u8>();}",
        "use tokio::sync::Semaphore;",
        "fn f(){async_io::Timer::after(d()).await;}",
        "fn f(){receiver.recv_timeout(d());}",
        "fn f(){std::thread::sleep_ms(1);}",
        "fn f(){while Instant::now() < deadline {std::thread::yield_now();}}",
        "fn f(){while Instant::now() < deadline {}}",
        "fn f(){let start = Instant::now(); while start.elapsed() < d() {}}",
        "struct X { pending: Mutex<HashMap<RunId, WaitSpec>> }",
        "struct X { pending: Mutex<HashMap<String, ResumeContext>> }",
        "struct X { pending: Vec<WakeReason> }",
        "type State = ResumeContext; struct X { pending: HashMap<String, State> }",
        "fn f(){while Instant::now() < deadline { do_work(); }}",
        "fn f(){let start = Instant::now(); while start.elapsed() < d() { do_work(); }}",
        "fn f(){let start = Instant::now(); loop { if start.elapsed() >= d() {break;} do_work(); }}",
        "fn f(){std::future::pending::<()>().await;}",
        "use std::future::{pending as forever};",
        "fn f(){smol::channel::bounded::<()>(1);}",
        "fn f(){receiver.recv_deadline(t());}",
        "fn f(){receiver.wait_timeout(d());}",
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
    assert!(source_findings("source.rs","enum PlanOperation { Timer { deadline: Deadline } } fn f(){ match x { PlanOperation::Timer { deadline } => save(deadline) } } fn new_id() -> Result<WaitId> { todo!() } struct Durable { waits: std::collections::BTreeMap<String, WaitCheckpoint>, lock: std::sync::Mutex<()> }").unwrap().is_empty());
    assert!(source_findings("source.rs", "fn make_wait() -> Result<(WaitSpec, Option<EventRoute>)> { todo!() } fn reload(){ let checkpoint: (ContentHash, OpaqueRef, WaitSpec) = load(); } fn diagnostic(){ let started = Instant::now(); do_work(); report(started.elapsed()); } struct Status { pending: Vec<String> }").unwrap().is_empty());
}

#[test]
fn continuation_policy_follows_aliases_across_crate_modules_without_tainting_other_crates() {
    let t = tempfile::tempdir().unwrap();
    let root = t.path();
    std::fs::create_dir(root.join("examples")).unwrap();
    std::fs::write(root.join("Cargo.toml"), "[workspace]\nmembers=[]\n").unwrap();
    let runtime = root.join("crates/worldinterface-runtime");
    let storage = root.join("crates/worldinterface-foundation/storage");
    for dir in [&runtime, &storage] {
        std::fs::create_dir_all(dir.join("src")).unwrap();
        std::fs::write(dir.join("Cargo.toml"), "[package]\nname=\"probe\"\nversion=\"0.1.0\"\n")
            .unwrap();
    }
    std::fs::write(runtime.join("src/lib.rs"), "mod z_ids; mod y_reexport; mod a_registry;")
        .unwrap();
    std::fs::write(
        runtime.join("src/y_reexport.rs"),
        "pub use crate::z_ids::Key as Imported; pub type State = Imported;",
    )
    .unwrap();
    std::fs::write(runtime.join("src/a_registry.rs"), "use crate::y_reexport::State as Local; struct X { pending: Mutex<HashMap<Local, Handle>> }").unwrap();
    // Same spelling, different crate: crate-wide alias resolution must not
    // confuse a harmless storage key with the runtime's continuation state.
    std::fs::write(
        storage.join("src/lib.rs"),
        "type Local = String; struct X { entries: HashMap<Local, Value> }",
    )
    .unwrap();
    for identity in ["WaitId", "WaitSpec", "ResumeContext", "WakeReason"] {
        std::fs::write(runtime.join("src/z_ids.rs"), format!("pub type Key = {identity};"))
            .unwrap();
        let findings = wi_contract_check::continuations::check(root).unwrap();
        assert!(
            findings.iter().any(|f| f.rule == "CONTINUATION-LOCAL-WAIT"
                && f.path == "crates/worldinterface-runtime/src/a_registry.rs"),
            "{identity}: {findings:?}"
        );
        assert!(
            !findings.iter().any(|f| f.rule == "CONTINUATION-LOCAL-WAIT"
                && f.path.starts_with("crates/worldinterface-foundation/")),
            "{findings:?}"
        );
    }
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

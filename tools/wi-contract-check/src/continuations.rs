//! Production continuation policy. Inspect tokens, including imports and macro bodies.
use crate::{finding, hash, policy, relative, Finding, Result};
use proc_macro2::{TokenStream, TokenTree};
use quote::ToTokens;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};
use syn::visit::Visit;

const CRASH_BARRIER: &str = "crates/worldinterface-foundation/storage/src/crashpoint.rs";
const CRASH_BARRIER_HASH: &str = "c8c9d9f983ca833ffdd0d22552379aea3a5976ce39b6a537d2d3ac6a784f4e14";
// Approved WIP-09 operator disposition (docs/work-items/WIP-09.md): host supervision is distinct from
// AQ continuation correctness. Exact source pins require explicit review on edits;
// source-route checking and the continuation ban in every other file still apply.
const PROCESS_SUPERVISOR: &[(&str, &str)] = &[
    (
        "crates/worldinterface-runtime/src/worker_process/admission.rs",
        "d2488547620a933eba15536c07952226647de1e2625c1e528f3e70a0767d0719",
    ),
    (
        "crates/worldinterface-runtime/src/worker_process/bootstrap.rs",
        "6a9eccf4c9518ff1857f706e69077535afa5ad2de36686f9e7ecb2a71322ff3d",
    ),
    (
        "crates/worldinterface-runtime/src/worker_process/fixture.rs",
        "1c1a05a1c7308d101134740b84a92d20f6622ff637ed77d41b06599d324e7eb5",
    ),
    (
        "crates/worldinterface-runtime/src/worker_process/mod.rs",
        "2a2b85dee615c82ac15dcd7d6b2665da4d29657bfb3d7b472705a247ab0a728e",
    ),
    (
        "crates/worldinterface-runtime/src/worker_process/supervision.rs",
        "1e6acdbf2b57cab161945b14668c68a6ae8ef10861d98671647233f13bc52487",
    ),
];
fn process_supervisor_status(path: &str, source: &str) -> Option<bool> {
    PROCESS_SUPERVISOR
        .iter()
        .find(|(p, _)| *p == path)
        .map(|(_, expected)| hash(source.as_bytes()) == *expected)
}
fn forbidden(tokens: TokenStream, found: &mut BTreeSet<String>) {
    let tokens: Vec<_> = tokens.into_iter().collect();
    for (index, token) in tokens.iter().enumerate() {
        match token {
            TokenTree::Group(g) => forbidden(g.stream(), found),
            TokenTree::Ident(i) => {
                let name = i.to_string();
                let normalized = name.trim_start_matches("r#").to_lowercase().replace('_', "");
                // Match the qualified future constructor without banning
                // ordinary fields/variables named pending. Imports and renames
                // retain this path and are inspected too.
                if normalized == "future"
                    && tokens.get(index + 1..index + 4).is_some_and(|tail| {
                        matches!(&tail[0], TokenTree::Punct(p) if p.as_char() == ':')
                            && matches!(&tail[1], TokenTree::Punct(p) if p.as_char() == ':')
                            && match &tail[2] {
                                TokenTree::Ident(n) => n == "pending" || n == "r#pending",
                                TokenTree::Group(g) => g.stream().into_iter().any(|t|
                                    matches!(t, TokenTree::Ident(n) if n == "pending" || n == "r#pending")),
                                _ => false,
                            }
                    })
                {
                    found.insert("future::pending".into());
                }
                // Flow's durable deadline variant is not an executor timer.
                // Only its definition and qualified variant construction/match
                // are allowed; Timer imports and Timer::after still reject.
                if normalized == "timer"
                    && matches!(tokens.get(index + 1), Some(TokenTree::Group(g))
                    if g.delimiter() == proc_macro2::Delimiter::Brace
                    && (g.stream().to_string() == "deadline : Deadline"
                        || index >= 3
                            && matches!(&tokens[index-3], TokenTree::Ident(n) if n == "PlanOperation")
                            && matches!(&tokens[index-2], TokenTree::Punct(p) if p.as_char() == ':')
                            && matches!(&tokens[index-1], TokenTree::Punct(p) if p.as_char() == ':')))
                {
                    continue;
                }
                if [
                    "lazystatic",
                    "threadlocal",
                    "lazylock",
                    "sleep",
                    "sleepuntil",
                    "park",
                    "parktimeout",
                    "delay",
                    "delayfor",
                    "delayuntil",
                    "signalregistry",
                    "waitregistry",
                    "waiterregistry",
                    "waiters",
                    "sleepers",
                    "notify",
                    "condvar",
                    "mpsc",
                    "oneshot",
                    "broadcast",
                    "watch",
                    "wakechannel",
                    "timeout",
                    "timeoutat",
                    "interval",
                    "intervalat",
                    "spinloop",
                    "barrier",
                    "crossbeamchannel",
                    "crossbeam",
                    "atomicwaker",
                    "eventlistener",
                    "flume",
                    "asyncchannel",
                    "kanal",
                    "waker",
                    "joinhandle",
                    "semaphore",
                    "yieldnow",
                    "sleepms",
                    "recvtimeout",
                    "recvdeadline",
                    "waittimeout",
                    "smol",
                    "timer",
                ]
                .contains(&normalized.as_str())
                {
                    found.insert(name);
                }
            }
            TokenTree::Literal(l) => {
                if let Ok(s) = syn::parse_str::<syn::LitStr>(&l.to_string()) {
                    if ["signal.await", "signal.emit", "delay"].contains(&s.value().as_str()) {
                        found.insert(s.value());
                    }
                }
            }
            _ => {}
        }
    }
}
fn identity_types() -> BTreeSet<String> {
    ["WaitId", "ResumeContextId", "WaitSpec", "ResumeContext", "WakeReason"]
        .map(String::from)
        .into()
}
fn contains_identity(tokens: TokenStream, ids: &BTreeSet<String>) -> bool {
    tokens.into_iter().any(|t| match t {
        TokenTree::Ident(n) => ids.contains(n.to_string().trim_start_matches("r#")),
        TokenTree::Group(g) => contains_identity(g.stream(), ids),
        _ => false,
    })
}
fn extend_aliases(ast: &syn::File, identities: &mut BTreeSet<String>) {
    struct Aliases<'a>(&'a mut BTreeSet<String>);
    impl<'ast> Visit<'ast> for Aliases<'_> {
        fn visit_use_rename(&mut self, r: &'ast syn::UseRename) {
            if self.0.contains(&r.ident.to_string()) {
                self.0.insert(r.rename.to_string().trim_start_matches("r#").into());
            }
        }
        fn visit_item_type(&mut self, item: &'ast syn::ItemType) {
            use quote::ToTokens;
            if contains_identity(item.ty.to_token_stream(), self.0) {
                self.0.insert(item.ident.to_string().trim_start_matches("r#").into());
            }
        }
    }
    Aliases(identities).visit_file(ast);
}
pub fn source_findings(path: &str, source: &str) -> Result<Vec<Finding>> {
    source_findings_with_types(path, source, identity_types())
}
fn source_findings_with_types(
    path: &str,
    source: &str,
    mut identities: BTreeSet<String>,
) -> Result<Vec<Finding>> {
    let production = policy::production_tokens(source)?;
    let mut found = BTreeSet::new();
    forbidden(production.parse()?, &mut found);
    // Static state cannot own continuation correctness. The sealed-plan cache
    // is the existing bounded compiler optimization, inspected separately.
    let ast = syn::parse_file(&production)?;
    loop {
        let before = identities.len();
        extend_aliases(&ast, &mut identities);
        if before == identities.len() {
            break;
        }
    }
    // Containers and fields holding wait state can become registries. Result
    // and uncontained local/return tuples transport a constructed durable wait;
    // a tuple inside a container or field is still rejected.
    fn registries(tokens: TokenStream, ids: &BTreeSet<String>, found: &mut BTreeSet<String>) {
        let tokens: Vec<_> = tokens.into_iter().collect();
        for (i, t) in tokens.iter().enumerate() {
            if let TokenTree::Group(g) = t {
                registries(g.stream(), ids, found);
            }
            if !matches!(t, TokenTree::Ident(n) if n != "Result")
                || !matches!(tokens.get(i+1), Some(TokenTree::Punct(p)) if p.as_char() == '<')
            {
                continue;
            }
            let mut depth = 0;
            for t in &tokens[i + 1..] {
                match t {
                    TokenTree::Punct(p) if p.as_char() == '<' => depth += 1,
                    TokenTree::Punct(p) if p.as_char() == '>' => {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    TokenTree::Ident(n) if ids.contains(n.to_string().trim_start_matches("r#")) => {
                        found.insert(format!("container of {n}"));
                    }
                    TokenTree::Group(g) if contains_identity(g.stream(), ids) => {
                        found.insert("container of wait/resume identity".into());
                    }
                    _ => {}
                }
            }
        }
    }
    registries(production.parse()?, &identities, &mut found);
    struct TypesAndLoops<'a>(&'a BTreeSet<String>, &'a mut BTreeSet<String>);
    impl<'ast> Visit<'ast> for TypesAndLoops<'_> {
        fn visit_field(&mut self, field: &'ast syn::Field) {
            use quote::ToTokens;
            if contains_identity(field.ty.to_token_stream(), self.0) {
                self.1.insert("field holding wait/resume identity".into());
            }
            syn::visit::visit_field(self, field);
        }
        fn visit_type(&mut self, ty: &'ast syn::Type) {
            use quote::ToTokens;
            if matches!(ty, syn::Type::Array(_) | syn::Type::Slice(_))
                && contains_identity(ty.to_token_stream(), self.0)
            {
                self.1.insert("tuple/array of wait/resume identity".into());
            }
            syn::visit::visit_type(self, ty);
        }
        fn visit_expr_while(&mut self, expr: &'ast syn::ExprWhile) {
            if expr.body.stmts.is_empty() {
                self.1.insert("empty polling loop".into());
            }
            if timed_poll(expr.to_token_stream()) {
                self.1.insert("timed polling loop".into());
            }
            syn::visit::visit_expr_while(self, expr);
        }
        fn visit_expr_loop(&mut self, expr: &'ast syn::ExprLoop) {
            if expr.body.stmts.is_empty() {
                self.1.insert("empty polling loop".into());
            }
            if timed_poll(expr.to_token_stream()) {
                self.1.insert("timed polling loop".into());
            }
            syn::visit::visit_expr_loop(self, expr);
        }
    }
    fn timed_poll(tokens: TokenStream) -> bool {
        contains_identity(
            tokens,
            &BTreeSet::from(["Instant".into(), "elapsed".into(), "duration_since".into()]),
        )
    }
    TypesAndLoops(&identities, &mut found).visit_file(&ast);
    struct Statics<'a>(&'a mut BTreeSet<String>, &'a str);
    impl<'ast> Visit<'ast> for Statics<'_> {
        fn visit_item_static(&mut self, item: &'ast syn::ItemStatic) {
            if !(self.1 == "crates/worldinterface-foundation/storage/src/flow.rs" && {
                use quote::ToTokens;
                item.to_token_stream().to_string()
                    == "static PLANS : OnceLock < Mutex < Plans > > = OnceLock :: new () ;"
            }) {
                self.0.insert(format!("static {}", item.ident));
            }
            syn::visit::visit_item_static(self, item);
        }
    }
    Statics(&mut found, path).visit_file(&ast);
    Ok(found.into_iter().map(|name| finding("CONTINUATION-LOCAL-WAIT",path,&name,"WI-ADR-015 / WIP-08",
        "persist source/outbox and return AQ Awaiting; process-local waits, wake channels and executor sleeps are forbidden")).collect())
}
pub fn check(root: &Path) -> Result<Vec<Finding>> {
    let root = root.canonicalize()?;
    let mut findings = policy::source_coverage(&root)?;
    let mut sources = BTreeMap::<_, Vec<_>>::new();
    for path in policy::production_sources(&root)? {
        let rel = relative(&root, &path);
        if !rel.starts_with("crates/worldinterface-foundation/")
            && !rel.starts_with("crates/worldinterface-runtime/")
        {
            continue;
        }
        let source = fs::read_to_string(&path)?;
        let owner = path
            .ancestors()
            .find(|p| p.join("Cargo.toml").is_file())
            .ok_or("production source has no owning crate")?
            .to_path_buf();
        sources.entry(owner).or_default().push((path, source));
    }
    for files in sources.values() {
        // Conservative crate-wide name resolution: follow type/import aliases
        // to a fixed point across every inspected module and feature branch.
        // A same-named alias in another crate cannot taint this crate.
        let asts = files
            .iter()
            .map(|(_, s)| Ok(syn::parse_file(&policy::production_tokens(s)?)?))
            .collect::<Result<Vec<_>>>()?;
        let mut identities = identity_types();
        loop {
            let before = identities.len();
            for ast in &asts {
                extend_aliases(ast, &mut identities);
            }
            if before == identities.len() {
                break;
            }
        }
        for (path, source) in files {
            let rel = relative(&root, path);
            if rel == CRASH_BARRIER {
                if hash(source.as_bytes()) != CRASH_BARRIER_HASH {
                    findings.push(finding("CONTINUATION-CRASH-BARRIER",&rel,"approved development barrier","WIP-04 operator approval",
                    "only the exact approved crash barrier is exempt; production exclusion remains required"));
                }
                continue;
            }
            match process_supervisor_status(&rel, source) {
                Some(true) => {}
                Some(false) => {
                    findings.push(finding("CONTINUATION-PROCESS-SUPERVISOR",&rel,"reviewed host supervision","WIP-09 operator disposition","process supervisor source changed; explicit review and disposition required"));
                    findings.extend(source_findings_with_types(&rel, source, identities.clone())?);
                }
                None => {
                    findings.extend(source_findings_with_types(&rel, source, identities.clone())?)
                }
            }
            use syn::visit::Visit;
            struct Routes<'a> {
                root: &'a Path,
                path: &'a Path,
                rel: &'a str,
                findings: &'a mut Vec<Finding>,
            }
            impl Routes<'_> {
                fn route(&mut self, value: Option<String>) {
                    let target =
                        value.and_then(|s| self.path.parent().unwrap().join(s).canonicalize().ok());
                    if target.is_none_or(|p| {
                        let r = relative(self.root, &p);
                        !r.starts_with("crates/worldinterface-foundation/")
                            && !r.starts_with("crates/worldinterface-runtime/")
                    }) {
                        self.findings.push(finding(
                            "CONTINUATION-SOURCE-ROUTE",
                            self.rel,
                            "source inclusion",
                            "WIP-08 inspected production scope",
                            "continuation code must stay in inspected foundation/runtime sources",
                        ));
                    }
                }
            }
            impl<'ast> Visit<'ast> for Routes<'_> {
                fn visit_attribute(&mut self, a: &'ast syn::Attribute) {
                    if a.path().is_ident("path") {
                        let value = match &a.meta {
                            syn::Meta::NameValue(v) => match &v.value {
                                syn::Expr::Lit(l) => match &l.lit {
                                    syn::Lit::Str(s) => Some(s.value()),
                                    _ => None,
                                },
                                _ => None,
                            },
                            _ => None,
                        };
                        self.route(value);
                    }
                    syn::visit::visit_attribute(self, a);
                }
                fn visit_macro(&mut self, m: &'ast syn::Macro) {
                    if m.path.segments.last().is_some_and(|s| s.ident == "include") {
                        self.route(
                            syn::parse2::<syn::LitStr>(m.tokens.clone()).ok().map(|s| s.value()),
                        );
                    }
                    syn::visit::visit_macro(self, m);
                }
            }
            Routes { root: &root, path, rel: &rel, findings: &mut findings }
                .visit_file(&syn::parse_file(&policy::production_tokens(source)?)?);
        }
    }
    Ok(findings)
}

#[cfg(test)]
mod process_supervisor_tests {
    use super::*;
    #[test]
    fn only_exact_reviewed_host_sources_have_the_supervisor_exception() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        for (path, _) in PROCESS_SUPERVISOR {
            let source = fs::read_to_string(root.join(path)).unwrap();
            assert_eq!(process_supervisor_status(path, &source), Some(true));
            assert_eq!(
                process_supervisor_status(
                    path,
                    &(source.clone() + "\nfn unreviewed(){std::thread::sleep(d);}")
                ),
                Some(false)
            );
            assert_eq!(
                process_supervisor_status(
                    "crates/worldinterface-runtime/src/unreviewed.rs",
                    &source
                ),
                None
            );
        }
        // The generic policy itself is unchanged, even when passed a supervisor path.
        for path in [
            "crates/worldinterface-runtime/src/worker_process/mod.rs",
            "crates/worldinterface-runtime/src/new.rs",
        ] {
            assert!(!source_findings(path, "fn f(){std::thread::sleep(d);}").unwrap().is_empty());
        }
    }

    #[test]
    fn repository_scan_rejects_supervisor_drift_and_unlisted_local_waits() {
        let approved = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let runtime = root.join("crates/worldinterface-runtime");
        fs::create_dir_all(runtime.join("src/worker_process")).unwrap();
        fs::create_dir(root.join("examples")).unwrap();
        fs::write(root.join("Cargo.toml"), "[workspace]\nmembers=[]\n").unwrap();
        fs::write(runtime.join("Cargo.toml"), "[package]\nname=\"probe\"\nversion=\"0.1.0\"\n")
            .unwrap();
        for (path, _) in PROCESS_SUPERVISOR {
            let source = fs::read_to_string(approved.join(path)).unwrap();
            fs::write(root.join(path), &source).unwrap();
            let findings = check(root).unwrap();
            assert!(
                !findings.iter().any(|f| f.rule == "CONTINUATION-LOCAL-WAIT"
                    || f.rule == "CONTINUATION-PROCESS-SUPERVISOR"),
                "{path}: {findings:?}"
            );

            // Even harmless byte drift requires review; existing local waits
            // must be inspected again rather than silently inheriting approval.
            fs::write(root.join(path), source + "\n").unwrap();
            let findings = check(root).unwrap();
            for rule in ["CONTINUATION-PROCESS-SUPERVISOR", "CONTINUATION-LOCAL-WAIT"] {
                assert!(
                    findings.iter().any(|f| f.rule == rule && f.path == *path),
                    "{path}: {findings:?}"
                );
            }
            fs::remove_file(root.join(path)).unwrap();
        }

        // Placement beside an approved supervisor does not confer an exception.
        let path = "crates/worldinterface-runtime/src/worker_process/unreviewed.rs";
        fs::write(root.join(path), "fn f(){std::thread::sleep(d());}").unwrap();
        let findings = check(root).unwrap();
        assert!(findings.iter().any(|f| f.rule == "CONTINUATION-LOCAL-WAIT" && f.path == path));
    }
}

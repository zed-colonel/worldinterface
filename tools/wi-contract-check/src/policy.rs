use crate::{array, files, finding, hash, read, relative, string, Finding, Result};
use quote::ToTokens;
use regex::Regex;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};
use syn::visit::Visit;

pub fn milestone_number(s: &str) -> Result<u64> {
    let n = s.strip_prefix("WI-").ok_or("invalid milestone")?.parse::<u64>()?;
    if !(1..=14).contains(&n) || s != format!("WI-{n:02}") {
        return Err("unknown milestone".into());
    }
    Ok(n)
}
pub fn validate_state(
    v: &Value,
    approved_digest: Option<&str>,
    bytes: &[u8],
    external: bool,
) -> Result<BTreeSet<u64>> {
    if external && approved_digest != Some(hash(bytes).as_str()) {
        return Err("milestone state lacks matching controller-approved digest".into());
    }
    if v["version"] != 1 {
        return Err("unsupported milestone state version".into());
    }
    let reached = array(v, "reached")?;
    if reached.is_empty() {
        return Err("regressed milestone state".into());
    }
    let mut approved = BTreeSet::new();
    for m in reached {
        if !approved.insert(milestone_number(m.as_str().ok_or("invalid milestone")?)?) {
            return Err("duplicate milestone".into());
        }
    }
    if !approved.contains(&1) {
        return Err("regressed milestone state: WI-01 entry missing".into());
    }
    if !external && approved != BTreeSet::from([1]) {
        return Err("repository cannot advance controller milestones".into());
    }
    // Dependency admission and rollback protection belong to the controller.
    // Numeric IDs do not imply predecessor edges (WI-03/04/05 are independent).
    Ok(approved)
}
pub fn state(root: &Path) -> Result<BTreeSet<u64>> {
    let ext = std::env::var_os("WI_MILESTONE_STATE");
    let p =
        ext.as_ref().map(PathBuf::from).unwrap_or_else(|| root.join("contracts/milestones.json"));
    let b = fs::read(p)?;
    validate_state(
        &serde_json::from_slice(&b)?,
        std::env::var("WI_APPROVED_MILESTONE_SHA256").ok().as_deref(),
        &b,
        ext.is_some(),
    )
}
fn test_only(attrs: &[syn::Attribute]) -> bool {
    attrs
        .iter()
        .any(|a| a.path().is_ident("cfg") && a.meta.to_token_stream().to_string() == "cfg (test)")
}
/// Preserve every feature branch. Only exact #[cfg(test)] scopes are excluded.
pub fn production_tokens(source: &str) -> Result<String> {
    let mut file = syn::parse_file(source)?;
    fn strip(items: &mut Vec<syn::Item>) {
        items.retain_mut(|item| {
            let attrs = match item {
                syn::Item::Mod(x) => &x.attrs,
                syn::Item::Fn(x) => &x.attrs,
                syn::Item::Struct(x) => &x.attrs,
                syn::Item::Enum(x) => &x.attrs,
                syn::Item::Impl(x) => &x.attrs,
                syn::Item::Use(x) => &x.attrs,
                syn::Item::Const(x) => &x.attrs,
                syn::Item::Macro(x) => &x.attrs,
                syn::Item::Type(x) => &x.attrs,
                _ => &[][..],
            };
            if test_only(attrs) {
                return false;
            }
            if let syn::Item::Mod(x) = item {
                if let Some((_, sub)) = &mut x.content {
                    strip(sub)
                }
            }
            true
        });
    }
    strip(&mut file.items);
    Ok(file.to_token_stream().to_string())
}
pub fn symbol_findings(
    path: &str,
    tokens: &str,
    policy: &Value,
    reached: &BTreeSet<u64>,
) -> Result<Vec<Finding>> {
    let mut f = vec![];
    for r in array(policy, "rules")? {
        if reached.contains(&milestone_number(string(r, "activate_at")?)?)
            && array(r, "paths")?.iter().any(|p| path.starts_with(p.as_str().unwrap_or("!")))
            && Regex::new(string(r, "pattern")?)?.is_match(tokens)
        {
            f.push(finding(
                string(r, "id")?,
                path,
                string(r, "id")?,
                string(r, "source")?,
                string(r, "disposition")?,
            ));
        }
    }
    // Definitions and typed references are semantic coupling; ordinary string refs stay opaque.
    let ast = syn::parse_file(tokens)?;
    struct Domain<'a> {
        names: &'a Vec<Value>,
        found: BTreeSet<String>,
    }
    impl<'ast> Visit<'ast> for Domain<'_> {
        fn visit_ident(&mut self, i: &'ast syn::Ident) {
            if self.names.iter().any(|x| x.as_str() == Some(i.to_string().as_str())) {
                self.found.insert(i.to_string());
            }
        }
    }
    let mut d = Domain { names: array(policy, "domain_types")?, found: BTreeSet::new() };
    d.visit_file(&ast);
    for name in d.found {
        f.push(finding(
            "DOMAIN-EXO",
            path,
            &name,
            "stack WI-FABRIC-2 must_not_own",
            "use an opaque owner-domain reference; do not import or define EXO semantics",
        ))
    }
    if path.contains("/protocol/") || path.contains("/capability/") {
        let names = ["runtime", "flow_coordinator", "flow_compiler", "flow_model", "exoskeleton"];
        for name in names {
            if Regex::new(&format!(r"\b(crate|super|worldinterface)\s*::\s*{name}\b"))?
                .is_match(tokens)
            {
                f.push(finding(
                    "DEP-MODULE-DIRECTION",
                    path,
                    name,
                    "implementation plan §5.11; §12.3",
                    "foundational modules cannot depend on runtime or Flow ownership",
                ))
            }
        }
    }
    Ok(f)
}
fn archive_path(root: &Path, path: &Path) -> bool {
    match path.canonicalize() {
        Ok(p) => {
            !p.starts_with(root)
                || p.components()
                    .any(|c| matches!(c.as_os_str().to_str(), Some("tests" | "test-modules")))
                || ["fixtures", "contracts", "conformance", "inventory", "docs", "tools"]
                    .iter()
                    .any(|n| p.starts_with(root.join(n)))
        }
        Err(_) => true,
    }
}
// Only literal, concat!, and CARGO_MANIFEST_DIR expressions have a statically
// auditable include path. Dynamic generated source is rejected for review.
fn static_string(expr: &syn::Expr, manifest_dir: &Path) -> Option<String> {
    match expr {
        syn::Expr::Lit(x) => {
            if let syn::Lit::Str(s) = &x.lit {
                Some(s.value())
            } else {
                None
            }
        }
        syn::Expr::Macro(x) if x.mac.path.is_ident("concat") => {
            let args = syn::parse::Parser::parse2(
                syn::punctuated::Punctuated::<syn::Expr, syn::Token![,]>::parse_terminated,
                x.mac.tokens.clone(),
            )
            .ok()?;
            args.iter()
                .map(|e| static_string(e, manifest_dir))
                .collect::<Option<Vec<_>>>()
                .map(|s| s.concat())
        }
        syn::Expr::Macro(x) if x.mac.path.is_ident("env") => {
            let s = syn::parse2::<syn::LitStr>(x.mac.tokens.clone()).ok()?;
            (s.value() == "CARGO_MANIFEST_DIR").then(|| manifest_dir.to_string_lossy().into_owned())
        }
        _ => None,
    }
}
/// The scanner visits this finite set once, so source-route cycles cannot hide
/// declarations or cause recursive traversal. Every code route must stay in it.
pub fn production_sources(root: &Path) -> Result<BTreeSet<PathBuf>> {
    let mut sources = BTreeSet::new();
    for dir in ["crates", "examples"] {
        for p in files(&root.join(dir))? {
            let rel = relative(root, &p);
            if p.extension().and_then(|x| x.to_str()) == Some("rs")
                && !rel.contains("/tests/")
                && !rel.contains("/test-modules/")
            {
                sources.insert(p.canonicalize()?);
            }
        }
    }
    Ok(sources)
}
fn scanned_source(sources: &BTreeSet<PathBuf>, path: &Path) -> bool {
    path.canonicalize().is_ok_and(|p| sources.contains(&p))
}
/// Inventory may only claim completeness when all production code routes are
/// inspectable. Unsupported extensions/layouts fail closed rather than vanishing.
pub fn source_coverage(root: &Path) -> Result<Vec<Finding>> {
    let root = root.canonicalize()?;
    let mut findings = vec![];
    let sources = production_sources(&root)?;
    for path in &sources {
        findings.extend(source_routes_in(&root, path, &fs::read_to_string(path)?, &sources)?);
    }
    let mut manifests = vec![root.join("Cargo.toml")];
    for dir in ["crates", "examples"] {
        manifests.extend(
            files(&root.join(dir))?
                .into_iter()
                .filter(|p| p.file_name().is_some_and(|n| n == "Cargo.toml")),
        );
    }
    for path in manifests {
        findings.extend(target_routes(&root, &path, &read(&path)?, &sources)?);
    }
    Ok(findings)
}
fn target_routes(
    root: &Path,
    manifest: &Path,
    value: &Value,
    sources: &BTreeSet<PathBuf>,
) -> Result<Vec<Finding>> {
    let mut findings = vec![];
    for kind in ["lib", "bin", "example", "test", "bench"] {
        let entries: Vec<&Value> = if let Some(a) = value[kind].as_array() {
            a.iter().collect()
        } else {
            vec![&value[kind]]
        };
        for entry in entries {
            if let Some(s) = entry["path"].as_str() {
                if !scanned_source(sources, &manifest.parent().unwrap().join(s)) {
                    findings.push(finding(
                        "ARCHIVE-TARGET",
                        &relative(root, manifest),
                        kind,
                        "WI-01 §1.5",
                        "Cargo target is outside inspected production Rust sources",
                    ));
                }
            }
        }
    }
    Ok(findings)
}
pub fn source_routes(root: &Path, path: &Path, source: &str) -> Result<Vec<Finding>> {
    source_routes_in(root, path, source, &production_sources(root)?)
}
fn source_routes_in(
    root: &Path,
    path: &Path,
    source: &str,
    sources: &BTreeSet<PathBuf>,
) -> Result<Vec<Finding>> {
    let mut manifest_dir = path.parent().ok_or("source parent missing")?;
    while !manifest_dir.join("Cargo.toml").exists() {
        manifest_dir = manifest_dir.parent().ok_or("Cargo manifest missing")?
    }
    struct Routes<'a> {
        root: &'a Path,
        path: &'a Path,
        manifest_dir: &'a Path,
        findings: Vec<Finding>,
        sources: &'a BTreeSet<PathBuf>,
        inline_depth: usize,
    }
    impl Routes<'_> {
        fn route(&mut self, target: Option<PathBuf>, code: bool) {
            if target.as_ref().is_none_or(|t| {
                archive_path(self.root, t) || code && !scanned_source(self.sources, t)
            }) {
                self.findings.push(finding("ARCHIVE-INCLUSION",&relative(self.root,self.path),"production-source-route","WI-01 §1.5", "archive, missing, dynamic, or uninspected source inclusion; code must use inspected .rs files under crates/ or examples/"));
            }
        }
    }
    impl<'ast> Visit<'ast> for Routes<'_> {
        fn visit_item_mod(&mut self, m: &'ast syn::ItemMod) {
            // Inline #[path] resolution depends on module/expansion context. Do
            // not guess a path and accidentally approve a different file.
            let inline = m.content.is_some();
            self.inline_depth += usize::from(inline);
            syn::visit::visit_item_mod(self, m);
            self.inline_depth -= usize::from(inline);
        }
        fn visit_attribute(&mut self, a: &'ast syn::Attribute) {
            if a.path().is_ident("path") {
                let target = if self.inline_depth > 0 {
                    None
                } else if let syn::Meta::NameValue(n) = &a.meta {
                    static_string(&n.value, self.manifest_dir)
                        .map(|s| self.path.parent().unwrap().join(s))
                } else {
                    None
                };
                self.route(target, true);
            }
            // cfg_attr may hide #[path]; fail closed unless it has no path argument.
            if a.path().is_ident("cfg_attr")
                && a.to_token_stream()
                    .to_string()
                    .split(|c: char| !c.is_alphanumeric() && c != '_')
                    .any(|s| s == "path")
            {
                self.route(None, true)
            }
            syn::visit::visit_attribute(self, a);
        }
        fn visit_macro(&mut self, m: &'ast syn::Macro) {
            if m.path.segments.last().is_some_and(|s| {
                ["include", "include_str", "include_bytes"].contains(&s.ident.to_string().as_str())
            }) {
                let target = syn::parse2::<syn::Expr>(m.tokens.clone())
                    .ok()
                    .and_then(|e| static_string(&e, self.manifest_dir))
                    .map(|s| self.path.parent().unwrap().join(s));
                self.route(target, m.path.segments.last().unwrap().ident == "include");
            }
            // Inspect macro token bodies too: a declaration generator can hide inclusion.
            fn suspicious(t: proc_macro2::TokenStream) -> bool {
                t.into_iter().any(|x| match x {
                    proc_macro2::TokenTree::Ident(i) => matches!(
                        i.to_string().as_str(),
                        "include" | "include_str" | "include_bytes"
                    ),
                    proc_macro2::TokenTree::Group(g) => suspicious(g.stream()),
                    _ => false,
                })
            }
            if !["include", "include_str", "include_bytes"].iter().any(|x| m.path.is_ident(x))
                && suspicious(m.tokens.clone())
            {
                self.route(None, true)
            }
        }
    }
    let tokens = production_tokens(source)?;
    let ast = syn::parse_file(&tokens)?;
    let mut r = Routes { root, path, manifest_dir, findings: vec![], sources, inline_depth: 0 };
    r.visit_file(&ast);
    Ok(r.findings)
}
fn dep_tables(v: &Value) -> Vec<(&str, &Value)> {
    let mut out = vec![];
    for k in ["dependencies", "build-dependencies", "dev-dependencies"] {
        if let Some(m) = v[k].as_object() {
            out.extend(m.iter().map(|(k, v)| (k.as_str(), v)))
        }
    }
    if let Some(targets) = v["target"].as_object() {
        for t in targets.values() {
            out.extend(dep_tables(t))
        }
    }
    out
}
fn prohibited_dependency(policy: &Value, name: &str, identity_restricted: bool) -> Result<bool> {
    let normalized = name.to_lowercase();
    Ok(array(policy, "unconditional_forbidden_dependency_patterns")?
        .iter()
        .any(|x| normalized.contains(x.as_str().unwrap_or("!")))
        || identity_restricted
            && array(policy, "forbidden_dependency_patterns")?
                .iter()
                .any(|x| normalized.contains(x.as_str().unwrap_or("!"))))
}
pub fn dependencies(root: &Path, policy: &Value) -> Result<Vec<Finding>> {
    let root = root.canonicalize()?;
    let ws = read(&root.join("Cargo.toml"))?;
    let sources = production_sources(&root)?;
    let mut f = vec![];
    let mut manifests = BTreeMap::new();
    if ws["package"].is_object() {
        manifests.insert(root.join("Cargo.toml"), ws.clone());
    }
    // Reject archive/workspace routes even when no feature currently enables them.
    fn manifest_routes(root: &Path, v: &Value, f: &mut Vec<Finding>) {
        if let Some(o) = v.as_object() {
            for (k, x) in o {
                if k == "path" {
                    if let Some(s) = x.as_str() {
                        if archive_path(root, &root.join(s)) {
                            f.push(finding(
                                "ARCHIVE-WORKSPACE",
                                "Cargo.toml",
                                s,
                                "WI-01 archive isolation",
                                "workspace dependency/patch path escapes production",
                            ))
                        }
                    }
                } else {
                    manifest_routes(root, x, f)
                }
            }
        } else if let Some(a) = v.as_array() {
            for x in a {
                manifest_routes(root, x, f)
            }
        }
    }
    manifest_routes(&root, &ws, &mut f);
    if let Some(members) = ws["workspace"]["members"].as_array() {
        for member in members {
            if let Some(s) = member.as_str() {
                if s.contains('*') || archive_path(&root, &root.join(s)) {
                    f.push(finding(
                        "ARCHIVE-WORKSPACE",
                        "Cargo.toml",
                        s,
                        "WI-01 archive isolation",
                        "workspace member must be explicit production path",
                    ))
                }
            }
        }
    }
    for dir in ["crates", "examples"] {
        for p in files(&root.join(dir))? {
            if p.file_name().unwrap() == "Cargo.toml" {
                manifests.insert(p.clone(), read(&p)?);
            }
        }
    }
    // Workspace dependency aliases resolve at their declaring root.
    fn walk(
        env: (&Path, &Value, &Value, &Value),
        p: &Path,
        v: &Value,
        foundation: bool,
        mandatory: bool,
        seen: &mut BTreeSet<(PathBuf, bool, bool, String)>,
        f: &mut Vec<Finding>,
    ) -> Result<()> {
        let (root, ws, policy, lock) = env;
        if !seen.insert((
            p.to_path_buf(),
            foundation,
            mandatory,
            hash(serde_json::to_vec(&v["features"])?),
        )) {
            return Ok(());
        }
        let path = relative(root, p);
        for (alias, decl) in dep_tables(v) {
            let (d, base) = if decl["workspace"] == true {
                (&ws["workspace"]["dependencies"][alias], root)
            } else {
                (decl, p.parent().unwrap())
            };
            if d.is_null() {
                f.push(finding(
                    "DEP-UNRESOLVED",
                    &path,
                    alias,
                    "§12.3",
                    "workspace dependency is unresolved",
                ));
                continue;
            }
            let name = d["package"].as_str().unwrap_or(alias);
            let mut enabled = BTreeSet::new();
            fn feature(v: &Value, name: &str, seen: &mut BTreeSet<String>) {
                if !seen.insert(name.into()) {
                    return;
                }
                if let Some(a) = v["features"][name].as_array() {
                    for x in a {
                        if let Some(s) = x.as_str() {
                            feature(v, s, seen)
                        }
                    }
                }
            }
            feature(v, "default", &mut enabled);
            let default_enabled = enabled.contains(alias)
                || enabled.contains(&format!("dep:{alias}"))
                || enabled.iter().any(|feature| feature.starts_with(&format!("{alias}/")));
            let required = mandatory
                && ((d["optional"] != true && decl["optional"] != true) || default_enabled);
            let prohibited = prohibited_dependency(policy, name, required || foundation)?;
            let direction = foundation
                && array(policy, "foundation_forbidden_packages")?.iter().any(|x| x == name);
            if direction || prohibited {
                f.push(finding("DEP-DOMAIN",&path,name,"stack dependency direction; §12.3","remove domain dependency; optional identity adapters belong outside protocol/capability"))
            }
            if d["path"].is_null() && root.join("Cargo.lock").exists() {
                let mut pending = vec![name.to_string()];
                let mut visited = BTreeSet::new();
                while let Some(package) = pending.pop() {
                    if !visited.insert(package.clone()) {
                        continue;
                    }
                    let bad = prohibited_dependency(policy, &package, required || foundation)?;
                    if bad {
                        f.push(finding("DEP-LOCK-REACHABILITY",&path,&package,"stack dependency direction; §12.3","locked transitive dependency reaches prohibited domain; review feature graph before admission"));
                    }
                    for pkg in array(lock, "package")?.iter().filter(|x| x["name"] == package) {
                        if let Some(deps) = pkg["dependencies"].as_array() {
                            for dep in deps {
                                if let Some(n) =
                                    dep.as_str().and_then(|s| s.split_whitespace().next())
                                {
                                    pending.push(n.into());
                                }
                            }
                        }
                    }
                }
            }
            if let Some(s) = d["path"].as_str() {
                let target = base.join(s).join("Cargo.toml");
                if archive_path(root, &target) {
                    f.push(finding(
                        "ARCHIVE-DEPENDENCY",
                        &path,
                        name,
                        "WI-01 §1.5",
                        "production dependency reaches archive/development/outside-root path",
                    ));
                    continue;
                }
                let target = target.canonicalize()?;
                let mut child = read(&target)?;
                // Features requested by a required parent can activate an otherwise
                // optional identity SDK. Carry those selections into local traversal.
                let mut selected =
                    if d["default-features"] == false || decl["default-features"] == false {
                        vec![]
                    } else {
                        child["features"]["default"].as_array().cloned().unwrap_or_default()
                    };
                for spec in [d, decl] {
                    if let Some(features) = spec["features"].as_array() {
                        selected.extend(features.iter().cloned());
                    }
                }
                if child.get("features").is_none() {
                    child["features"] = serde_json::json!({});
                }
                child["features"]["default"] = serde_json::json!(selected);
                // A renamed local package is checked using its actual package identity.
                let actual =
                    child["package"]["name"].as_str().ok_or("local package missing name")?;
                let bad = prohibited_dependency(policy, actual, required || foundation)?;
                if bad
                    || foundation
                        && array(policy, "foundation_forbidden_packages")?
                            .iter()
                            .any(|x| x == actual)
                {
                    f.push(finding(
                        "DEP-DOMAIN",
                        &path,
                        actual,
                        "stack dependency direction; §12.3",
                        "renamed/transitive package violates domain ownership",
                    ))
                }
                walk(env, &target, &child, foundation, required, seen, f)?;
            }
        }
        Ok(())
    }
    let lock = if root.join("Cargo.lock").exists() {
        read(&root.join("Cargo.lock"))?
    } else {
        serde_json::json!({"package":[]})
    };
    for (p, v) in &manifests {
        let foundation =
            array(policy, "foundation_packages")?.iter().any(|x| x == &v["package"]["name"]);
        walk((&root, &ws, policy, &lock), p, v, foundation, true, &mut BTreeSet::new(), &mut f)?;
        let build = v["package"]["build"]
            .as_str()
            .map(|b| p.parent().unwrap().join(b))
            .unwrap_or_else(|| p.parent().unwrap().join("build.rs"));
        if v["package"]["build"] != false && build.exists() {
            f.push(finding("ARCHIVE-BUILD-SCRIPT",&relative(&root,&build),"build-script","WI-01 archive isolation","arbitrary build scripts require a separately reviewed source-generator policy; none is approved at WI-01"))
        }
        f.extend(target_routes(&root, p, v, &sources)?);
    }
    Ok(f)
}
pub fn check_at(root: &Path, reached: &BTreeSet<u64>) -> Result<Vec<Finding>> {
    let root = root.canonicalize()?;
    let policy = read(&root.join("contracts/forbidden-symbols.yaml"))?;
    let mut f = dependencies(&root, &policy)?;
    let sources = production_sources(&root)?;
    for p in &sources {
        let rel = relative(&root, p);
        let source = fs::read_to_string(p)?;
        f.extend(symbol_findings(&rel, &production_tokens(&source)?, &policy, reached)?);
        f.extend(source_routes_in(&root, p, &source, &sources)?);
    }
    Ok(f)
}
pub fn check(root: &Path) -> Result<Vec<Finding>> {
    check_at(root, &state(root)?)
}

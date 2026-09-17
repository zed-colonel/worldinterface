use crate::{files, finding, hash, read, relative, Finding, Result};
use quote::ToTokens;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};
use syn::visit::Visit;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Item {
    pub id: String,
    pub kind: String,
    pub path: String,
    pub name: String,
    pub source_sha256: String,
    pub conditions: Vec<String>,
    pub registration_sites: Vec<String>,
}
struct Scanner {
    path: String,
    digest: String,
    scope: Vec<String>,
    conditions: Vec<String>,
    items: Vec<Item>,
    registrations: Vec<(String, String)>,
    errors: Vec<String>,
}
impl Scanner {
    fn add(&mut self, kind: &str, name: &str) {
        self.items.push(Item {
            id: format!(
                "{}::{}::{}:{}{}",
                self.path,
                self.scope.join("::"),
                kind,
                name,
                if self.conditions.is_empty() {
                    String::new()
                } else {
                    format!("@{}", &hash(self.conditions.join(";"))[..12])
                }
            ),
            kind: kind.into(),
            path: self.path.clone(),
            name: name.into(),
            source_sha256: self.digest.clone(),
            conditions: self.conditions.clone(),
            registration_sites: vec![],
        });
    }
}
impl<'ast> Visit<'ast> for Scanner {
    fn visit_item(&mut self, node: &'ast syn::Item) {
        let attrs = match node {
            syn::Item::Mod(x) => &x.attrs,
            syn::Item::Struct(x) => &x.attrs,
            syn::Item::Enum(x) => &x.attrs,
            syn::Item::Type(x) => &x.attrs,
            syn::Item::Trait(x) => &x.attrs,
            syn::Item::Impl(x) => &x.attrs,
            syn::Item::Fn(x) => &x.attrs,
            syn::Item::Macro(x) => &x.attrs,
            _ => &[][..],
        };
        let n = self.conditions.len();
        self.conditions.extend(
            attrs
                .iter()
                .filter(|a| a.path().is_ident("cfg") || a.path().is_ident("cfg_attr"))
                .map(|a| a.to_token_stream().to_string()),
        );
        match node {
            syn::Item::Mod(x) => {
                self.add("module", &x.ident.to_string());
                self.scope.push(x.ident.to_string());
                if let Some((_, items)) = &x.content {
                    for i in items {
                        self.visit_item(i)
                    }
                }
                self.scope.pop();
            }
            syn::Item::Struct(x) => self.add("struct", &x.ident.to_string()),
            syn::Item::Enum(x) => self.add("enum", &x.ident.to_string()),
            syn::Item::Type(x) => self.add("type", &x.ident.to_string()),
            syn::Item::Trait(x) => self.add("trait", &x.ident.to_string()),
            syn::Item::Impl(x) => {
                if x.trait_.as_ref().is_some_and(|(_, p, _)| {
                    p.segments.last().is_some_and(|s| s.ident == "Connector")
                }) {
                    self.add("connector", &x.self_ty.to_token_stream().to_string());
                }
                self.scope.push(format!("impl {}", x.self_ty.to_token_stream()));
                syn::visit::visit_item_impl(self, x);
                self.scope.pop();
            }
            syn::Item::Fn(x) => {
                self.scope.push(x.sig.ident.to_string());
                syn::visit::visit_item_fn(self, x);
                self.scope.pop();
            }
            syn::Item::Macro(x) => {
                let name = x.mac.path.to_token_stream().to_string().replace(' ', "");
                if name == "macro_rules" {
                    self.add("macro-definition", &x.ident.as_ref().unwrap().to_string());
                } else if name == "define_id" {
                    let args = syn::parse::Parser::parse2(
                        syn::punctuated::Punctuated::<syn::Expr, syn::Token![,]>::parse_terminated,
                        x.mac.tokens.clone(),
                    );
                    match args {
                        Ok(a) if a.len() == 2 => {
                            if let syn::Expr::Path(p) = &a[0] {
                                self.add("generated-id", &p.path.to_token_stream().to_string())
                            } else {
                                self.errors.push("define_id requires an identifier".into())
                            }
                        }
                        _ => self.errors.push("unsupported define_id shape".into()),
                    }
                } else if matches!(
                    name.as_str(),
                    "proptest"
                        | "wit_bindgen::generate"
                        | "wasmtime::component::bindgen"
                        | "export"
                ) {
                    // Reviewed code generators: record each invocation and its byte identity.
                    // External WIT bindings are not hand-written WI type declarations.
                    self.add(
                        "reviewed-generator",
                        &format!("{}:{}", name, hash(x.mac.tokens.to_string())),
                    );
                } else {
                    self.errors.push(format!(
                        "unsupported declaration-generating macro {name}; add a reviewed extractor"
                    ));
                }
            }
            _ => syn::visit::visit_item(self, node),
        }
        self.conditions.truncate(n);
    }
    fn visit_stmt_macro(&mut self, x: &'ast syn::StmtMacro) {
        let name = x.mac.path.segments.last().unwrap().ident.to_string();
        if ![
            "assert",
            "assert_eq",
            "assert_ne",
            "debug_assert",
            "debug_assert_eq",
            "debug_assert_ne",
            "println",
            "eprintln",
            "print",
            "eprint",
            "write",
            "writeln",
            "panic",
            "unreachable",
            "todo",
            "unimplemented",
            "bail",
            "ensure",
            "prop_assert",
            "prop_assert_eq",
            "prop_assert_ne",
            "info",
            "debug",
            "warn",
            "error",
            "trace",
            "json",
            "vec",
            "format",
            "select",
        ]
        .contains(&name.as_str())
        {
            self.errors.push(format!(
                "unsupported statement generator {name}; declaration completeness requires review"
            ));
        }
        syn::visit::visit_stmt_macro(self, x);
    }
    fn visit_expr_method_call(&mut self, x: &'ast syn::ExprMethodCall) {
        if x.method == "register" || x.method == "register_runtime" {
            self.registrations.push((
                format!("{}::{}", self.path, self.scope.join("::")),
                x.args.to_token_stream().to_string(),
            ));
        }
        syn::visit::visit_expr_method_call(self, x);
    }
}
pub type RegistrationSite = (String, String);
pub type SourceInventory = (Vec<Item>, Vec<RegistrationSite>);
pub fn scan_source(path: &str, source: &str) -> Result<SourceInventory> {
    let ast = syn::parse_file(source)?;
    let mut s = Scanner {
        path: path.into(),
        digest: hash(source),
        scope: vec![],
        conditions: vec![],
        items: vec![],
        registrations: vec![],
        errors: vec![],
    };
    s.add("source-module", path);
    s.visit_file(&ast);
    if !s.errors.is_empty() {
        return Err(format!("{path}: {}", s.errors.join("; ")).into());
    }
    Ok((s.items, s.registrations))
}
pub fn generate(root: &Path) -> Result<Vec<Item>> {
    let uncovered = crate::policy::source_coverage(root)?;
    if !uncovered.is_empty() {
        return Err(format!(
            "inventory source coverage rejected: {}",
            serde_json::to_string(&uncovered)?
        )
        .into());
    }
    let mut items = vec![];
    let mut sites = vec![];
    let workspace = read(&root.join("Cargo.toml"))?;
    let members: BTreeSet<_> = workspace["workspace"]["members"]
        .as_array()
        .ok_or("workspace members missing")?
        .iter()
        .filter_map(Value::as_str)
        .collect();
    for dir in ["crates", "examples"] {
        for p in files(&root.join(dir))? {
            let path = relative(root, &p);
            if p.extension().and_then(|x| x.to_str()) == Some("rs") {
                let (mut found, mut regs) = scan_source(&path, &fs::read_to_string(&p)?)?;
                items.append(&mut found);
                sites.append(&mut regs);
            } else if p.file_name().unwrap() == "Cargo.toml" {
                let v = read(&p)?;
                if let Some(name) = v["package"]["name"].as_str() {
                    let kind = if members.contains(relative(root, p.parent().unwrap()).as_str()) {
                        "workspace-crate"
                    } else {
                        "auxiliary-crate"
                    };
                    items.push(Item {
                        id: format!("{path}::{kind}:{name}"),
                        kind: kind.into(),
                        path: path.clone(),
                        name: name.into(),
                        source_sha256: hash(fs::read(&p)?),
                        conditions: vec![],
                        registration_sites: vec![],
                    });
                }
            } else if path.ends_with(".connector.toml") {
                let v = read(&p)?;
                let name = v["connector"]["name"].as_str().ok_or("WASM connector name missing")?;
                items.push(Item {
                    id: format!("{path}::wasm-manifest:{name}"),
                    kind: "wasm-manifest".into(),
                    path: path.clone(),
                    name: name.into(),
                    source_sha256: hash(fs::read(&p)?),
                    conditions: vec![],
                    registration_sites: vec![],
                });
            }
        }
    }
    for i in &mut items {
        if i.kind == "connector" {
            i.registration_sites = sites
                .iter()
                .filter(|(_, arg)| {
                    arg.split(|c: char| !c.is_alphanumeric() && c != '_').any(|t| t == i.name)
                })
                .map(|(s, _)| s.clone())
                .collect();
            i.registration_sites.sort();
            i.registration_sites.dedup();
        }
    }
    // Preserve unresolved/dynamic registration calls as explicit review items.
    let mut occurrences = BTreeMap::new();
    for (site, args) in sites {
        let key = format!("{site}::registration:{}", hash(&args));
        let count = occurrences.entry(key.clone()).or_insert(0);
        *count += 1;
        items.push(Item {
            id: format!("{key}#{count}"),
            kind: "registration".into(),
            path: site.split("::").next().unwrap().into(),
            name: args,
            source_sha256: hash(fs::read(root.join(site.split("::").next().unwrap()))?),
            conditions: vec![],
            registration_sites: vec![site],
        });
    }
    items.sort_by(|a, b| a.id.cmp(&b.id));
    let mut ids = BTreeSet::new();
    for i in &items {
        if !ids.insert(&i.id) {
            return Err(format!("ambiguous inventory ID {}", i.id).into());
        }
    }
    for member in members {
        if !items
            .iter()
            .any(|i| i.kind == "workspace-crate" && i.path == format!("{member}/Cargo.toml"))
        {
            return Err(format!("unaccounted workspace member {member}").into());
        }
    }
    Ok(items)
}
pub fn check_dispositions(items: &[Item], d: &Value) -> Vec<Finding> {
    let mut findings = vec![];
    let mut seen = BTreeSet::new();
    let by_id: BTreeMap<_, _> = items.iter().map(|x| (x.id.as_str(), x)).collect();
    if let Some(records) = d.as_array() {
        for r in records {
            let id = r["id"].as_str().unwrap_or("");
            let path = by_id.get(id).map_or("inventory/dispositions.json", |x| x.path.as_str());
            let problem = if !seen.insert(id) {
                Some("duplicate disposition")
            } else if !by_id.contains_key(id) {
                Some("stale disposition")
            } else if ![
                "retain-align",
                "rebuild",
                "split",
                "delete",
                "specialist",
                "test-evidence",
                "optional-adapter",
            ]
            .contains(&r["disposition"].as_str().unwrap_or(""))
            {
                Some("missing or invalid disposition")
            } else if ["target", "source", "rationale"]
                .iter()
                .any(|k| r[k].as_str().unwrap_or("").is_empty())
            {
                Some("target, governing source and rationale required")
            } else {
                None
            };
            if let Some(p) = problem {
                findings.push(finding(
                    "INV-DISPOSITION",
                    path,
                    id,
                    "implementation plan §1.1/1.4",
                    p,
                ))
            }
        }
    }
    for i in items {
        if !seen.contains(i.id.as_str()) {
            findings.push(finding(
                "INV-DISPOSITION",
                &i.path,
                &i.id,
                "implementation plan §1.1/1.4",
                "missing disposition; review target ownership",
            ))
        }
    }
    findings
}
pub fn check(root: &Path) -> Result<Vec<Finding>> {
    let items = generate(root)?;
    let mut f = check_dispositions(&items, &read(&root.join("inventory/dispositions.json"))?);
    if json!(items) != read(&root.join("inventory/current.json"))? {
        f.push(finding(
            "INV-STALE",
            "inventory/current.json",
            "inventory",
            "WI-01",
            "regenerate inventory and review changed dispositions",
        ))
    }
    Ok(f)
}

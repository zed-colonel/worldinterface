//! Production continuation policy. Inspect tokens, including imports and macro bodies.
use crate::{finding, hash, policy, relative, Finding, Result};
use proc_macro2::{TokenStream, TokenTree};
use std::{collections::BTreeSet, fs, path::Path};

const CRASH_BARRIER: &str = "crates/worldinterface-foundation/storage/src/crashpoint.rs";
const CRASH_BARRIER_HASH: &str = "c8c9d9f983ca833ffdd0d22552379aea3a5976ce39b6a537d2d3ac6a784f4e14";
fn forbidden(tokens: TokenStream, found: &mut BTreeSet<String>) {
    for token in tokens {
        match token {
            TokenTree::Group(g) => forbidden(g.stream(), found),
            TokenTree::Ident(i) => {
                let name = i.to_string();
                let normalized = name.to_lowercase().replace('_', "");
                if [
                    "lazy_static",
                    "thread_local",
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
pub fn source_findings(path: &str, source: &str) -> Result<Vec<Finding>> {
    let production = policy::production_tokens(source)?;
    let mut found = BTreeSet::new();
    forbidden(production.parse()?, &mut found);
    // Static state cannot own continuation correctness. The sealed-plan cache
    // is the existing bounded compiler optimization, inspected separately.
    let ast = syn::parse_file(&production)?;
    use syn::visit::Visit;
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
    for path in policy::production_sources(&root)? {
        let rel = relative(&root, &path);
        if !rel.starts_with("crates/worldinterface-foundation/")
            && !rel.starts_with("crates/worldinterface-runtime/")
        {
            continue;
        }
        let source = fs::read_to_string(&path)?;
        if rel == CRASH_BARRIER {
            if hash(source.as_bytes()) != CRASH_BARRIER_HASH {
                findings.push(finding("CONTINUATION-CRASH-BARRIER",&rel,"approved development barrier","WIP-04 operator approval",
                    "only the exact approved crash barrier is exempt; production exclusion remains required"));
            }
            continue;
        }
        findings.extend(source_findings(&rel, &source)?);
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
        Routes { root: &root, path: &path, rel: &rel, findings: &mut findings }
            .visit_file(&syn::parse_file(&policy::production_tokens(&source)?)?);
    }
    Ok(findings)
}

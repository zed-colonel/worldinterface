use serde_json::{json, Value};
use worldinterface_core::descriptor::{ConnectorCategory, Descriptor};

use crate::context::InvocationContext;
use crate::error::ConnectorError;
use crate::semantic;
use crate::traits::Connector;

const DEFAULT_LIMIT: usize = 8;

pub struct CodeSymbolConnector;

impl Connector for CodeSymbolConnector {
    fn describe(&self) -> Descriptor {
        Descriptor {
            name: "code.symbol".into(),
            display_name: "Code Symbol".into(),
            description: "Finds functions, methods, structs, enums, traits, impl blocks, and modules in a Rust workspace using semantic symbol lookup.".into(),
            category: ConnectorCategory::Code,
            input_schema: Some(json!({
                "type": "object",
                "required": ["path", "query"],
                "properties": {
                    "path": { "type": "string", "description": "Absolute workspace root" },
                    "query": { "type": "string", "description": "Symbol name or semantic query" },
                    "path_hint": { "type": "string", "description": "Optional absolute path or module/path hint" },
                    "kind_hint": { "type": "string", "description": "Optional symbol kind hint" },
                    "limit": { "type": "integer", "minimum": 1, "description": "Maximum matches to return. Default: 8" }
                }
            })),
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "content": { "type": "string" },
                    "workspace_root": { "type": "string" },
                    "matches": { "type": "array" }
                }
            })),
            idempotent: true,
            side_effects: false,
            is_read_only: true,
            is_mutating: false,
            is_concurrency_safe: true,
            requires_read_before_write: false,
        }
    }

    fn invoke(&self, _ctx: &InvocationContext, params: &Value) -> Result<Value, ConnectorError> {
        let path = params.get("path").and_then(Value::as_str).ok_or_else(|| {
            ConnectorError::InvalidParams("missing or invalid 'path' (expected string)".into())
        })?;
        let query = params.get("query").and_then(Value::as_str).ok_or_else(|| {
            ConnectorError::InvalidParams("missing or invalid 'query' (expected string)".into())
        })?;
        let path_hint = params.get("path_hint").and_then(Value::as_str);
        let kind_hint = params.get("kind_hint").and_then(Value::as_str);
        let limit =
            params.get("limit").and_then(Value::as_u64).unwrap_or(DEFAULT_LIMIT as u64) as usize;

        let (workspace_root, matches) =
            semantic::lookup_symbol(path, query, path_hint, kind_hint, limit)?;

        Ok(json!({
            "content": render_symbol_matches(&matches),
            "workspace_root": workspace_root.display().to_string(),
            "matches": matches,
        }))
    }
}

fn render_symbol_matches(matches: &[semantic::SemanticSymbolMatch]) -> String {
    if matches.is_empty() {
        return "No symbol matches found.".into();
    }

    let mut out = String::from("Semantic symbol matches:\n");
    for item in matches {
        let container =
            item.container_name.as_deref().map(|value| format!(" in {value}")).unwrap_or_default();
        let signature = item
            .signature_summary
            .as_deref()
            .map(|value| format!(" [{value}]"))
            .unwrap_or_default();
        out.push_str(&format!(
            "- {} {} {}:{}-{}{}{}\n",
            item.kind,
            item.name,
            item.file_path,
            item.range.start_line,
            item.range.end_line,
            container,
            signature,
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;
    use worldinterface_core::id::{FlowRunId, NodeId, StepRunId};

    use super::*;
    use crate::context::CancellationToken;

    fn ctx() -> InvocationContext {
        InvocationContext {
            flow_run_id: FlowRunId::new(),
            node_id: NodeId::new(),
            step_run_id: StepRunId::new(),
            run_id: Uuid::new_v4(),
            attempt_id: Uuid::new_v4(),
            attempt_number: 1,
            cancellation: CancellationToken::new(),
        }
    }

    fn fixture() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("Cargo.toml"),
            "[package]\nname = \"symbol-fixture\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        )
        .unwrap();
        std::fs::create_dir_all(dir.path().join("src")).unwrap();
        std::fs::write(
            dir.path().join("src/lib.rs"),
            "pub fn parse_config_line() -> bool { true }\n",
        )
        .unwrap();
        dir
    }

    #[test]
    fn code_symbol_returns_match() {
        let dir = fixture();
        let result = CodeSymbolConnector
            .invoke(
                &ctx(),
                &json!({
                    "path": dir.path().to_str().unwrap(),
                    "query": "parse_config_line",
                }),
            )
            .unwrap();
        assert_eq!(result["matches"][0]["name"], "parse_config_line");
    }
}

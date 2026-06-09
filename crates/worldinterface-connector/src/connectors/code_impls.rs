use serde_json::{json, Value};
use worldinterface_core::descriptor::{ConnectorCategory, Descriptor};

use crate::context::InvocationContext;
use crate::error::ConnectorError;
use crate::semantic::{self, SymbolSelector};
use crate::traits::Connector;

const DEFAULT_LIMIT: usize = 12;

pub struct CodeImplsConnector;

impl Connector for CodeImplsConnector {
    fn describe(&self) -> Descriptor {
        Descriptor {
            name: "code.impls".into(),
            display_name: "Code Impls".into(),
            description: "Finds impl blocks for a Rust type or trait inside a workspace.".into(),
            category: ConnectorCategory::Code,
            input_schema: Some(json!({
                "type": "object",
                "required": ["path"],
                "properties": {
                    "path": { "type": "string", "description": "Absolute workspace root" },
                    "symbol_id": { "type": "string" },
                    "query": { "type": "string" },
                    "path_hint": { "type": "string" },
                    "kind_hint": { "type": "string" },
                    "limit": { "type": "integer", "minimum": 1 }
                }
            })),
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "content": { "type": "string" },
                    "workspace_root": { "type": "string" },
                    "impls": { "type": "array" },
                    "summary": { "type": "string" }
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
        let selector = parse_selector(params)?;
        let limit =
            params.get("limit").and_then(Value::as_u64).unwrap_or(DEFAULT_LIMIT as u64) as usize;
        let (workspace_root, impls) = semantic::find_impls(path, selector, limit)?;

        Ok(json!({
            "content": render_impls(&impls),
            "workspace_root": workspace_root.display().to_string(),
            "impls": impls,
            "summary": format!("{} impl blocks", impls.len()),
        }))
    }
}

fn parse_selector(params: &Value) -> Result<SymbolSelector, ConnectorError> {
    if let Some(symbol_id) = params.get("symbol_id").and_then(Value::as_str) {
        if !symbol_id.trim().is_empty() {
            return Ok(SymbolSelector::SymbolId(symbol_id.to_string()));
        }
    }

    let query = params.get("query").and_then(Value::as_str).ok_or_else(|| {
        ConnectorError::InvalidParams("code.impls requires 'symbol_id' or 'query'".into())
    })?;
    Ok(SymbolSelector::Query {
        query: query.to_string(),
        path_hint: params.get("path_hint").and_then(Value::as_str).map(str::to_string),
        kind_hint: params.get("kind_hint").and_then(Value::as_str).map(str::to_string),
    })
}

fn render_impls(impls: &[semantic::SemanticImplMatch]) -> String {
    if impls.is_empty() {
        return "No impl blocks found.".into();
    }
    let mut out = String::from("Impl blocks:\n");
    for item in impls {
        out.push_str(&format!(
            "- {}:{}-{} :: {}\n",
            item.file_path, item.range.start_line, item.range.end_line, item.impl_summary
        ));
    }
    out
}

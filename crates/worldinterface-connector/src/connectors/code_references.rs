use serde_json::{json, Value};
use worldinterface_core::descriptor::{ConnectorCategory, Descriptor};

use crate::context::InvocationContext;
use crate::error::ConnectorError;
use crate::semantic::{self, SymbolSelector};
use crate::traits::Connector;

const DEFAULT_LIMIT: usize = 12;

pub struct CodeReferencesConnector;

impl Connector for CodeReferencesConnector {
    fn describe(&self) -> Descriptor {
        Descriptor {
            name: "code.references".into(),
            display_name: "Code References".into(),
            description:
                "Finds semantic/reference-like occurrences for a symbol across a Rust workspace."
                    .into(),
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
                    "limit": { "type": "integer", "minimum": 1 },
                    "include_tests": { "type": "boolean", "description": "Include test files and tests/ modules. Default: true" }
                }
            })),
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "content": { "type": "string" },
                    "workspace_root": { "type": "string" },
                    "references": { "type": "array" },
                    "summary": { "type": "string" },
                    "truncated": { "type": "boolean" }
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
        let include_tests = params.get("include_tests").and_then(Value::as_bool).unwrap_or(true);

        let (workspace_root, references) =
            semantic::find_references(path, selector, limit, include_tests)?;
        let truncated = references.len() >= limit.max(1);

        Ok(json!({
            "content": render_references(&references),
            "workspace_root": workspace_root.display().to_string(),
            "references": references,
            "summary": format!("{} references", references.len()),
            "truncated": truncated,
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
        ConnectorError::InvalidParams("code.references requires 'symbol_id' or 'query'".into())
    })?;
    Ok(SymbolSelector::Query {
        query: query.to_string(),
        path_hint: params.get("path_hint").and_then(Value::as_str).map(str::to_string),
        kind_hint: params.get("kind_hint").and_then(Value::as_str).map(str::to_string),
    })
}

fn render_references(references: &[semantic::SemanticReferenceMatch]) -> String {
    if references.is_empty() {
        return "No references found.".into();
    }
    let mut out = String::from("References:\n");
    for item in references {
        out.push_str(&format!(
            "- {}:{}:{} :: {}\n",
            item.file_path, item.range.start_line, item.range.start_col, item.context_snippet
        ));
    }
    out
}

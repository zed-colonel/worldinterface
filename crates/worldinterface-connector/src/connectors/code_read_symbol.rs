use serde_json::{json, Value};
use worldinterface_core::descriptor::{ConnectorCategory, Descriptor};

use crate::context::InvocationContext;
use crate::error::ConnectorError;
use crate::semantic::{self, SymbolSelector};
use crate::traits::Connector;

pub struct CodeReadSymbolConnector;

impl Connector for CodeReadSymbolConnector {
    fn describe(&self) -> Descriptor {
        Descriptor {
            name: "code.read_symbol".into(),
            display_name: "Code Read Symbol".into(),
            description: "Reads the exact source span for a resolved symbol or impl block from a Rust workspace.".into(),
            category: ConnectorCategory::Code,
            input_schema: Some(json!({
                "type": "object",
                "required": ["path"],
                "properties": {
                    "path": { "type": "string", "description": "Absolute workspace root" },
                    "symbol_id": { "type": "string", "description": "Symbol id returned by code.symbol" },
                    "query": { "type": "string", "description": "Fallback symbol query when symbol_id is not provided" },
                    "path_hint": { "type": "string" },
                    "kind_hint": { "type": "string" },
                    "include_body": { "type": "boolean", "description": "Whether to include the full symbol body. Default: true" }
                }
            })),
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "content": { "type": "string" },
                    "workspace_root": { "type": "string" },
                    "symbol": { "type": "object" },
                    "file_path": { "type": "string" },
                    "range": { "type": "object" }
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
        let include_body = params.get("include_body").and_then(Value::as_bool).unwrap_or(true);
        let selector = parse_selector(params)?;
        let (workspace_root, result) = semantic::read_symbol(path, selector, include_body)?;

        Ok(json!({
            "content": result.content,
            "workspace_root": workspace_root.display().to_string(),
            "symbol": result.symbol,
            "file_path": result.file_path,
            "range": result.range,
            "enclosing_context": result.enclosing_context,
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
        ConnectorError::InvalidParams("code.read_symbol requires 'symbol_id' or 'query'".into())
    })?;
    Ok(SymbolSelector::Query {
        query: query.to_string(),
        path_hint: params.get("path_hint").and_then(Value::as_str).map(str::to_string),
        kind_hint: params.get("kind_hint").and_then(Value::as_str).map(str::to_string),
    })
}

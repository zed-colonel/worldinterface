use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticRange {
    pub start_line: u32,
    pub start_col: u32,
    pub end_line: u32,
    pub end_col: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticSymbolMatch {
    pub symbol_id: String,
    pub name: String,
    pub kind: String,
    pub file_path: String,
    pub range: SemanticRange,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature_summary: Option<String>,
    pub score: i64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub why: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticReadResult {
    pub symbol: SemanticSymbolMatch,
    pub content: String,
    pub file_path: String,
    pub range: SemanticRange,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enclosing_context: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticReferenceMatch {
    pub file_path: String,
    pub range: SemanticRange,
    pub context_snippet: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticImplMatch {
    pub symbol_id: String,
    pub file_path: String,
    pub range: SemanticRange,
    pub type_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trait_name: Option<String>,
    pub impl_summary: String,
}

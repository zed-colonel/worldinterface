//! Output formatting for CLI display.

use worldinterface_core::descriptor::Descriptor;
use worldinterface_host::{FlowRunStatus, FlowRunSummary};

use crate::client::WebhookSummary;

/// Format a list of flow run summaries as a table.
pub fn format_run_list(runs: &[FlowRunSummary]) -> String {
    if runs.is_empty() {
        return "No flow runs found.".to_string();
    }

    let mut lines = Vec::new();
    lines.push(format!(
        "{:<38} {:<12} {:>14} {:>14}",
        "FLOW_RUN_ID", "PHASE", "SUBMITTED", "LAST UPDATED"
    ));
    for run in runs {
        lines.push(format!(
            "{:<38} {:<12} {:>14} {:>14}",
            run.flow_run_id,
            format_phase(run.phase),
            format_timestamp(run.submitted_at),
            format_timestamp(run.last_updated_at),
        ));
    }
    lines.join("\n")
}

/// Format a detailed flow run status for inspection.
pub fn format_run_inspect(status: &FlowRunStatus) -> String {
    let mut lines = Vec::new();

    lines.push(format!("Flow Run: {}", status.flow_run_id));
    lines.push(format!("Phase:    {}", format_phase(status.phase)));
    lines.push(format!("Submitted:    {}", format_timestamp(status.submitted_at)));
    lines.push(format!("Last Updated: {}", format_timestamp(status.last_updated_at)));

    if let Some(ref error) = status.error {
        lines.push(format!("Error:    {}", error));
    }

    if let Some(ref receipt) = status.trigger_receipt {
        lines.push(String::new());
        lines.push("Trigger Receipt:".to_string());
        lines.push(format!(
            "  Connector: {}",
            receipt.get("connector").and_then(|v| v.as_str()).unwrap_or("-")
        ));
        lines.push(format!(
            "  Status:    {}",
            receipt.get("status").and_then(|v| v.as_str()).unwrap_or("-")
        ));
    }

    if !status.steps.is_empty() {
        lines.push(String::new());
        lines.push("Steps:".to_string());
        lines.push(format!(
            "  {:<38} {:<16} {:<12} {:<12} {}",
            "NODE_ID", "CONNECTOR", "PHASE", "DURATION", "OUTPUT"
        ));
        for step in &status.steps {
            let connector_str = step.connector.as_deref().unwrap_or("-");
            let duration_str = step
                .receipt
                .as_ref()
                .and_then(|r| r.get("duration_ms"))
                .and_then(|v| v.as_u64())
                .map(|ms| format!("{}ms", ms))
                .unwrap_or_else(|| "-".to_string());
            let output_str =
                step.output.as_ref().map(|v| truncate(&v.to_string(), 40)).unwrap_or_default();

            lines.push(format!(
                "  {:<38} {:<16} {:<12} {:<12} {}",
                step.label.as_deref().unwrap_or(&step.node_id.to_string()),
                truncate(connector_str, 14),
                format_step_phase(step.phase),
                duration_str,
                output_str,
            ));

            if let Some(ref error) = step.error {
                lines.push(format!("    Error: {}", truncate(error, 70)));
            }
        }
    }

    if let Some(ref outputs) = status.outputs {
        lines.push(String::new());
        lines.push("Outputs:".to_string());
        for (node_id, value) in outputs {
            lines.push(format!("  {}: {}", node_id, value));
        }
    }

    lines.join("\n")
}

/// Format a list of capabilities as a table.
pub fn format_capabilities_list(descriptors: &[Descriptor]) -> String {
    if descriptors.is_empty() {
        return "No connectors registered.".to_string();
    }

    let mut lines = Vec::new();
    lines.push(format!(
        "{:<16} {:<14} {:<12} {:<14} {}",
        "NAME", "CATEGORY", "IDEMPOTENT", "SIDE_EFFECTS", "DESCRIPTION"
    ));
    for d in descriptors {
        lines.push(format!(
            "{:<16} {:<14} {:<12} {:<14} {}",
            d.name,
            format!("{:?}", d.category).to_lowercase(),
            if d.idempotent { "yes" } else { "no" },
            if d.side_effects { "yes" } else { "no" },
            truncate(&d.description, 50),
        ));
    }
    lines.join("\n")
}

/// Format a detailed capability description.
pub fn format_capability_describe(d: &Descriptor) -> String {
    let mut lines = Vec::new();
    lines.push(format!("Name:         {}", d.name));
    lines.push(format!("Display Name: {}", d.display_name));
    lines.push(format!("Category:     {}", format!("{:?}", d.category).to_lowercase()));
    lines.push(format!("Description:  {}", d.description));
    lines.push(format!("Idempotent:   {}", if d.idempotent { "yes" } else { "no" }));
    lines.push(format!("Side Effects: {}", if d.side_effects { "yes" } else { "no" }));

    if let Some(ref schema) = d.input_schema {
        lines.push(String::new());
        lines.push("Input Schema:".to_string());
        if let Ok(pretty) = serde_json::to_string_pretty(schema) {
            for line in pretty.lines() {
                lines.push(format!("  {}", line));
            }
        }
    }

    if let Some(ref schema) = d.output_schema {
        lines.push(String::new());
        lines.push("Output Schema:".to_string());
        if let Ok(pretty) = serde_json::to_string_pretty(schema) {
            for line in pretty.lines() {
                lines.push(format!("  {}", line));
            }
        }
    }

    lines.join("\n")
}

/// Format a list of webhook summaries as a table.
pub fn format_webhook_list(webhooks: &[WebhookSummary]) -> String {
    if webhooks.is_empty() {
        return "No webhooks registered.".to_string();
    }

    let mut lines = Vec::new();
    lines.push(format!("{:<38} {:<18} {:<22} {}", "WEBHOOK_ID", "PATH", "CREATED", "INVOKE_URL"));
    for w in webhooks {
        lines.push(format!(
            "{:<38} {:<18} {:<22} {}",
            w.id,
            truncate(&w.path, 16),
            format_timestamp(w.created_at),
            w.invoke_url,
        ));
    }
    lines.join("\n")
}

fn format_phase(phase: worldinterface_host::FlowPhase) -> &'static str {
    match phase {
        worldinterface_host::FlowPhase::Pending => "Pending",
        worldinterface_host::FlowPhase::Running => "Running",
        worldinterface_host::FlowPhase::Completed => "Completed",
        worldinterface_host::FlowPhase::Failed => "Failed",
        worldinterface_host::FlowPhase::Canceled => "Canceled",
    }
}

fn format_step_phase(phase: worldinterface_host::StepPhase) -> &'static str {
    match phase {
        worldinterface_host::StepPhase::Pending => "Pending",
        worldinterface_host::StepPhase::Running => "Running",
        worldinterface_host::StepPhase::Completed => "Completed",
        worldinterface_host::StepPhase::Failed => "Failed",
        worldinterface_host::StepPhase::Excluded => "Excluded",
    }
}

fn format_timestamp(epoch_secs: u64) -> String {
    if epoch_secs == 0 {
        return "-".to_string();
    }
    chrono::DateTime::from_timestamp(epoch_secs as i64, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| epoch_secs.to_string())
}

fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len - 3])
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;
    use worldinterface_core::descriptor::ConnectorCategory;
    use worldinterface_core::id::{FlowRunId, NodeId};
    use worldinterface_host::{FlowPhase, FlowRunStatus, StepPhase, StepStatus};

    use super::*;

    fn sample_summary(phase: FlowPhase) -> FlowRunSummary {
        FlowRunSummary {
            flow_run_id: FlowRunId::new(),
            phase,
            submitted_at: 1741200000,
            last_updated_at: 1741200005,
        }
    }

    fn sample_descriptor() -> Descriptor {
        Descriptor {
            name: "delay".to_string(),
            display_name: "Delay".to_string(),
            description: "Pauses execution for a specified duration".to_string(),
            category: ConnectorCategory::Delay,
            input_schema: Some(json!({"type": "object"})),
            output_schema: None,
            idempotent: true,
            side_effects: false,
            is_read_only: true,
            is_mutating: false,
            is_concurrency_safe: true,
            requires_read_before_write: false,
        }
    }

    #[test]
    fn format_run_list_empty() {
        let output = format_run_list(&[]);
        assert_eq!(output, "No flow runs found.");
    }

    #[test]
    fn format_run_list_table() {
        let runs = vec![sample_summary(FlowPhase::Completed), sample_summary(FlowPhase::Running)];
        let output = format_run_list(&runs);
        assert!(output.contains("FLOW_RUN_ID"));
        assert!(output.contains("PHASE"));
        assert!(output.contains("Completed"));
        assert!(output.contains("Running"));
        // Should have header + 2 data rows
        assert_eq!(output.lines().count(), 3);
    }

    #[test]
    fn format_capabilities_list_empty() {
        let output = format_capabilities_list(&[]);
        assert_eq!(output, "No connectors registered.");
    }

    #[test]
    fn format_capabilities_table() {
        let descriptors = vec![sample_descriptor()];
        let output = format_capabilities_list(&descriptors);
        assert!(output.contains("NAME"));
        assert!(output.contains("delay"));
        assert!(output.contains("yes")); // idempotent
        assert!(output.contains("no")); // side_effects
    }

    #[test]
    fn format_run_inspect_completed() {
        let node_id = NodeId::new();
        let mut outputs = HashMap::new();
        outputs.insert(node_id, json!({"result": 42}));
        let status = FlowRunStatus {
            flow_run_id: FlowRunId::new(),
            phase: FlowPhase::Completed,
            steps: vec![StepStatus {
                node_id,
                phase: StepPhase::Completed,
                output: Some(json!({"result": 42})),
                receipt: None,
                error: None,
                label: None,
                connector: None,
            }],
            outputs: Some(outputs),
            error: None,
            submitted_at: 1741200000,
            last_updated_at: 1741200005,
            trigger_receipt: None,
        };
        let output = format_run_inspect(&status);
        assert!(output.contains("Completed"));
        assert!(output.contains("Steps:"));
        assert!(output.contains("Outputs:"));
        assert!(!output.contains("Error:"));
    }

    #[test]
    fn format_run_inspect_failed() {
        let status = FlowRunStatus {
            flow_run_id: FlowRunId::new(),
            phase: FlowPhase::Failed,
            steps: vec![],
            outputs: None,
            error: Some("step failed: file not found".to_string()),
            submitted_at: 1741200000,
            last_updated_at: 1741200005,
            trigger_receipt: None,
        };
        let output = format_run_inspect(&status);
        assert!(output.contains("Failed"));
        assert!(output.contains("Error:"));
        assert!(output.contains("file not found"));
    }

    // T-12: CLI Output Enrichment

    #[test]
    fn format_run_inspect_shows_connector() {
        let node_id = NodeId::new();
        let status = FlowRunStatus {
            flow_run_id: FlowRunId::new(),
            phase: FlowPhase::Completed,
            steps: vec![StepStatus {
                node_id,
                phase: StepPhase::Completed,
                output: Some(json!({"ok": true})),
                receipt: None,
                error: None,
                label: None,
                connector: Some("delay".to_string()),
            }],
            outputs: None,
            error: None,
            submitted_at: 1741200000,
            last_updated_at: 1741200005,
            trigger_receipt: None,
        };
        let output = format_run_inspect(&status);
        assert!(output.contains("CONNECTOR"));
        assert!(output.contains("delay"));
    }

    #[test]
    fn format_run_inspect_shows_label() {
        let node_id = NodeId::new();
        let status = FlowRunStatus {
            flow_run_id: FlowRunId::new(),
            phase: FlowPhase::Completed,
            steps: vec![StepStatus {
                node_id,
                phase: StepPhase::Completed,
                output: None,
                receipt: None,
                error: None,
                label: Some("fetch user data".to_string()),
                connector: Some("http.request".to_string()),
            }],
            outputs: None,
            error: None,
            submitted_at: 1741200000,
            last_updated_at: 1741200005,
            trigger_receipt: None,
        };
        let output = format_run_inspect(&status);
        // Label should appear instead of UUID
        assert!(output.contains("fetch user data"));
    }

    #[test]
    fn format_run_inspect_shows_step_error() {
        let node_id = NodeId::new();
        let status = FlowRunStatus {
            flow_run_id: FlowRunId::new(),
            phase: FlowPhase::Failed,
            steps: vec![StepStatus {
                node_id,
                phase: StepPhase::Failed,
                output: None,
                receipt: None,
                error: Some("connector timeout".to_string()),
                label: None,
                connector: Some("http.request".to_string()),
            }],
            outputs: None,
            error: Some("flow failed".to_string()),
            submitted_at: 1741200000,
            last_updated_at: 1741200005,
            trigger_receipt: None,
        };
        let output = format_run_inspect(&status);
        assert!(output.contains("Error: connector timeout"));
    }

    #[test]
    fn format_run_inspect_shows_duration() {
        let node_id = NodeId::new();
        let status = FlowRunStatus {
            flow_run_id: FlowRunId::new(),
            phase: FlowPhase::Completed,
            steps: vec![StepStatus {
                node_id,
                phase: StepPhase::Completed,
                output: Some(json!({"ok": true})),
                receipt: Some(json!({"duration_ms": 245, "connector": "delay"})),
                error: None,
                label: None,
                connector: Some("delay".to_string()),
            }],
            outputs: None,
            error: None,
            submitted_at: 1741200000,
            last_updated_at: 1741200005,
            trigger_receipt: None,
        };
        let output = format_run_inspect(&status);
        assert!(output.contains("DURATION"));
        assert!(output.contains("245ms"));
    }

    #[test]
    fn format_run_inspect_shows_trigger_receipt() {
        let status = FlowRunStatus {
            flow_run_id: FlowRunId::new(),
            phase: FlowPhase::Completed,
            steps: vec![],
            outputs: None,
            error: None,
            submitted_at: 1741200000,
            last_updated_at: 1741200005,
            trigger_receipt: Some(json!({
                "connector": "webhook.trigger",
                "status": "Success"
            })),
        };
        let output = format_run_inspect(&status);
        assert!(output.contains("Trigger Receipt:"));
        assert!(output.contains("webhook.trigger"));
        assert!(output.contains("Success"));
    }

    #[test]
    fn format_capability_describe_includes_schema() {
        let d = sample_descriptor();
        let output = format_capability_describe(&d);
        assert!(output.contains("Name:         delay"));
        assert!(output.contains("Display Name: Delay"));
        assert!(output.contains("Idempotent:   yes"));
        assert!(output.contains("Side Effects: no"));
        assert!(output.contains("Input Schema:"));
    }

    #[test]
    fn truncate_short_string_unchanged() {
        assert_eq!(truncate("hello", 10), "hello");
    }

    #[test]
    fn truncate_long_string_adds_ellipsis() {
        assert_eq!(truncate("hello world", 8), "hello...");
    }

    #[test]
    fn format_timestamp_zero_returns_dash() {
        assert_eq!(format_timestamp(0), "-");
    }

    #[test]
    fn format_timestamp_valid_epoch() {
        let result = format_timestamp(1741200000);
        // Should be a date string, not raw number
        assert!(result.contains("2025"), "expected date string, got: {}", result);
    }
}

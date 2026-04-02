//! The `sandbox.exec` connector — restricted command execution for sandboxed code experimentation.
//!
//! Wraps `ShellExecConnector` with enforced constraints:
//! - Working directory locked to `/sandbox`
//! - Reduced timeout (10s default, 60s max)
//! - Reduced output limit (256KB)
//! - Shell mode disabled
//! - Minimal environment (PATH, HOME, LANG only)
//! - Path validation (no /data access)

use std::time::Duration;

use serde_json::{json, Value};
use worldinterface_core::descriptor::{ConnectorCategory, Descriptor};

use super::shell_exec::ShellExecConnector;
use crate::context::InvocationContext;
use crate::error::ConnectorError;
use crate::traits::Connector;

/// Restricted command execution for sandboxed code experimentation.
///
/// Wraps `ShellExecConnector` with enforced constraints:
/// - Working directory locked to `/sandbox`
/// - Reduced timeout (10s default, 60s max)
/// - Reduced output limit (256KB)
/// - Shell mode disabled
/// - Minimal environment (PATH, HOME, LANG only)
/// - Path validation (no /data access)
pub struct SandboxExecConnector {
    /// The underlying shell.exec connector with sandbox-appropriate limits.
    inner: ShellExecConnector,
    /// Sandbox working directory. Default: "/sandbox".
    sandbox_dir: String,
    /// Paths that sandbox commands cannot reference in arguments.
    /// Default: ["/data"].
    forbidden_path_prefixes: Vec<String>,
}

impl SandboxExecConnector {
    pub fn new() -> Self {
        let inner = ShellExecConnector::with_limits(
            Duration::from_secs(10), // default timeout
            Duration::from_secs(60), // max timeout
            262_144,                 // 256KB max output per stream
            vec!["PATH".into(), "HOME".into(), "LANG".into()],
        );
        Self {
            inner,
            sandbox_dir: "/sandbox".into(),
            forbidden_path_prefixes: vec!["/data".into()],
        }
    }

    /// Create a sandbox connector with a custom sandbox directory.
    ///
    /// Used in testing (where `/sandbox` may not exist) and for custom deployments.
    pub fn with_sandbox_dir(sandbox_dir: &str) -> Self {
        let inner = ShellExecConnector::with_limits(
            Duration::from_secs(10),
            Duration::from_secs(60),
            262_144,
            vec!["PATH".into(), "HOME".into(), "LANG".into()],
        );
        Self {
            inner,
            sandbox_dir: sandbox_dir.into(),
            forbidden_path_prefixes: vec!["/data".into()],
        }
    }
}

impl Default for SandboxExecConnector {
    fn default() -> Self {
        Self::new()
    }
}

impl Connector for SandboxExecConnector {
    fn describe(&self) -> Descriptor {
        Descriptor {
            name: "sandbox.exec".into(),
            display_name: "Sandbox Execute".into(),
            description: "Executes a command in the isolated sandbox at /sandbox. Use for code \
                          experimentation, data processing, and safe testing. Available runtimes: \
                          python3 (with venv support), shell utilities (curl, jq, git). \
                          Constraints: 60s max timeout, 256KB output limit, no /data access. The \
                          command is a binary path (e.g. \"python3\") with separate args."
                .into(),
            category: ConnectorCategory::Sandbox,
            input_schema: Some(json!({
                "type": "object",
                "required": ["command"],
                "properties": {
                    "command": {
                        "type": "string",
                        "description": "Binary path to execute (e.g. \"python3\", \"curl\"). No shell expansion."
                    },
                    "args": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Arguments to the command. For python: [\"-c\", \"print('hello')\"]"
                    },
                    "env": { "type": "object" },
                    "timeout_ms": { "type": "integer", "minimum": 0, "maximum": 60000 }
                }
            })),
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "stdout": { "type": "string" },
                    "stderr": { "type": "string" },
                    "exit_code": { "type": ["integer", "null"] },
                    "timed_out": { "type": "boolean" },
                    "truncated": { "type": "boolean" }
                }
            })),
            idempotent: false,
            side_effects: true,
            is_read_only: false,
            is_mutating: true,
            is_concurrency_safe: false,
            requires_read_before_write: false,
        }
    }

    fn invoke(&self, ctx: &InvocationContext, params: &Value) -> Result<Value, ConnectorError> {
        // 1. Reject shell mode
        if params.get("shell").and_then(Value::as_bool).unwrap_or(false) {
            return Err(ConnectorError::InvalidParams(
                "sandbox.exec does not allow shell mode".into(),
            ));
        }

        // 2. Validate command doesn't reference forbidden paths
        if let Some(cmd) = params.get("command").and_then(Value::as_str) {
            for prefix in &self.forbidden_path_prefixes {
                if cmd.starts_with(prefix) {
                    return Err(ConnectorError::InvalidParams(format!(
                        "sandbox.exec: command references forbidden path '{prefix}'"
                    )));
                }
            }
        }

        // 3. Validate args don't reference forbidden paths
        if let Some(args) = params.get("args").and_then(Value::as_array) {
            for arg in args {
                if let Some(s) = arg.as_str() {
                    for prefix in &self.forbidden_path_prefixes {
                        if s.starts_with(prefix) {
                            return Err(ConnectorError::InvalidParams(format!(
                                "sandbox.exec: argument references forbidden path '{prefix}'"
                            )));
                        }
                    }
                }
            }
        }

        // 4. Build sandboxed params: force working_dir, strip shell
        let mut sandboxed = params.clone();
        if let Some(obj) = sandboxed.as_object_mut() {
            obj.insert("working_dir".into(), json!(self.sandbox_dir));
            obj.remove("shell");
        }

        // 5. Delegate to inner ShellExecConnector
        self.inner.invoke(ctx, &sandboxed)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;
    use worldinterface_core::id::{FlowRunId, NodeId, StepRunId};

    use super::*;
    use crate::context::{CancellationToken, InvocationContext};

    fn test_ctx() -> InvocationContext {
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

    fn test_connector() -> SandboxExecConnector {
        let dir = std::env::temp_dir();
        SandboxExecConnector::with_sandbox_dir(dir.to_str().unwrap())
    }

    async fn invoke_on_blocking_thread(params: Value) -> Result<Value, ConnectorError> {
        let connector = test_connector();
        let ctx = test_ctx();
        tokio::task::spawn_blocking(move || connector.invoke(&ctx, &params))
            .await
            .expect("spawn_blocking join failed")
    }

    async fn invoke_on_blocking_thread_with_connector(
        connector: SandboxExecConnector,
        params: Value,
    ) -> Result<Value, ConnectorError> {
        let ctx = test_ctx();
        tokio::task::spawn_blocking(move || connector.invoke(&ctx, &params))
            .await
            .expect("spawn_blocking join failed")
    }

    async fn invoke_on_blocking_thread_with_ctx(
        ctx: InvocationContext,
        params: Value,
    ) -> Result<Value, ConnectorError> {
        let connector = test_connector();
        tokio::task::spawn_blocking(move || connector.invoke(&ctx, &params))
            .await
            .expect("spawn_blocking join failed")
    }

    // ── E2S2-T1: Runs in sandbox dir ──

    #[tokio::test]
    async fn sandbox_exec_runs_in_sandbox_dir() {
        let dir = tempfile::tempdir().unwrap();
        let connector = SandboxExecConnector::with_sandbox_dir(dir.path().to_str().unwrap());
        let result =
            invoke_on_blocking_thread_with_connector(connector, json!({ "command": "pwd" }))
                .await
                .unwrap();

        let stdout = result["stdout"].as_str().unwrap().trim();
        assert_eq!(stdout, dir.path().to_str().unwrap());
    }

    // ── E2S2-T2: Ignores working_dir param ──

    #[tokio::test]
    async fn sandbox_exec_ignores_working_dir_param() {
        let dir = tempfile::tempdir().unwrap();
        let connector = SandboxExecConnector::with_sandbox_dir(dir.path().to_str().unwrap());
        let result = invoke_on_blocking_thread_with_connector(
            connector,
            json!({
                "command": "pwd",
                "working_dir": "/tmp"
            }),
        )
        .await
        .unwrap();

        let stdout = result["stdout"].as_str().unwrap().trim();
        // Should be the sandbox dir, NOT /tmp
        assert_eq!(stdout, dir.path().to_str().unwrap());
    }

    // ── E2S2-T3: Rejects shell mode ──

    #[tokio::test]
    async fn sandbox_exec_rejects_shell_mode() {
        let result = invoke_on_blocking_thread(json!({
            "command": "echo hello",
            "shell": true
        }))
        .await;

        assert!(
            matches!(result, Err(ConnectorError::InvalidParams(msg)) if msg.contains("shell mode"))
        );
    }

    // ── E2S2-T4: Rejects /data path in args ──

    #[tokio::test]
    async fn sandbox_exec_rejects_data_path_in_args() {
        let result = invoke_on_blocking_thread(json!({
            "command": "cat",
            "args": ["/data/secret"]
        }))
        .await;

        assert!(matches!(result, Err(ConnectorError::InvalidParams(msg)) if msg.contains("/data")));
    }

    // ── E2S2-T5: Rejects /data path in command ──

    #[tokio::test]
    async fn sandbox_exec_rejects_data_path_in_command() {
        let result = invoke_on_blocking_thread(json!({
            "command": "/data/bin/evil"
        }))
        .await;

        assert!(matches!(result, Err(ConnectorError::InvalidParams(msg)) if msg.contains("/data")));
    }

    // ── E2S2-T6: Timeout enforced ──

    #[tokio::test]
    async fn sandbox_exec_timeout_enforced() {
        let start = std::time::Instant::now();
        let result = invoke_on_blocking_thread(json!({
            "command": "sleep",
            "args": ["60"]
        }))
        .await
        .unwrap();

        let elapsed = start.elapsed();
        assert_eq!(result["timed_out"], true);
        // Default timeout is 10s — should complete well before 60s
        assert!(
            elapsed < std::time::Duration::from_secs(30),
            "Timeout should have fired, elapsed: {elapsed:?}"
        );
    }

    // ── E2S2-T7: Timeout clamped to max ──

    #[tokio::test]
    async fn sandbox_exec_timeout_clamped_to_max() {
        // Max is 60s. Request 120000ms. Should be clamped, not rejected.
        // Just verify it doesn't error by running a quick command.
        let result = invoke_on_blocking_thread(json!({
            "command": "echo",
            "args": ["clamped"],
            "timeout_ms": 120000
        }))
        .await
        .unwrap();

        assert_eq!(result["stdout"], "clamped\n");
    }

    // ── E2S2-T8: Output limit ──

    #[tokio::test]
    async fn sandbox_exec_output_limit() {
        // Sandbox limit is 256KB (262144 bytes). Generate >256KB output.
        let connector = test_connector();
        let result = invoke_on_blocking_thread_with_connector(
            connector,
            json!({
                "command": "sh",
                "args": ["-c", "dd if=/dev/zero bs=1024 count=300 2>/dev/null | tr '\\0' 'A'"]
            }),
        )
        .await
        .unwrap();

        assert_eq!(result["truncated"], true);
        assert!(result["stdout"].as_str().unwrap().len() <= 262_144);
    }

    // ── E2S2-T9: Minimal environment ──

    #[tokio::test]
    async fn sandbox_exec_minimal_env() {
        let result = invoke_on_blocking_thread(json!({
            "command": "env"
        }))
        .await
        .unwrap();

        let stdout = result["stdout"].as_str().unwrap();
        // Should have PATH, HOME, LANG — but NOT USER, TERM
        assert!(stdout.contains("PATH="));
        assert!(!stdout.contains("USER="));
        assert!(!stdout.contains("TERM="));
    }

    // ── E2S2-T10: Env vars merged ──

    #[tokio::test]
    async fn sandbox_exec_env_vars_merged() {
        let result = invoke_on_blocking_thread(json!({
            "command": "env",
            "env": { "MY_SANDBOX_VAR": "test_value" }
        }))
        .await
        .unwrap();

        let stdout = result["stdout"].as_str().unwrap();
        assert!(stdout.contains("MY_SANDBOX_VAR=test_value"));
        assert!(stdout.contains("PATH="));
    }

    // ── E2S2-T11: Receipt generation ──

    #[tokio::test]
    async fn sandbox_exec_receipt() {
        use crate::receipt_gen::invoke_with_receipt;

        let ctx = test_ctx();
        let connector = test_connector();
        let params = json!({ "command": "echo", "args": ["receipt_test"] });

        let (result, receipt) =
            tokio::task::spawn_blocking(move || invoke_with_receipt(&connector, &ctx, &params))
                .await
                .expect("spawn_blocking join failed");

        assert!(result.is_ok());
        assert_eq!(receipt.connector, "sandbox.exec");
        assert_eq!(receipt.status, worldinterface_core::receipt::ReceiptStatus::Success);
        assert!(receipt.output_hash.is_some());
        assert!(!receipt.input_hash.is_empty());
    }

    // ── E2S2-T12: Descriptor ──

    #[test]
    fn sandbox_exec_descriptor() {
        let desc = SandboxExecConnector::new().describe();
        assert_eq!(desc.name, "sandbox.exec");
        assert_eq!(desc.category, ConnectorCategory::Sandbox);
        assert!(!desc.idempotent);
        assert!(desc.side_effects);
        assert!(!desc.is_read_only);
        assert!(desc.is_mutating);
        assert!(!desc.is_concurrency_safe);
        assert!(!desc.requires_read_before_write);
        assert!(desc.input_schema.is_some());
        assert!(desc.output_schema.is_some());
    }

    // ── E2S2-T13: Simple command ──

    #[tokio::test]
    async fn sandbox_exec_simple_command() {
        let result = invoke_on_blocking_thread(json!({
            "command": "echo",
            "args": ["-n", "hello"]
        }))
        .await
        .unwrap();

        assert_eq!(result["stdout"], "hello");
        assert_eq!(result["exit_code"], 0);
    }

    // ── E2S2-T14: Non-zero exit code ──

    #[tokio::test]
    async fn sandbox_exec_non_zero_exit_code() {
        let result = invoke_on_blocking_thread(json!({
            "command": "sh",
            "args": ["-c", "exit 42"]
        }))
        .await
        .unwrap();

        assert_eq!(result["exit_code"], 42);
    }

    // ── E2S2-T15: Cancellation ──

    #[tokio::test]
    async fn sandbox_exec_cancellation() {
        let ctx = test_ctx();
        let cancel = ctx.cancellation.clone();

        // Cancel after a short delay
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            cancel.cancel();
        });

        let start = std::time::Instant::now();
        let result = invoke_on_blocking_thread_with_ctx(
            ctx,
            json!({
                "command": "sleep",
                "args": ["60"]
            }),
        )
        .await;
        let elapsed = start.elapsed();

        assert!(
            elapsed < std::time::Duration::from_secs(15),
            "Cancellation should prevent 60s wait, elapsed: {elapsed:?}"
        );

        match result {
            Err(ConnectorError::Cancelled) => {}
            Ok(v) if v["timed_out"] == true => {}
            other => panic!("unexpected result: {other:?}"),
        }
    }

    // ── E2S2-T16: Missing command ──

    #[tokio::test]
    async fn sandbox_exec_missing_command() {
        let result = invoke_on_blocking_thread(json!({
            "args": ["hello"]
        }))
        .await;

        assert!(matches!(result, Err(ConnectorError::InvalidParams(_))));
    }

    // ── E2S2-T17: Normal args pass validation ──

    #[tokio::test]
    async fn sandbox_exec_args_without_data_pass() {
        let result = invoke_on_blocking_thread(json!({
            "command": "echo",
            "args": ["/workspace/file.py", "--flag", "value"]
        }))
        .await
        .unwrap();

        assert_eq!(result["exit_code"], 0);
    }
}

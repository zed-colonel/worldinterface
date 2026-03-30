//! The `shell.exec` connector — executes commands in the host container environment.
//!
//! Security model:
//! - The embedding application is responsible for gating execution policy
//! - Environment filtered: child starts with empty env, only explicit allowlist inherited
//! - Output bounded: stdout/stderr capped at `max_output_bytes`
//! - Timeout enforced: SIGTERM then SIGKILL after 5s grace period
//! - No shell expansion by default: commands exec'd directly, `shell: true` opts in

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::AsyncRead;
use worldinterface_core::descriptor::{ConnectorCategory, Descriptor};

use crate::context::InvocationContext;
use crate::error::ConnectorError;
use crate::traits::Connector;

/// Executes shell commands with timeout, output bounding, and environment filtering.
pub struct ShellExecConnector {
    /// Default timeout if not specified per-invocation.
    default_timeout: Duration,
    /// Maximum allowed timeout per-invocation.
    max_timeout: Duration,
    /// Maximum bytes captured per stream (stdout, stderr).
    max_output_bytes: usize,
    /// Base environment variables inherited by child processes.
    base_env_keys: Vec<String>,
}

impl ShellExecConnector {
    pub fn new() -> Self {
        Self {
            default_timeout: Duration::from_secs(30),
            max_timeout: Duration::from_secs(300),
            max_output_bytes: 1_048_576, // 1MB
            base_env_keys: vec![
                "PATH".into(),
                "HOME".into(),
                "USER".into(),
                "LANG".into(),
                "TERM".into(),
            ],
        }
    }

    /// Create a connector with custom limits.
    ///
    /// Used by `SandboxExecConnector` to create a restricted inner instance.
    pub fn with_limits(
        default_timeout: Duration,
        max_timeout: Duration,
        max_output_bytes: usize,
        base_env_keys: Vec<String>,
    ) -> Self {
        Self { default_timeout, max_timeout, max_output_bytes, base_env_keys }
    }
}

impl Default for ShellExecConnector {
    fn default() -> Self {
        Self::new()
    }
}

impl ShellExecConnector {
    async fn invoke_async(
        &self,
        ctx: &InvocationContext,
        params: &Value,
    ) -> Result<Value, ConnectorError> {
        if ctx.cancellation.is_cancelled() {
            return Err(ConnectorError::Cancelled);
        }

        // ── Parse params ──

        let command = params.get("command").and_then(Value::as_str).ok_or_else(|| {
            ConnectorError::InvalidParams("missing or invalid 'command' (expected string)".into())
        })?;

        let args: Vec<String> = params
            .get("args")
            .and_then(Value::as_array)
            .map(|arr| arr.iter().filter_map(Value::as_str).map(String::from).collect())
            .unwrap_or_default();

        let working_dir = params.get("working_dir").and_then(Value::as_str);

        let env_vars: Vec<(String, String)> = params
            .get("env")
            .and_then(Value::as_object)
            .map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();

        let timeout_ms = params.get("timeout_ms").and_then(Value::as_u64);
        let shell_mode = params.get("shell").and_then(Value::as_bool).unwrap_or(false);

        // ── Compute timeout ──

        let timeout = match timeout_ms {
            Some(ms) => {
                let requested = Duration::from_millis(ms);
                std::cmp::min(requested, self.max_timeout)
            }
            None => self.default_timeout,
        };

        // ── Build command ──

        let mut cmd = if shell_mode {
            let full_cmd = if args.is_empty() {
                command.to_string()
            } else {
                format!("{} {}", command, args.join(" "))
            };
            let mut c = tokio::process::Command::new("/bin/sh");
            c.args(["-c", &full_cmd]);
            c
        } else {
            let mut c = tokio::process::Command::new(command);
            c.args(&args);
            c
        };

        // Environment: start empty, add base vars, merge per-invocation vars
        cmd.env_clear();
        for key in &self.base_env_keys {
            if let Ok(val) = std::env::var(key) {
                cmd.env(key, val);
            }
        }
        for (key, val) in &env_vars {
            cmd.env(key, val);
        }

        if let Some(dir) = working_dir {
            cmd.current_dir(dir);
        }

        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());

        // ── Spawn ──

        let mut child = cmd.spawn().map_err(|e| {
            ConnectorError::Terminal(format!("failed to spawn command '{command}': {e}"))
        })?;

        // ── Read stdout/stderr concurrently with timeout + cancellation ──
        //
        // I/O buffers are shared via Arc<Mutex<>> so the timeout path can
        // retrieve partial output already buffered before the process was killed.

        let stdout_buf: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let stderr_buf: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let stdout_truncated = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stderr_truncated = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let stdout_handle = child.stdout.take();
        let stderr_handle = child.stderr.take();
        let max_bytes = self.max_output_bytes;
        let cancel = ctx.cancellation.clone();

        // Spawn I/O readers that write into shared buffers
        let stdout_task = {
            let buf = Arc::clone(&stdout_buf);
            let trunc = Arc::clone(&stdout_truncated);
            tokio::spawn(async move {
                if let Some(reader) = stdout_handle {
                    read_bounded(reader, max_bytes, &buf, &trunc).await
                } else {
                    Ok(())
                }
            })
        };
        let stderr_task = {
            let buf = Arc::clone(&stderr_buf);
            let trunc = Arc::clone(&stderr_truncated);
            tokio::spawn(async move {
                if let Some(reader) = stderr_handle {
                    read_bounded(reader, max_bytes, &buf, &trunc).await
                } else {
                    Ok(())
                }
            })
        };

        let cancel_future = async {
            loop {
                tokio::time::sleep(Duration::from_millis(50)).await;
                if cancel.is_cancelled() {
                    return;
                }
            }
        };

        let process_future = async {
            let (stdout_res, stderr_res) = tokio::try_join!(stdout_task, stderr_task)
                .map_err(|e| ConnectorError::Retryable(format!("I/O task join error: {e}")))?;

            stdout_res
                .map_err(|e| ConnectorError::Retryable(format!("failed to read stdout: {e}")))?;
            stderr_res
                .map_err(|e| ConnectorError::Retryable(format!("failed to read stderr: {e}")))?;

            let status = child.wait().await.map_err(|e| {
                ConnectorError::Retryable(format!("failed to wait for process: {e}"))
            })?;

            Ok::<_, ConnectorError>(status)
        };

        // Race: (process + timeout) vs cancellation
        tokio::select! {
            result = tokio::time::timeout(timeout, process_future) => {
                match result {
                    Ok(Ok(status)) => {
                        if ctx.cancellation.is_cancelled() {
                            return Err(ConnectorError::Cancelled);
                        }
                        let exit_code = status.code();
                        let truncated = stdout_truncated.load(std::sync::atomic::Ordering::Relaxed)
                            || stderr_truncated.load(std::sync::atomic::Ordering::Relaxed);
                        Ok(json!({
                            "stdout": extract_buf(&stdout_buf),
                            "stderr": extract_buf(&stderr_buf),
                            "exit_code": exit_code,
                            "timed_out": false,
                            "truncated": truncated,
                        }))
                    }
                    Ok(Err(e)) => Err(e),
                    Err(_timeout) => {
                        // Kill with SIGTERM then SIGKILL after grace period
                        kill_with_grace(&mut child, Duration::from_secs(5)).await;
                        let truncated = stdout_truncated.load(std::sync::atomic::Ordering::Relaxed)
                            || stderr_truncated.load(std::sync::atomic::Ordering::Relaxed);
                        Ok(json!({
                            "stdout": extract_buf(&stdout_buf),
                            "stderr": extract_buf(&stderr_buf),
                            "exit_code": null,
                            "timed_out": true,
                            "truncated": truncated,
                        }))
                    }
                }
            }
            _ = cancel_future => {
                kill_with_grace(&mut child, Duration::from_secs(1)).await;
                Err(ConnectorError::Cancelled)
            }
        }
    }
}

/// Extract buffered content as a String from the shared buffer.
fn extract_buf(buf: &Arc<Mutex<Vec<u8>>>) -> String {
    let locked = buf.lock().unwrap_or_else(|e| e.into_inner());
    String::from_utf8_lossy(&locked).into_owned()
}

/// Read from an async reader into a shared bounded buffer.
///
/// Generic over the reader type — works with both `ChildStdout` and `ChildStderr`.
async fn read_bounded<R: AsyncRead + Unpin>(
    mut reader: R,
    max_bytes: usize,
    buf: &Arc<Mutex<Vec<u8>>>,
    truncated: &std::sync::atomic::AtomicBool,
) -> Result<(), std::io::Error> {
    use tokio::io::AsyncReadExt;

    let mut total = 0;
    let mut chunk = [0u8; 8192];

    loop {
        let n = reader.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        let remaining = max_bytes.saturating_sub(total);
        if remaining == 0 {
            truncated.store(true, std::sync::atomic::Ordering::Relaxed);
            break;
        }
        let take = std::cmp::min(n, remaining);
        {
            let mut locked = buf.lock().unwrap_or_else(|e| e.into_inner());
            locked.extend_from_slice(&chunk[..take]);
        }
        total += take;
        if take < n {
            truncated.store(true, std::sync::atomic::Ordering::Relaxed);
            break;
        }
    }

    Ok(())
}

/// Kill a child process with SIGTERM first, then SIGKILL after grace period.
#[cfg(unix)]
async fn kill_with_grace(child: &mut tokio::process::Child, grace: Duration) {
    if let Some(pid) = child.id() {
        let _ = nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(pid as i32),
            nix::sys::signal::Signal::SIGTERM,
        );
    }
    match tokio::time::timeout(grace, child.wait()).await {
        Ok(_) => {} // Process exited gracefully after SIGTERM
        Err(_) => {
            // Grace period expired, force kill
            let _ = child.kill().await;
        }
    }
}

#[cfg(not(unix))]
async fn kill_with_grace(child: &mut tokio::process::Child, _grace: Duration) {
    let _ = child.kill().await;
}

impl Connector for ShellExecConnector {
    fn describe(&self) -> Descriptor {
        Descriptor {
            name: "shell.exec".into(),
            display_name: "Shell Execute".into(),
            description: "Executes a command in the host container environment. By default, \
                'command' is a binary path (e.g. \"echo\") with separate 'args'. \
                Set shell:true to pass a full shell string to /bin/sh -c."
                .into(),
            category: ConnectorCategory::Shell,
            input_schema: Some(json!({
                "type": "object",
                "required": ["command"],
                "properties": {
                    "command": {
                        "type": "string",
                        "description": "Binary path (default) or shell string (if shell:true). Example: \"ls\" or \"echo hello && date\""
                    },
                    "args": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Arguments passed to the binary (ignored when shell:true)"
                    },
                    "working_dir": { "type": "string" },
                    "env": { "type": "object" },
                    "timeout_ms": { "type": "integer", "minimum": 0 },
                    "shell": {
                        "type": "boolean",
                        "description": "If true, command is passed to /bin/sh -c (enables pipes, expansion). Default: false"
                    }
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
        }
    }

    fn invoke(&self, ctx: &InvocationContext, params: &Value) -> Result<Value, ConnectorError> {
        tokio::runtime::Handle::current().block_on(self.invoke_async(ctx, params))
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

    async fn invoke_on_blocking_thread(params: Value) -> Result<Value, ConnectorError> {
        let ctx = test_ctx();
        tokio::task::spawn_blocking(move || ShellExecConnector::new().invoke(&ctx, &params))
            .await
            .expect("spawn_blocking join failed")
    }

    async fn invoke_on_blocking_thread_with_connector(
        connector: ShellExecConnector,
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
        tokio::task::spawn_blocking(move || ShellExecConnector::new().invoke(&ctx, &params))
            .await
            .expect("spawn_blocking join failed")
    }

    // ── E2S1-T1: Simple command ──

    #[tokio::test]
    async fn shell_exec_simple_command() {
        let result = invoke_on_blocking_thread(json!({
            "command": "echo",
            "args": ["hello"]
        }))
        .await
        .unwrap();

        assert_eq!(result["stdout"], "hello\n");
        assert_eq!(result["exit_code"], 0);
        assert_eq!(result["timed_out"], false);
        assert_eq!(result["truncated"], false);
    }

    // ── E2S1-T2: Command with args ──

    #[tokio::test]
    async fn shell_exec_command_with_args() {
        let result = invoke_on_blocking_thread(json!({
            "command": "echo",
            "args": ["-n", "foo bar"]
        }))
        .await
        .unwrap();

        assert_eq!(result["stdout"], "foo bar");
    }

    // ── E2S1-T3: Nonexistent command ──

    #[tokio::test]
    async fn shell_exec_nonexistent_command() {
        let result = invoke_on_blocking_thread(json!({
            "command": "nonexistent_command_xyz_12345"
        }))
        .await;

        assert!(matches!(result, Err(ConnectorError::Terminal(_))));
    }

    // ── E2S1-T4: Non-zero exit code ──

    #[tokio::test]
    async fn shell_exec_non_zero_exit_code() {
        let result = invoke_on_blocking_thread(json!({
            "command": "sh",
            "args": ["-c", "exit 42"]
        }))
        .await
        .unwrap();

        assert_eq!(result["exit_code"], 42);
        assert_eq!(result["timed_out"], false);
    }

    // ── E2S1-T5: Stderr captured ──

    #[tokio::test]
    async fn shell_exec_stderr_captured() {
        let result = invoke_on_blocking_thread(json!({
            "command": "sh",
            "args": ["-c", "echo error_msg >&2"]
        }))
        .await
        .unwrap();

        assert!(result["stderr"].as_str().unwrap().contains("error_msg"));
    }

    // ── E2S1-T6: Timeout kills process ──

    #[tokio::test]
    async fn shell_exec_timeout_kills_process() {
        let result = invoke_on_blocking_thread(json!({
            "command": "sleep",
            "args": ["60"],
            "timeout_ms": 200
        }))
        .await
        .unwrap();

        assert_eq!(result["timed_out"], true);
        assert!(result["exit_code"].is_null());
    }

    // ── E2S1-T7: Output truncation ──

    #[tokio::test]
    async fn shell_exec_output_truncation() {
        let connector = ShellExecConnector::with_limits(
            Duration::from_secs(30),
            Duration::from_secs(300),
            32, // 32 bytes max output
            vec!["PATH".into(), "HOME".into(), "USER".into(), "LANG".into(), "TERM".into()],
        );
        // Generate output larger than 32 bytes
        let result = invoke_on_blocking_thread_with_connector(
            connector,
            json!({
                "command": "sh",
                "args": ["-c", "dd if=/dev/zero bs=1 count=100 2>/dev/null | tr '\\0' 'A'"]
            }),
        )
        .await
        .unwrap();

        assert_eq!(result["truncated"], true);
        assert!(result["stdout"].as_str().unwrap().len() <= 32);
    }

    // ── E2S1-T8: Working directory ──

    #[tokio::test]
    async fn shell_exec_working_dir() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path().to_str().unwrap().to_string();
        let result = invoke_on_blocking_thread(json!({
            "command": "pwd",
            "working_dir": dir_path
        }))
        .await
        .unwrap();

        let stdout = result["stdout"].as_str().unwrap().trim();
        // Resolve symlinks for macOS /private/var vs /var
        let expected = std::fs::canonicalize(&dir_path).unwrap();
        let actual = std::fs::canonicalize(stdout).unwrap();
        assert_eq!(actual, expected);
    }

    // ── E2S1-T9: Env vars passed ──

    #[tokio::test]
    async fn shell_exec_env_vars_passed() {
        let result = invoke_on_blocking_thread(json!({
            "command": "sh",
            "args": ["-c", "echo $MY_TEST_VAR"],
            "env": { "MY_TEST_VAR": "hello_world" }
        }))
        .await
        .unwrap();

        assert!(result["stdout"].as_str().unwrap().contains("hello_world"));
    }

    // ── E2S1-T10: Base env filtered ──

    #[tokio::test]
    async fn shell_exec_base_env_filtered() {
        // Set a decoy env var in this process
        std::env::set_var("SHELL_EXEC_TEST_SECRET", "s3cr3t");

        let result = invoke_on_blocking_thread(json!({
            "command": "env"
        }))
        .await
        .unwrap();

        let stdout = result["stdout"].as_str().unwrap();
        assert!(
            !stdout.contains("SHELL_EXEC_TEST_SECRET"),
            "Child should NOT see non-base env vars"
        );

        // Cleanup
        std::env::remove_var("SHELL_EXEC_TEST_SECRET");
    }

    // ── E2S1-T11: Shell mode ──

    #[tokio::test]
    async fn shell_exec_shell_mode() {
        let result = invoke_on_blocking_thread(json!({
            "command": "echo",
            "args": ["$HOME"],
            "shell": true
        }))
        .await
        .unwrap();

        let stdout = result["stdout"].as_str().unwrap().trim();
        assert!(
            !stdout.is_empty() && stdout != "$HOME",
            "Shell mode should expand $HOME, got: {stdout}"
        );
    }

    // ── E2S1-T12: Cancellation ──

    #[tokio::test]
    async fn shell_exec_cancellation() {
        let ctx = test_ctx();
        let token = ctx.cancellation.clone();

        // Cancel after 50ms from a separate task
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            token.cancel();
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

        // The cancellation should have been detected on the pre-check or
        // the process should time out quickly. In practice, the pre-cancel
        // check may not catch it since the task starts before cancel fires.
        // We primarily verify it completes quickly (not 60s).
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_secs(10),
            "Cancellation should prevent 60s wait, elapsed: {elapsed:?}"
        );

        // The result might be Cancelled (if caught early) or a timeout output
        match result {
            Err(ConnectorError::Cancelled) => {}  // ideal
            Ok(v) if v["timed_out"] == true => {} // also acceptable
            other => panic!("unexpected result: {other:?}"),
        }
    }

    // ── E2S1-T13: Receipt generation ──

    #[tokio::test]
    async fn shell_exec_receipt() {
        use crate::receipt_gen::invoke_with_receipt;

        let ctx = test_ctx();
        let connector = ShellExecConnector::new();
        let params = json!({ "command": "echo", "args": ["receipt_test"] });

        let (result, receipt) =
            tokio::task::spawn_blocking(move || invoke_with_receipt(&connector, &ctx, &params))
                .await
                .expect("spawn_blocking join failed");

        assert!(result.is_ok());
        assert_eq!(receipt.connector, "shell.exec");
        assert_eq!(receipt.status, worldinterface_core::receipt::ReceiptStatus::Success);
        assert!(receipt.output_hash.is_some());
        assert!(!receipt.input_hash.is_empty());
    }

    // ── E2S1-T14: Descriptor ──

    #[test]
    fn shell_exec_descriptor() {
        let desc = ShellExecConnector::new().describe();
        assert_eq!(desc.name, "shell.exec");
        assert_eq!(desc.category, ConnectorCategory::Shell);
        assert!(!desc.idempotent);
        assert!(desc.side_effects);
        assert!(desc.input_schema.is_some());
        assert!(desc.output_schema.is_some());
    }

    // ── E2S1-T15: Missing command ──

    #[tokio::test]
    async fn shell_exec_missing_command() {
        let result = invoke_on_blocking_thread(json!({
            "args": ["hello"]
        }))
        .await;

        assert!(matches!(result, Err(ConnectorError::InvalidParams(_))));
    }

    // ── E2S1-T16: Timeout clamped ──

    #[tokio::test]
    async fn shell_exec_timeout_clamped() {
        // max_timeout is 300s. Request 999999ms. Should be clamped, not rejected.
        // Just verify it doesn't error (we can't easily test the clamped value
        // without observing behavior, but a successful run proves no rejection).
        let result = invoke_on_blocking_thread(json!({
            "command": "echo",
            "args": ["clamped"],
            "timeout_ms": 999999
        }))
        .await
        .unwrap();

        assert_eq!(result["stdout"], "clamped\n");
    }

    // ── E2S1-T17: Empty args ──

    #[tokio::test]
    async fn shell_exec_empty_args() {
        let result = invoke_on_blocking_thread(json!({
            "command": "echo",
            "args": []
        }))
        .await
        .unwrap();

        assert_eq!(result["exit_code"], 0);
    }

    // ── E2S1-T18: Both stdout and stderr ──

    #[tokio::test]
    async fn shell_exec_stdout_and_stderr() {
        let result = invoke_on_blocking_thread(json!({
            "command": "sh",
            "args": ["-c", "echo out_msg; echo err_msg >&2"]
        }))
        .await
        .unwrap();

        assert!(result["stdout"].as_str().unwrap().contains("out_msg"));
        assert!(result["stderr"].as_str().unwrap().contains("err_msg"));
    }

    // ── E2S2-T18: with_limits() constructor ──

    #[test]
    fn shell_exec_with_limits_constructor() {
        let connector = ShellExecConnector::with_limits(
            Duration::from_secs(10),
            Duration::from_secs(60),
            262_144,
            vec!["PATH".into(), "HOME".into(), "LANG".into()],
        );
        // Verify via describe() that the connector is functional
        let desc = connector.describe();
        assert_eq!(desc.name, "shell.exec");
        // The limits are internal, but we can verify the connector was constructed
        // by checking it's a valid connector with different-from-default limits
        assert!(!desc.idempotent);
        assert!(desc.side_effects);
    }
}

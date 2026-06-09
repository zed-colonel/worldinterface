//! `code.test` connector.
//!
//! Runs a project test command with bounded output and timeout handling. This
//! intentionally exposes a coding-native verification primitive instead of
//! requiring models to route tests through unrestricted shell execution.

use std::io::Read;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use worldinterface_core::descriptor::{ConnectorCategory, Descriptor};

use crate::context::InvocationContext;
use crate::error::ConnectorError;
use crate::traits::Connector;

const DEFAULT_TIMEOUT_MS: u64 = 300_000;
const MAX_TIMEOUT_MS: u64 = 900_000;
const MAX_OUTPUT_BYTES: usize = 512 * 1024;

pub struct CodeTestConnector;

impl Connector for CodeTestConnector {
    fn describe(&self) -> Descriptor {
        Descriptor {
            name: "code.test".into(),
            display_name: "Code Test".into(),
            description: "Runs a project test command from a workspace directory. If no command \
                          is supplied, infers a conventional test command from project files \
                          such as Cargo.toml, package.json, pyproject.toml, or go.mod."
                .into(),
            category: ConnectorCategory::Code,
            input_schema: Some(json!({
                "type": "object",
                "required": ["path"],
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Workspace or package directory in which to run tests"
                    },
                    "command": {
                        "type": "string",
                        "description": "Optional test binary. Use 'auto' or omit to infer. Supported examples: cargo, npm, pnpm, yarn, python, pytest, go, mvn, gradle, ./gradlew"
                    },
                    "args": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Arguments for the test binary. Ignored when command is omitted or 'auto'."
                    },
                    "timeout_ms": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "Timeout in milliseconds. Default 300000, capped at 900000."
                    },
                    "env": {
                        "type": "object",
                        "description": "Optional string environment variables to add to the test process."
                    }
                }
            })),
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "command": { "type": "string" },
                    "args": { "type": "array", "items": { "type": "string" } },
                    "command_line": { "type": "string" },
                    "exit_code": { "type": ["integer", "null"] },
                    "passed": { "type": "boolean" },
                    "timed_out": { "type": "boolean" },
                    "stdout": { "type": "string" },
                    "stderr": { "type": "string" },
                    "stdout_truncated": { "type": "boolean" },
                    "stderr_truncated": { "type": "boolean" }
                }
            })),
            idempotent: false,
            side_effects: true,
            is_read_only: false,
            is_mutating: false,
            is_concurrency_safe: false,
            requires_read_before_write: false,
        }
    }

    fn invoke(&self, ctx: &InvocationContext, params: &Value) -> Result<Value, ConnectorError> {
        if ctx.cancellation.is_cancelled() {
            return Err(ConnectorError::Cancelled);
        }

        let path_str = params
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| ConnectorError::InvalidParams("missing 'path'".into()))?;
        let path = Path::new(path_str);
        if !path.exists() {
            return Err(ConnectorError::terminal(format!(
                "workspace path does not exist: {path_str}"
            )));
        }
        if !path.is_dir() {
            return Err(ConnectorError::terminal(format!(
                "workspace path is not a directory: {path_str}"
            )));
        }

        let (command, args) = resolve_test_command(path, params)?;
        validate_test_command(path, &command)?;
        let timeout_ms = params
            .get("timeout_ms")
            .and_then(Value::as_u64)
            .unwrap_or(DEFAULT_TIMEOUT_MS)
            .clamp(1, MAX_TIMEOUT_MS);
        let env = parse_env(params.get("env"))?;

        let mut cmd = Command::new(&command);
        cmd.args(&args).current_dir(path).env_clear().stdout(Stdio::piped()).stderr(Stdio::piped());

        for key in [
            "PATH",
            "HOME",
            "USER",
            "LANG",
            "TERM",
            "CARGO_HOME",
            "CARGO_TARGET_DIR",
            "RUSTFLAGS",
            "RUSTUP_TOOLCHAIN",
            "RUSTC_WRAPPER",
            "RUSTC_WORKSPACE_WRAPPER",
        ] {
            if let Ok(value) = std::env::var(key) {
                cmd.env(key, value);
            }
        }
        for (key, value) in env {
            cmd.env(key, value);
        }

        let mut child = cmd.spawn().map_err(|err| {
            ConnectorError::terminal(format!("failed to spawn test command '{command}': {err}"))
        })?;

        let stdout = child.stdout.take().ok_or_else(|| {
            ConnectorError::Retryable("failed to capture test command stdout".into())
        })?;
        let stderr = child.stderr.take().ok_or_else(|| {
            ConnectorError::Retryable("failed to capture test command stderr".into())
        })?;

        let stdout_handle = thread::spawn(move || read_bounded(stdout, MAX_OUTPUT_BYTES));
        let stderr_handle = thread::spawn(move || read_bounded(stderr, MAX_OUTPUT_BYTES));

        let timeout = Duration::from_millis(timeout_ms);
        let start = Instant::now();
        let mut timed_out = false;
        let status = loop {
            if ctx.cancellation.is_cancelled() {
                let _ = child.kill();
                let _ = child.wait();
                return Err(ConnectorError::Cancelled);
            }

            match child.try_wait() {
                Ok(Some(status)) => break Some(status),
                Ok(None) if start.elapsed() >= timeout => {
                    timed_out = true;
                    let _ = child.kill();
                    let _ = child.wait();
                    break None;
                }
                Ok(None) => thread::sleep(Duration::from_millis(100)),
                Err(err) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(ConnectorError::Retryable(format!(
                        "test command wait failed: {err}"
                    )));
                }
            }
        };

        let (stdout, stdout_truncated) = stdout_handle
            .join()
            .map_err(|_| ConnectorError::Retryable("stdout reader thread panicked".into()))?;
        let (stderr, stderr_truncated) = stderr_handle
            .join()
            .map_err(|_| ConnectorError::Retryable("stderr reader thread panicked".into()))?;
        let exit_code = status.and_then(|status| status.code());
        let passed = !timed_out && exit_code == Some(0);

        Ok(json!({
            "path": path.display().to_string(),
            "command": command,
            "args": args,
            "command_line": command_line(&command, &args),
            "exit_code": exit_code,
            "passed": passed,
            "timed_out": timed_out,
            "stdout": stdout,
            "stderr": stderr,
            "stdout_truncated": stdout_truncated,
            "stderr_truncated": stderr_truncated,
        }))
    }
}

fn resolve_test_command(
    path: &Path,
    params: &Value,
) -> Result<(String, Vec<String>), ConnectorError> {
    let command = params.get("command").and_then(Value::as_str).unwrap_or("auto").trim();
    if command.is_empty() || command == "auto" {
        return infer_test_command(path);
    }

    let args = params
        .get("args")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .map(|value| {
                    value.as_str().map(str::to_string).ok_or_else(|| {
                        ConnectorError::InvalidParams("args must contain only strings".into())
                    })
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();

    Ok((command.to_string(), args))
}

fn infer_test_command(path: &Path) -> Result<(String, Vec<String>), ConnectorError> {
    if path.join("Cargo.toml").exists() {
        return Ok(("cargo".into(), vec!["test".into(), "--no-fail-fast".into()]));
    }
    if path.join("package.json").exists() {
        return Ok(("npm".into(), vec!["test".into()]));
    }
    if path.join("pyproject.toml").exists()
        || path.join("setup.py").exists()
        || path.join("setup.cfg").exists()
        || path.join("requirements.txt").exists()
    {
        return Ok(("python".into(), vec!["-m".into(), "pytest".into()]));
    }
    if path.join("go.mod").exists() {
        return Ok(("go".into(), vec!["test".into(), "./...".into()]));
    }
    if path.join("pom.xml").exists() {
        return Ok(("mvn".into(), vec!["test".into()]));
    }
    if path.join("gradlew").exists() {
        return Ok(("./gradlew".into(), vec!["test".into()]));
    }
    if path.join("build.gradle").exists() || path.join("build.gradle.kts").exists() {
        return Ok(("gradle".into(), vec!["test".into()]));
    }

    Err(ConnectorError::terminal(format!("could not infer a test command for {}", path.display())))
}

fn validate_test_command(path: &Path, command: &str) -> Result<(), ConnectorError> {
    let allowed = matches!(
        command,
        "cargo"
            | "npm"
            | "pnpm"
            | "yarn"
            | "bun"
            | "deno"
            | "python"
            | "python3"
            | "pytest"
            | "py.test"
            | "go"
            | "mvn"
            | "gradle"
            | "dotnet"
            | "mix"
            | "rspec"
            | "jest"
            | "vitest"
            | "phpunit"
    ) || command == "./gradlew";

    if !allowed {
        return Err(ConnectorError::InvalidParams(format!("unsupported test command '{command}'")));
    }
    if command == "./gradlew" && !path.join("gradlew").exists() {
        return Err(ConnectorError::terminal(format!(
            "./gradlew does not exist under {}",
            path.display()
        )));
    }
    Ok(())
}

fn parse_env(value: Option<&Value>) -> Result<Vec<(String, String)>, ConnectorError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let object = value
        .as_object()
        .ok_or_else(|| ConnectorError::InvalidParams("env must be an object".into()))?;
    object
        .iter()
        .map(|(key, value)| {
            value
                .as_str()
                .map(|value| (key.clone(), value.to_string()))
                .ok_or_else(|| ConnectorError::InvalidParams("env values must be strings".into()))
        })
        .collect()
}

fn read_bounded<R: Read>(mut reader: R, max_bytes: usize) -> (String, bool) {
    let mut bytes = Vec::new();
    let mut truncated = false;
    let mut chunk = [0_u8; 8192];
    loop {
        let read = match reader.read(&mut chunk) {
            Ok(0) => break,
            Ok(read) => read,
            Err(_) => break,
        };
        let remaining = max_bytes.saturating_sub(bytes.len());
        if remaining == 0 {
            truncated = true;
            continue;
        }
        let take = read.min(remaining);
        bytes.extend_from_slice(&chunk[..take]);
        if take < read {
            truncated = true;
        }
    }

    (String::from_utf8_lossy(&bytes).into_owned(), truncated)
}

fn command_line(command: &str, args: &[String]) -> String {
    std::iter::once(command.to_string()).chain(args.iter().cloned()).collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use uuid::Uuid;
    use worldinterface_core::id::{FlowRunId, NodeId, StepRunId};

    use super::*;
    use crate::context::CancellationToken;

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

    #[test]
    fn descriptor_is_code_verification_tool() {
        let desc = CodeTestConnector.describe();
        assert_eq!(desc.name, "code.test");
        assert_eq!(desc.category, ConnectorCategory::Code);
        assert!(!desc.is_read_only);
        assert!(!desc.is_mutating);
    }

    #[test]
    fn infers_cargo_test_command() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("Cargo.toml"), "[package]\nname=\"x\"\n").unwrap();

        let (command, args) = infer_test_command(dir.path()).unwrap();
        assert_eq!(command, "cargo");
        assert_eq!(args, vec!["test", "--no-fail-fast"]);
    }

    #[test]
    fn runs_explicit_test_command_and_reports_passed() {
        let dir = tempfile::tempdir().unwrap();
        let result = CodeTestConnector
            .invoke(
                &test_ctx(),
                &json!({
                    "path": dir.path(),
                    "command": "python",
                    "args": ["-c", "print('ok')"],
                    "timeout_ms": 10_000
                }),
            )
            .unwrap();

        assert_eq!(result["passed"], true);
        assert_eq!(result["exit_code"], 0);
        assert!(result["stdout"].as_str().unwrap().contains("ok"));
    }

    #[test]
    fn non_zero_exit_is_successful_invocation_with_failed_tests() {
        let dir = tempfile::tempdir().unwrap();
        let result = CodeTestConnector
            .invoke(
                &test_ctx(),
                &json!({
                    "path": dir.path(),
                    "command": "python",
                    "args": ["-c", "import sys; sys.exit(7)"],
                    "timeout_ms": 10_000
                }),
            )
            .unwrap();

        assert_eq!(result["passed"], false);
        assert_eq!(result["exit_code"], 7);
    }
}

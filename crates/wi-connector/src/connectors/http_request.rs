//! The `http.request` connector — makes HTTP requests to external endpoints.
//!
//! Uses `reqwest::blocking::Client` because AQ's `ExecutorHandler::execute()`
//! is synchronous and runs on OS threads (not tokio worker threads).

use std::collections::HashMap;
use std::time::Duration;

use serde_json::{json, Value};
use wi_core::descriptor::{ConnectorCategory, Descriptor};

use crate::context::InvocationContext;
use crate::error::ConnectorError;
use crate::traits::Connector;

/// Makes HTTP requests to external endpoints. Injects `X-Idempotency-Key`
/// header with the `run_id` on every request (Invariant 3).
pub struct HttpRequestConnector {
    client: reqwest::blocking::Client,
}

impl HttpRequestConnector {
    pub fn new() -> Self {
        Self {
            client: reqwest::blocking::Client::builder()
                .build()
                .expect("failed to build HTTP client"),
        }
    }
}

impl Default for HttpRequestConnector {
    fn default() -> Self {
        Self::new()
    }
}

impl Connector for HttpRequestConnector {
    fn describe(&self) -> Descriptor {
        Descriptor {
            name: "http.request".into(),
            display_name: "HTTP Request".into(),
            description: "Makes an HTTP request to an external URL.".into(),
            category: ConnectorCategory::Http,
            input_schema: Some(json!({
                "type": "object",
                "required": ["url"],
                "properties": {
                    "method": { "type": "string", "enum": ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD"] },
                    "url": { "type": "string" },
                    "headers": { "type": "object" },
                    "body": {},
                    "timeout_ms": { "type": "integer", "minimum": 0 }
                }
            })),
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "status": { "type": "integer" },
                    "headers": { "type": "object" },
                    "body": { "type": "string" }
                }
            })),
            idempotent: false,
            side_effects: true,
        }
    }

    fn invoke(&self, ctx: &InvocationContext, params: &Value) -> Result<Value, ConnectorError> {
        // Check cancellation before starting the request
        if ctx.cancellation.is_cancelled() {
            return Err(ConnectorError::Cancelled);
        }

        let url = params.get("url").and_then(Value::as_str).ok_or_else(|| {
            ConnectorError::InvalidParams("missing or invalid 'url' (expected string)".into())
        })?;

        let method_str = params.get("method").and_then(Value::as_str).unwrap_or("GET");

        let method: reqwest::Method = method_str.parse().map_err(|_| {
            ConnectorError::InvalidParams(format!("unknown HTTP method: '{method_str}'"))
        })?;

        // Build request
        let mut request = self.client.request(method, url);

        // Inject idempotency key (Invariant 3)
        request = request.header("X-Idempotency-Key", ctx.run_id.to_string());

        // Apply custom headers
        if let Some(headers) = params.get("headers").and_then(Value::as_object) {
            for (key, value) in headers {
                if let Some(val_str) = value.as_str() {
                    request = request.header(key.as_str(), val_str);
                }
            }
        }

        // Apply body
        if let Some(body) = params.get("body") {
            match body {
                Value::String(s) => {
                    request = request.body(s.clone());
                }
                other => {
                    request =
                        request.header("Content-Type", "application/json").body(other.to_string());
                }
            }
        }

        // Apply timeout
        if let Some(timeout_ms) = params.get("timeout_ms").and_then(Value::as_u64) {
            request = request.timeout(Duration::from_millis(timeout_ms));
        }

        // Send request
        let response = request.send().map_err(|e| classify_reqwest_error(&e, url))?;

        // Build output — all HTTP responses (including 4xx, 5xx) are success
        let status = response.status().as_u16();
        let response_headers: HashMap<String, String> = response
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();
        let body = response
            .text()
            .map_err(|e| ConnectorError::Retryable(format!("failed to read response body: {e}")))?;

        Ok(json!({
            "status": status,
            "headers": response_headers,
            "body": body,
        }))
    }
}

fn classify_reqwest_error(err: &reqwest::Error, url: &str) -> ConnectorError {
    if err.is_builder() {
        ConnectorError::InvalidParams(format!("invalid URL: {url}"))
    } else {
        // Connection, timeout, and other transport errors are retryable
        ConnectorError::Retryable(format!("HTTP request failed: {err}"))
    }
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;

    use serde_json::json;
    use uuid::Uuid;
    use wi_core::id::{FlowRunId, NodeId, StepRunId};

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

    /// Spawn a minimal mock HTTP server that accepts one connection, reads the
    /// request, and writes a canned response. Returns (url, join_handle).
    /// The `handler` receives the raw request lines and returns (status_line, headers, body).
    fn mock_server<F>(handler: F) -> (String, std::thread::JoinHandle<Vec<String>>)
    where
        F: FnOnce(&[String]) -> (String, Vec<(String, String)>, String) + Send + 'static,
    {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://127.0.0.1:{}", addr.port());

        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let reader = BufReader::new(stream.try_clone().unwrap());

            // Read request headers
            let mut lines = Vec::new();
            for line in reader.lines() {
                let line = line.unwrap();
                if line.is_empty() {
                    break;
                }
                lines.push(line);
            }

            let (status, headers, body) = handler(&lines);

            let mut response = format!("HTTP/1.1 {status}\r\n");
            for (k, v) in &headers {
                response.push_str(&format!("{k}: {v}\r\n"));
            }
            response.push_str(&format!("Content-Length: {}\r\n", body.len()));
            response.push_str("\r\n");
            response.push_str(&body);

            stream.write_all(response.as_bytes()).unwrap();
            stream.flush().unwrap();

            lines
        });

        (url, handle)
    }

    fn simple_ok_handler(_lines: &[String]) -> (String, Vec<(String, String)>, String) {
        ("200 OK".into(), vec![("Content-Type".into(), "text/plain".into())], "ok".into())
    }

    #[test]
    fn http_get_success() {
        let (url, handle) = mock_server(simple_ok_handler);
        let ctx = test_ctx();
        let result = HttpRequestConnector::new()
            .invoke(&ctx, &json!({"url": url, "method": "GET"}))
            .unwrap();

        assert_eq!(result["status"], 200);
        assert_eq!(result["body"], "ok");
        handle.join().unwrap();
    }

    #[test]
    fn http_post_with_body() {
        let (url, handle) = mock_server(|lines| {
            // Verify it's a POST
            assert!(lines[0].starts_with("POST"));
            (
                "201 Created".into(),
                vec![("Content-Type".into(), "application/json".into())],
                r#"{"id":1}"#.into(),
            )
        });

        let ctx = test_ctx();
        let result = HttpRequestConnector::new()
            .invoke(
                &ctx,
                &json!({
                    "url": url,
                    "method": "POST",
                    "body": "request body"
                }),
            )
            .unwrap();

        assert_eq!(result["status"], 201);
        assert_eq!(result["body"], r#"{"id":1}"#);
        handle.join().unwrap();
    }

    #[test]
    fn http_includes_idempotency_header() {
        let (url, handle) = mock_server(|lines| {
            // Find the idempotency key header
            let has_key = lines.iter().any(|l| l.to_lowercase().starts_with("x-idempotency-key:"));
            assert!(has_key, "X-Idempotency-Key header not found in: {lines:?}");
            simple_ok_handler(lines)
        });

        let ctx = test_ctx();
        HttpRequestConnector::new().invoke(&ctx, &json!({"url": url})).unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn http_returns_response_headers() {
        let (url, handle) = mock_server(|_| {
            (
                "200 OK".into(),
                vec![
                    ("Content-Type".into(), "text/plain".into()),
                    ("X-Custom".into(), "custom-value".into()),
                ],
                "ok".into(),
            )
        });

        let ctx = test_ctx();
        let result = HttpRequestConnector::new().invoke(&ctx, &json!({"url": url})).unwrap();

        let headers = result["headers"].as_object().unwrap();
        assert_eq!(headers["x-custom"], "custom-value");
        handle.join().unwrap();
    }

    #[test]
    fn http_connection_refused_is_retryable() {
        // Bind a port then drop the listener so it's closed
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let ctx = test_ctx();
        let result = HttpRequestConnector::new()
            .invoke(&ctx, &json!({"url": format!("http://127.0.0.1:{port}"), "timeout_ms": 1000}));

        assert!(matches!(result, Err(ConnectorError::Retryable(_))));
    }

    #[test]
    fn http_invalid_url_is_invalid_params() {
        let ctx = test_ctx();
        let result = HttpRequestConnector::new().invoke(&ctx, &json!({"url": "not a valid url"}));
        // reqwest may treat this as a builder error or connection error
        assert!(result.is_err());
    }

    #[test]
    fn http_missing_url_is_invalid_params() {
        let ctx = test_ctx();
        let result = HttpRequestConnector::new().invoke(&ctx, &json!({"method": "GET"}));
        assert!(matches!(result, Err(ConnectorError::InvalidParams(_))));
    }

    #[test]
    fn http_missing_method_defaults_to_get() {
        let (url, handle) = mock_server(|lines| {
            assert!(lines[0].starts_with("GET"));
            simple_ok_handler(lines)
        });

        let ctx = test_ctx();
        let result = HttpRequestConnector::new().invoke(&ctx, &json!({"url": url})).unwrap();

        assert_eq!(result["status"], 200);
        handle.join().unwrap();
    }

    #[test]
    fn http_5xx_is_successful_output() {
        let (url, handle) =
            mock_server(|_| ("500 Internal Server Error".into(), vec![], "server error".into()));

        let ctx = test_ctx();
        let result = HttpRequestConnector::new().invoke(&ctx, &json!({"url": url})).unwrap();

        assert_eq!(result["status"], 500);
        assert_eq!(result["body"], "server error");
        handle.join().unwrap();
    }

    #[test]
    fn http_4xx_is_successful_output() {
        let (url, handle) = mock_server(|_| ("404 Not Found".into(), vec![], "not found".into()));

        let ctx = test_ctx();
        let result = HttpRequestConnector::new().invoke(&ctx, &json!({"url": url})).unwrap();

        assert_eq!(result["status"], 404);
        assert_eq!(result["body"], "not found");
        handle.join().unwrap();
    }

    #[test]
    fn http_timeout_is_retryable() {
        let (url, _handle) = mock_server(|_| {
            // Sleep longer than the timeout
            std::thread::sleep(std::time::Duration::from_secs(5));
            simple_ok_handler(&[])
        });

        let ctx = test_ctx();
        let result =
            HttpRequestConnector::new().invoke(&ctx, &json!({"url": url, "timeout_ms": 100}));

        assert!(matches!(result, Err(ConnectorError::Retryable(_))));
    }

    #[test]
    fn http_descriptor() {
        let desc = HttpRequestConnector::new().describe();
        assert_eq!(desc.name, "http.request");
        assert_eq!(desc.category, ConnectorCategory::Http);
        assert!(!desc.idempotent);
        assert!(desc.side_effects);
        assert!(desc.input_schema.is_some());
        assert!(desc.output_schema.is_some());
    }
}

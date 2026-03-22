//! Host-caller test module — exercises custom Exo host functions.

wit_bindgen::generate!({
    world: "connector-world",
    path: "../../wit",
});

use exo::connector::{crypto, kv, logging, process};

struct HostCallerConnector;

impl exports::exo::connector::connector::Guest for HostCallerConnector {
    fn describe() -> exports::exo::connector::connector::Descriptor {
        exports::exo::connector::connector::Descriptor {
            name: "test.host-caller".to_string(),
            display_name: "Test Host Caller".to_string(),
            description: "Exercises host functions for testing".to_string(),
            input_schema: None,
            output_schema: None,
            idempotent: false,
            side_effects: true,
        }
    }

    fn invoke(
        _ctx: exports::exo::connector::connector::InvocationContext,
        params: String,
    ) -> Result<String, String> {
        // Parse action from params JSON (simple string matching)
        if params.contains("\"action\":\"log\"") || params.contains("\"action\": \"log\"") {
            logging::log(logging::LogLevel::Info, "hello from wasm");
            return Ok(r#"{"logged":true}"#.to_string());
        }

        if params.contains("\"action\":\"sha256\"") || params.contains("\"action\": \"sha256\"") {
            let digest = crypto::sha256(b"hello world");
            return Ok(format!(r#"{{"digest":"{}"}}"#, digest));
        }

        if params.contains("\"action\":\"hmac\"") || params.contains("\"action\": \"hmac\"") {
            let mac = crypto::hmac_sha256(b"secret", b"data");
            let hex: String = mac.iter().map(|b| format!("{:02x}", b)).collect();
            return Ok(format!(r#"{{"hmac":"{}"}}"#, hex));
        }

        if params.contains("\"action\":\"exec\"") || params.contains("\"action\": \"exec\"") {
            match process::exec("echo", &["hello from process".to_string()], None) {
                Ok(result) => {
                    return Ok(format!(
                        r#"{{"stdout":"{}","exit_code":{}}}"#,
                        result.stdout.trim(),
                        result.exit_code.unwrap_or(-1)
                    ));
                }
                Err(e) => return Err(format!("process error: {e}")),
            }
        }

        if params.contains("\"action\":\"kv_set\"") || params.contains("\"action\": \"kv_set\"") {
            kv::set("test-key", "test-value");
            return Ok(r#"{"stored":true}"#.to_string());
        }

        if params.contains("\"action\":\"kv_get\"") || params.contains("\"action\": \"kv_get\"") {
            let value = kv::get("test-key");
            return Ok(format!(
                r#"{{"value":{}}}"#,
                match value {
                    Some(v) => format!("\"{}\"", v),
                    None => "null".to_string(),
                }
            ));
        }

        if params.contains("\"action\":\"kv_delete\"")
            || params.contains("\"action\": \"kv_delete\"")
        {
            let deleted = kv::delete("test-key");
            return Ok(format!(r#"{{"deleted":{}}}"#, deleted));
        }

        if params.contains("\"action\":\"kv_list\"")
            || params.contains("\"action\": \"kv_list\"")
        {
            let keys = kv::list_keys("test");
            return Ok(format!(
                r#"{{"keys":[{}]}}"#,
                keys.iter()
                    .map(|k| format!("\"{}\"", k))
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }

        // WASI clocks: std::time uses wasi:clocks under the hood
        if params.contains("\"action\":\"clock\"") || params.contains("\"action\": \"clock\"") {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();
            return Ok(format!(r#"{{"epoch_secs":{}}}"#, now.as_secs()));
        }

        // WASI random: getrandom uses wasi:random under the hood
        if params.contains("\"action\":\"random\"") || params.contains("\"action\": \"random\"") {
            let mut buf = [0u8; 16];
            getrandom::getrandom(&mut buf).map_err(|e| format!("random error: {e}"))?;
            let hex: String = buf.iter().map(|b| format!("{:02x}", b)).collect();
            return Ok(format!(r#"{{"random":"{}","len":{}}}"#, hex, buf.len()));
        }

        // WASI environment: std::env uses wasi:cli/environment
        if params.contains("\"action\":\"env\"") || params.contains("\"action\": \"env\"") {
            let home = std::env::var("HOME").unwrap_or_default();
            let secret = std::env::var("SECRET_KEY").unwrap_or_default();
            return Ok(format!(
                r#"{{"home":"{}","secret":"{}"}}"#,
                home, secret
            ));
        }

        Err(format!("unknown action in params: {params}"))
    }
}

export!(HostCallerConnector);

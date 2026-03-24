//! discord — Discord WASM connector.
//!
//! Outbound: REST API (send_message via invoke()).
//! Inbound: Gateway WebSocket (streaming-connector export).

wit_bindgen::generate!({
    world: "streaming-connector-world",
    path: "../../../crates/worldinterface-wasm/wit",
});

use exo::connector::{http, kv, logging};

struct DiscordConnector;

const DISCORD_API_BASE: &str = "https://discord.com/api/v10";

// ── Standard connector export (outbound REST) ──

impl exports::exo::connector::connector::Guest for DiscordConnector {
    fn describe() -> exports::exo::connector::connector::Descriptor {
        exports::exo::connector::connector::Descriptor {
            name: "discord".to_string(),
            display_name: "Discord".to_string(),
            description: "Discord communication — outbound REST + inbound Gateway".to_string(),
            input_schema: Some(INPUT_SCHEMA.to_string()),
            output_schema: Some(OUTPUT_SCHEMA.to_string()),
            idempotent: false,
            side_effects: true,
        }
    }

    fn invoke(
        _ctx: exports::exo::connector::connector::InvocationContext,
        params: String,
    ) -> Result<String, String> {
        let parsed: serde_json::Value =
            serde_json::from_str(&params).map_err(|e| format!("invalid params JSON: {e}"))?;

        let action = parsed["action"].as_str().ok_or("missing required field 'action'")?;

        // Read bot token from environment
        let bot_token = std::env::var("DISCORD_BOT_TOKEN")
            .map_err(|_| "DISCORD_BOT_TOKEN not set in environment".to_string())?;

        // Read API base URL from KV (configurable for testing/custom deployments)
        let api_base = kv::get("api_base_url").unwrap_or_else(|| DISCORD_API_BASE.to_string());

        match action {
            "send_message" => send_message(&parsed, &bot_token, &api_base),
            _ => Err(format!("unknown action: '{action}'. Valid: send_message")),
        }
    }
}

// ── Streaming connector export (inbound Gateway) ──

impl exports::exo::connector::streaming_connector::Guest for DiscordConnector {
    fn stream_config() -> exports::exo::connector::streaming_connector::StreamSetup {
        // Read bot token from environment
        let bot_token = std::env::var("DISCORD_BOT_TOKEN").unwrap_or_default();

        // Read Gateway URL from KV (or use default)
        let gateway_url = kv::get("gateway_url")
            .unwrap_or_else(|| "wss://gateway.discord.gg/?v=10&encoding=json".to_string());

        // Build IDENTIFY payload (op 2)
        let identify = serde_json::json!({
            "op": 2,
            "d": {
                "token": bot_token,
                "intents": 512,  // GUILD_MESSAGES intent
                "properties": {
                    "os": "linux",
                    "browser": "exoskeleton",
                    "device": "exoskeleton"
                }
            }
        });

        exports::exo::connector::streaming_connector::StreamSetup {
            url: gateway_url,
            headers: vec![], // Discord Gateway doesn't use auth headers (token is in IDENTIFY)
            init_messages: vec![identify.to_string()],
            heartbeat_interval_ms: 41250, // Discord's standard interval
            heartbeat_payload: Some("{\"op\":1,\"d\":null}".to_string()),
        }
    }

    fn on_message(
        raw: String,
    ) -> Vec<exports::exo::connector::streaming_connector::StreamMessage> {
        on_message_impl(&raw)
    }
}

fn on_message_impl(
    raw: &str,
) -> Vec<exports::exo::connector::streaming_connector::StreamMessage> {
    let parsed: serde_json::Value = match serde_json::from_str(raw) {
        Ok(v) => v,
        Err(_) => return vec![],
    };

    let op = parsed["op"].as_u64().unwrap_or(u64::MAX);

    match op {
        // Dispatch event (op 0)
        0 => {
            let event_type = parsed["t"].as_str().unwrap_or("");
            if event_type != "MESSAGE_CREATE" {
                return vec![];
            }

            let data = &parsed["d"];

            // Check if the message mentions our bot
            let bot_user_id = kv::get("bot_user_id").unwrap_or_default();
            if bot_user_id.is_empty() {
                logging::log(
                    logging::LogLevel::Warn,
                    "discord: bot_user_id not configured in KV, cannot filter mentions",
                );
                return vec![];
            }

            // Check mentions array for our bot
            let mentions = data["mentions"].as_array();
            let is_mentioned = mentions
                .map(|arr| arr.iter().any(|m| m["id"].as_str() == Some(&bot_user_id)))
                .unwrap_or(false);

            if !is_mentioned {
                return vec![];
            }

            // Don't process our own messages
            let author_id = data["author"]["id"].as_str().unwrap_or("");
            if author_id == bot_user_id {
                return vec![];
            }

            // Extract and clean content — strip the bot mention
            let content = data["content"].as_str().unwrap_or("");
            let mention_tag = format!("<@{}>", bot_user_id);
            let cleaned = content.replace(&mention_tag, "").trim().to_string();

            if cleaned.is_empty() {
                return vec![]; // Mention-only message with no content
            }

            // Build metadata
            let mut metadata = vec![];
            if let Some(channel_id) = data["channel_id"].as_str() {
                metadata.push(("channel_id".to_string(), channel_id.to_string()));
            }
            if let Some(guild_id) = data["guild_id"].as_str() {
                metadata.push(("guild_id".to_string(), guild_id.to_string()));
            }
            if let Some(message_id) = data["id"].as_str() {
                metadata.push(("message_id".to_string(), message_id.to_string()));
            }

            let source_identity = format!("discord:user:{}", author_id);

            vec![exports::exo::connector::streaming_connector::StreamMessage {
                source_identity,
                content: cleaned,
                metadata,
            }]
        }

        // Hello — host handles heartbeat, we return nothing
        10 => vec![],

        // Heartbeat ACK — acknowledged, nothing to do
        11 => vec![],

        // All other ops — not application-level messages
        _ => vec![],
    }
}

fn send_message(
    params: &serde_json::Value,
    bot_token: &str,
    api_base: &str,
) -> Result<String, String> {
    let channel_id =
        params["channel_id"].as_str().ok_or("missing required field 'channel_id'")?;

    // Build message payload
    let mut payload = serde_json::Map::new();

    if let Some(content) = params["content"].as_str() {
        payload.insert("content".into(), serde_json::Value::String(content.into()));
    }
    if let Some(embeds) = params["embeds"].as_array() {
        payload.insert("embeds".into(), serde_json::Value::Array(embeds.clone()));
    }
    if payload.is_empty() {
        return Err("must provide 'content' and/or 'embeds'".into());
    }

    let url = format!("{}/channels/{}/messages", api_base, channel_id);
    let body = serde_json::to_string(&payload)
        .map_err(|e| format!("failed to serialize payload: {e}"))?;

    let headers = vec![
        ("Authorization".into(), format!("Bot {bot_token}")),
        ("Content-Type".into(), "application/json".into()),
        (
            "User-Agent".into(),
            "WorldInterface/0.1 (https://github.com/zed-colonel/worldinterface)".into(),
        ),
    ];

    logging::log(
        logging::LogLevel::Debug,
        &format!("discord: sending to channel {channel_id}"),
    );

    let response = http::request("POST", &url, &headers, Some(&body))
        .map_err(|e| format!("Discord API request failed: {e}"))?;

    if response.status < 200 || response.status >= 300 {
        return Err(format!(
            "Discord API error ({}): {}",
            response.status,
            &response.body[..response.body.len().min(300)]
        ));
    }

    // Parse response for message_id
    let resp_body: serde_json::Value = serde_json::from_str(&response.body)
        .map_err(|e| format!("failed to parse Discord response: {e}"))?;

    Ok(serde_json::json!({
        "message_id": resp_body["id"].as_str().unwrap_or(""),
        "channel_id": channel_id,
    })
    .to_string())
}

const INPUT_SCHEMA: &str = r#"{"type":"object","required":["action","channel_id"],"properties":{"action":{"type":"string","enum":["send_message"],"description":"Action to perform"},"channel_id":{"type":"string","description":"Discord channel ID"},"content":{"type":"string","description":"Message text content"},"embeds":{"type":"array","description":"Discord embeds","items":{"type":"object"}}}}"#;

const OUTPUT_SCHEMA: &str = r#"{"type":"object","properties":{"message_id":{"type":"string"},"channel_id":{"type":"string"}}}"#;

export!(DiscordConnector);

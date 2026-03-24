//! discord — Discord REST API WASM connector (outbound).
//!
//! Authenticated with the bot's own token from the environment.
//! S2: outbound REST only (send_message).
//! S3: will add streaming-connector export for Gateway inbound.

wit_bindgen::generate!({
    world: "connector-world",
    path: "../../../crates/worldinterface-wasm/wit",
});

use exo::connector::{http, logging};

struct DiscordConnector;

const DISCORD_API_BASE: &str = "https://discord.com/api/v10";

impl exports::exo::connector::connector::Guest for DiscordConnector {
    fn describe() -> exports::exo::connector::connector::Descriptor {
        exports::exo::connector::connector::Descriptor {
            name: "discord".to_string(),
            display_name: "Discord".to_string(),
            description: "Discord communication via REST API".to_string(),
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

        match action {
            "send_message" => send_message(&parsed, &bot_token),
            _ => Err(format!("unknown action: '{action}'. Valid: send_message")),
        }
    }
}

fn send_message(params: &serde_json::Value, bot_token: &str) -> Result<String, String> {
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

    let url = format!("{}/channels/{}/messages", DISCORD_API_BASE, channel_id);
    let body =
        serde_json::to_string(&payload).map_err(|e| format!("failed to serialize payload: {e}"))?;

    let headers = vec![
        ("Authorization".into(), format!("Bot {bot_token}")),
        ("Content-Type".into(), "application/json".into()),
        (
            "User-Agent".into(),
            "WorldInterface/0.1 (https://github.com/zed-colonel/worldinterface)".into(),
        ),
    ];

    logging::log(logging::LogLevel::Debug, &format!("discord: sending to channel {channel_id}"));

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

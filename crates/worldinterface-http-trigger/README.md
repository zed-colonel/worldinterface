# worldinterface-http-trigger

Dynamic webhook ingress for WorldInterface.

## Overview

This crate provides dynamic webhook registration and request handling:

- **WebhookRegistry** -- In-memory + ContextStore-backed webhook registration
- **WebhookRegistration** -- Associates a URL path with a FlowSpec
- **handle_webhook** -- Processes incoming HTTP requests, submits flows via EmbeddedHost
- **TriggerInput** -- Parsed webhook request payload injected as flow input
- **validation** -- Webhook path validation

Webhook registrations persist across restarts via the ContextStore global key space.

## Part of the WorldInterface workspace

See the [workspace root](https://github.com/zed-colonel/worldinterface) for full documentation.

## License

MIT

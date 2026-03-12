# wi-daemon

HTTP API daemon for WorldInterface.

## Overview

This crate provides the HTTP server that exposes WorldInterface as a service:

- **server** -- Daemon startup with graceful shutdown
- **routes** -- REST API endpoints for flow submission, run inspection, capability discovery, and webhook management
- **config** -- DaemonConfig with TOML file loading
- **metrics** -- Prometheus metrics registry for flow and step counters

## API Endpoints

- `POST /api/v1/flows` -- Submit a FlowSpec for execution
- `GET /api/v1/runs` -- List flow runs
- `GET /api/v1/runs/:id` -- Inspect a flow run
- `GET /api/v1/capabilities` -- List registered connectors
- `POST /api/v1/webhooks` -- Register a webhook
- `GET /metrics` -- Prometheus metrics

## Part of the WorldInterface workspace

See the [workspace root](https://github.com/zed-colonel/worldinterface) for full documentation.

## License

MIT

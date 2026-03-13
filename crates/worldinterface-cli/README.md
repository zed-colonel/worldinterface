# worldinterface-cli

Command-line interface for WorldInterface.

## Overview

This crate provides the `wi` binary for operating WorldInterface:

- `wi serve` -- Start the daemon in the foreground
- `wi flow submit` -- Submit a FlowSpec (YAML or JSON)
- `wi flow list` / `wi flow inspect` -- List and inspect flow runs
- `wi capabilities list` / `wi capabilities describe` -- Connector discovery
- `wi invoke` -- Invoke a single connector directly
- `wi webhook register` / `wi webhook list` / `wi webhook delete` -- Webhook management

## Part of the WorldInterface workspace

See the [workspace root](https://github.com/zed-colonel/worldinterface) for full documentation.

## License

MIT

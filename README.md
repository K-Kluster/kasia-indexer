# Kasia Messenger Indexer

A lightweight, specialized indexer for the Kasia messenger application built on Kaspa BlockDAG. This indexer only processes and stores messaging-related transaction data, making it highly efficient and resource-optimized for messenger-specific use cases.

## ⚠️ Development Status

**This project is currently in active development and is NOT ready for production use.**


## Features

- **Real-time BlockDAG Indexing**: Efficiently processes Kaspa blocks and transactions
- **Scalable Architecture**: Modular design with separate processing pipelines
- **Gap Detection & Recovery**: Automatic handling of missing blocks and chain reorganizations
- **Metrics & Monitoring**: Built-in metrics collection for operational visibility

## Architecture

The indexer consists of several key components:

### Core Modules
- **Block Processor**: Extracts and parses encrypted messages from transaction data
- **Virtual Chain Processor**: Handles Virtual Chain Changed (VCC) notifications and transaction acceptance
- **Periodic Processor**: Manages resolution of unknown transactions and DAA scores
- **Historical Syncer**: Syncs historical blockchain data from a specified starting point
- **Chain Subscriber**: Real-time subscription to new blocks and chain updates

### Database Organization
- **Headers**: Block compact headers and gap tracking
- **Messages**: Protocol message storage (handshakes, payments, contextual messages)
- **Processing**: Transaction processing state and resolution workflows

### Message Types
- **Handshakes**: Initial connection establishment between parties
- **Payments**: Payment transactions with optional attached messages  
- **Contextual Messages**: Application-specific encrypted messages

Useful commands:

- run locally: `RUST_LOG=info cargo run -r -p indexer`
- build docker image `docker build -t kasia-indexer .`
- run docker container: `docker run -v ./data:/root/.kasia-indexer -e RUST_LOG=info --restart always kasia-indexer ./indexer`

## Env vars

```bash
# debug, info, warn, error
RUST_LOG=info
# default to home_dir/.kasia-indexer, must be an existing directory with read/write permissions
# KASIA_INDEXER_DB_PATH=
# if not defined, fallback to public kaspa network, if specified, the `ws://{ip}:{port}` node url
KASPA_NODE_WBORSH_URL=
```

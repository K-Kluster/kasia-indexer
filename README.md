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
- build docker image `docker build -t kkluster/kasia-indexer .`
- run as docker-compose: `docker compose up -d`

## API

- http://localhost:8080/swagger-ui/
- `GET http://localhost:8080/metrics` exposes Prometheus text metrics.

## Reverse Proxy Hardening

For internet-exposed deployments, place the indexer behind nginx and enforce push endpoint limits at the edge.

- Example hardened snippet: `nginx/push-security.conf`
- Enforces:
  - `/v1/push/*` request body cap (`64k`)
  - rate limiting for push mutation endpoints
  - blocking deprecated unversioned `/push/*` paths

## Env vars

```bash
# debug, info, warn, error
RUST_LOG=info

# default to home_dir/.kasia-indexer/mainnet, must be an existing directory with read/write permissions
#KASIA_INDEXER_DB_ROOT=

# default to mainnet, allowed values: mainnet, testnet
NETWORK_TYPE=mainnet

# if not defined, fallback to public kaspa network, if specified, the `ws://{ip}:{port}` node url
#KASPA_NODE_WBORSH_URL=

# APNs push (optional)
#APNS_TEAM_ID=5V64BP2H3P
#APNS_KEY_ID=
#APNS_TOPIC=com.kasia.messenger # iOS bundle id, e.g. fyi.kasia.app
#APNS_KEY_PATH= # path to .p8 key file
#APNS_KEY=" # raw PEM key contents (optional alternative to APNS_KEY_PATH)
#APNS_ENVIRONMENT=sandbox # sandbox or production (default: sandbox)

# push mutation auth mode: legacy, mixed, strict (default: mixed)
# - legacy: no signature required
# - mixed: accepts signed + legacy, but wallet-bound tokens require auth
# - strict: signature + nonce + App Attest required for register/update/unregister
#   (requires APNS_TEAM_ID and APNS_TOPIC to derive App Attest App ID)
#PUSH_AUTH_MODE=mixed
```

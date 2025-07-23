# Introduction

A Proof-of-Conecept lightweight and well-optimized indexer tailored to a specific protocol (Kasia in this context). Can be modified to support a totally different protocol.

Disclaimer: this project is still in work-in-progress, don't use it for production usage.

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

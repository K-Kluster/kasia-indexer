FROM rust:1.89.0-alpine3.20 AS builder

WORKDIR /usr/src

RUN apk add --no-cache pcc-libs-dev musl-dev pkgconfig openssl-dev openssl-libs-static curl

COPY . .

RUN cargo build --release

FROM alpine:3.20

WORKDIR /usr/src

COPY --from=builder /usr/src/target/release/indexer .

CMD ["./indexer"]

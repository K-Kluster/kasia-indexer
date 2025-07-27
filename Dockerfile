FROM rust:1.88.0-alpine3.20 as builder

WORKDIR /usr/src/indexer

RUN apk add --no-cache pcc-libs-dev musl-dev pkgconfig openssl-dev openssl-libs-static curl

COPY . ./

RUN cargo build --release

FROM alpine:3.20

WORKDIR /usr/src/indexer

COPY --from=builder /usr/src/indexer/target/release/indexer .

CMD ["./indexer"]

FROM rustlang/rust:nightly AS builder
WORKDIR /usr/src/myapp
COPY . .
RUN cargo install --path . --bin rutin_server 

FROM debian:bullseye-slim
COPY --from=builder /usr/local/cargo/bin/rutin_server /usr/local/bin/rutin_server
CMD ["rutin_server"]

# Stage 1: Build
FROM rust:1.83-slim-bookworm as builder

WORKDIR /usr/src/app
COPY . .

# Install build dependencies (if any needed for sqlite3 source compilation)
# sqlx with sqlite feature usually compiles sqlite3 bundled, but we need pkg-config/openssl sometimes
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

RUN cargo build --release

# Stage 2: Runtime
FROM debian:bookworm-slim

WORKDIR /usr/local/bin

# Install runtime dependencies for SQLite and SSL
RUN apt-get update && apt-get install -y ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/app/target/release/backend .
COPY --from=builder /usr/src/app/metrics_manifest.toml .

# Environment variables
ENV RUST_LOG=info
ENV DATABASE_URL=sqlite:health.db

# Expose the port
EXPOSE 3000

CMD ["./backend"]

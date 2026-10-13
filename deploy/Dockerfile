# Multi-stage build for Market Chronicle
FROM rust:latest AS builder

WORKDIR /app

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# Build in release mode
RUN cargo build --release --bin market-chronicle

# Runtime stage - use same base as builder for glibc compatibility
FROM debian:sid-slim

# Install CA certificates and other runtime dependencies
RUN apt-get update && \
    apt-get install -y ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

# Create a non-root user
RUN useradd -m -u 1000 marketchronicle

# Copy binary from builder
COPY --from=builder /app/target/release/market-chronicle /usr/local/bin/market-chronicle

# Set user
USER marketchronicle

# Expose metrics port
EXPOSE 9090

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD ["curl", "-f", "http://localhost:9090/health"]

# Run the service
ENTRYPOINT ["market-chronicle"]
CMD ["/config/config.toml"]

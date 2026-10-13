# Market Chronicle - Cryptocurrency Market Data Archiving Service
# Use `just --list` to see all available recipes

export MINIO_ROOT_USER := "minioadmin"
export MINIO_ROOT_PASSWORD := "minioadmin"
export GF_SECURITY_ADMIN_PASSWORD := "admin"
export MC_S3_ACCESS_KEY_ID := "AKIAJDNTT3MJ7SFC2VJQ"
export MC_S3_SECRET_ACCESS_KEY := "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"

compose := "docker-compose -f deploy/docker-compose.yml"

# Default recipe (show help)
default:
    @just --list

# Build commands

# Build all crates in debug mode
build:
    cargo build --all

# Build all crates in release mode
build-release:
    cargo build --all --release

# Test commands

# Run all tests
test:
    cargo test --all

# Run tests with output
test-verbose:
    cargo test --all -- --nocapture

# Code quality

# Run clippy linter
lint:
    cargo clippy --all-targets --all-features -- -D warnings

# Check code formatting
fmt:
    cargo fmt --all -- --check

# Fix code formatting
fmt-fix:
    cargo fmt --all

# Run cargo check
check:
    cargo check --all

# Run commands

# Run the main service with default config
run:
    cargo run --bin market-chronicle -- config.toml

# Run the main service with example config
run-example:
    cargo run --bin market-chronicle -- config.example.toml

# Run the replay utility (requires arguments)
run-replay:
    @echo "Usage: just run-replay-args '--config config.toml --start-time 2024-01-15T00:00:00Z --end-time 2024-01-15T23:59:59Z'"
    @echo "Run: cargo run --bin market-chronicle-replay -- --help"

# Run the replay utility with custom arguments
run-replay-args ARGS:
    cargo run --bin market-chronicle-replay -- {{ARGS}}

# Run the main service in release mode
run-release:
    cargo run --release --bin market-chronicle -- config.toml

# Docker Compose dev environment

# Start all development services (MinIO, Prometheus, Grafana)
setup-dev:
    {{compose}} up -d
    @echo "Waiting for services to be ready..."
    @sleep 10
    @echo ""
    @echo "✅ Development environment ready!"
    @echo ""
    @echo "📦 MinIO Console:     http://localhost:9001"
    @echo "   Username: minioadmin"
    @echo "   Password: minioadmin"
    @echo ""
    @echo "📊 Prometheus:        http://localhost:9091"
    @echo ""
    @echo "📈 Grafana:           http://localhost:3000"
    @echo "   Username: admin"
    @echo "   Password: admin"
    @echo ""
    @echo "🔍 Loki (Logs):       http://localhost:3100"
    @echo ""
    @echo "Open MinIO at http://localhost:9001, Prometheus at http://localhost:9091, or Grafana at http://localhost:3000"

# Stop development services
stop-dev:
    {{compose}} stop
    @echo "✅ Development services stopped"

# Remove all containers and volumes
clean-dev:
    {{compose}} down -v
    @echo "✅ Development containers and volumes removed"

# Follow logs from all development services
logs-dev:
    {{compose}} logs -f

# Clean commands

# Remove build artifacts
clean-build-artifacts:
    cargo clean
    rm -rf target/

# Install commands

# Install the binaries to cargo bin directory
install:
    cargo install --path crates/market-chronicle
    cargo install --path crates/market-chronicle-replay

# Install development tools
install-dev-tools:
    cargo install cargo-edit
    rustup component add clippy
    rustup component add rustfmt

# Docker commands

# Build Docker image
docker-build:
    docker build -t market-chronicle:latest .

# Start all services including market-chronicle
docker-up:
    {{compose}} up -d

# Stop all Docker services
docker-down:
    {{compose}} down

# Follow logs from market-chronicle service
docker-logs:
    {{compose}} logs -f market-chronicle

# Follow logs from all services
docker-logs-all:
    {{compose}} logs -f

# Restart market-chronicle service
docker-restart:
    {{compose}} restart market-chronicle

# Rebuild and restart market-chronicle
docker-rebuild:
    {{compose}} build market-chronicle
    {{compose}} up -d market-chronicle

# Remove all containers, volumes, and images
docker-clean:
    {{compose}} down -v --rmi all

# Documentation

# Build and open documentation
docs:
    cargo doc --all --no-deps --open

# Build documentation without opening
docs-build:
    cargo doc --all --no-deps

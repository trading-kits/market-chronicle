# Market Chronicle - Cryptocurrency Market Data Archiving Service
# Use `just --list` to see all available recipes

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

# Start all development services (MinIO, PostgreSQL, LakeFS, Prometheus, Grafana)
setup-dev:
    docker-compose -f deploy/docker-compose.yml up -d
    @echo "Waiting for services to be ready..."
    @sleep 10
    @echo ""
    @echo "✅ Development environment ready!"
    @echo ""
    @echo "📦 MinIO Console:     http://localhost:9001"
    @echo "   Username: minioadmin"
    @echo "   Password: minioadmin"
    @echo ""
    @echo "🏔  LakeFS Console:    http://localhost:8000"
    @echo "   Setup required on first run"
    @echo ""
    @echo "📊 Prometheus:        http://localhost:9091"
    @echo ""
    @echo "📈 Grafana:           http://localhost:3000"
    @echo "   Username: admin"
    @echo "   Password: admin"
    @echo ""
    @echo "🔍 Loki (Logs):       http://localhost:3100"
    @echo ""
    @echo "Run 'just minio-console', 'just lakefs-console', or 'just grafana-console' to open in browser"

# Stop development services
stop-dev:
    docker-compose -f deploy/docker-compose.yml stop
    @echo "✅ Development services stopped"

# Follow logs from all development services
logs-dev:
    docker-compose -f deploy/docker-compose.yml logs -f

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
    docker-compose up -d

# Stop all Docker services
docker-down:
    docker-compose down

# Follow logs from market-chronicle service
docker-logs:
    docker-compose logs -f market-chronicle

# Follow logs from all services
docker-logs-all:
    docker-compose logs -f

# Restart market-chronicle service
docker-restart:
    docker-compose restart market-chronicle

# Rebuild and restart market-chronicle
docker-rebuild:
    docker-compose build market-chronicle
    docker-compose up -d market-chronicle

# Remove all containers, volumes, and images
docker-clean:
    docker-compose down -v --rmi all

# Documentation

# Build and open documentation
docs:
    cargo doc --all --no-deps --open

# Build documentation without opening
docs-build:
    cargo doc --all --no-deps

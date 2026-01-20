.PHONY: build build-release test lint fmt run run-replay setup-dev stop-dev clean-dev logs-dev minio-console lakefs-console clean install help

# Default target
.DEFAULT_GOAL := help

## Build commands
build: ## Build all crates in debug mode
	cargo build --all

build-release: ## Build all crates in release mode
	cargo build --all --release

## Test commands
test: ## Run all tests
	cargo test --all

test-verbose: ## Run tests with output
	cargo test --all -- --nocapture

## Code quality
lint: ## Run clippy linter
	cargo clippy --all-targets --all-features -- -D warnings

fmt: ## Check code formatting
	cargo fmt --all -- --check

fmt-fix: ## Fix code formatting
	cargo fmt --all

check: ## Run cargo check
	cargo check --all

## Run commands
run: ## Run the main service with default config
	cargo run --bin market-chronicle -- config.toml

run-example: ## Run the main service with example config
	cargo run --bin market-chronicle -- config.example.toml

run-replay: ## Run the replay utility (requires arguments)
	@echo "Usage: make run-replay ARGS='--config config.toml --start-time 2024-01-15T00:00:00Z --end-time 2024-01-15T23:59:59Z'"
	@echo "Run: cargo run --bin market-chronicle-replay -- --help"

run-release: ## Run the main service in release mode
	cargo run --release --bin market-chronicle -- config.toml

## Docker Compose dev environment
setup-dev: ## Start all development services (MinIO, PostgreSQL, LakeFS)
	docker-compose up -d
	@echo "Waiting for services to be ready..."
	@sleep 10
	@echo ""
	@echo "✅ Development environment ready!"
	@echo ""
	@echo "📦 MinIO Console:  http://localhost:9001"
	@echo "   Username: minioadmin"
	@echo "   Password: minioadmin"
	@echo ""
	@echo "🏔  LakeFS Console: http://localhost:8000"
	@echo "   Setup required on first run"
	@echo ""
	@echo "Run 'make minio-console' or 'make lakefs-console' to open in browser"

stop-dev: ## Stop development services
	docker-compose stop
	@echo "✅ Development services stopped"

clean-dev: ## Remove all development containers and volumes
	docker-compose down -v
	@echo "✅ Development environment cleaned"

logs-dev: ## Follow logs from all development services
	docker-compose logs -f

logs-minio: ## Follow MinIO logs only
	docker-compose logs -f minio

logs-lakefs: ## Follow LakeFS logs only
	docker-compose logs -f lakefs

logs-postgres: ## Follow PostgreSQL logs only
	docker-compose logs -f postgres

minio-console: ## Open MinIO console in browser
	@echo "Opening MinIO console at http://localhost:9001"
	@command -v open >/dev/null 2>&1 && open http://localhost:9001 || \
	 command -v xdg-open >/dev/null 2>&1 && xdg-open http://localhost:9001 || \
	 echo "Please open http://localhost:9001 in your browser"

lakefs-console: ## Open LakeFS console in browser
	@echo "Opening LakeFS console at http://localhost:8000"
	@command -v open >/dev/null 2>&1 && open http://localhost:8000 || \
	 command -v xdg-open >/dev/null 2>&1 && xdg-open http://localhost:8000 || \
	 echo "Please open http://localhost:8000 in your browser"

## Clean commands
clean: ## Remove build artifacts
	cargo clean
	rm -rf target/

clean-all: clean clean-dev ## Remove all build artifacts and dev environment

## Install commands
install: ## Install the binaries to cargo bin directory
	cargo install --path crates/market-chronicle
	cargo install --path crates/market-chronicle-replay

install-dev-tools: ## Install development tools
	cargo install cargo-watch
	cargo install cargo-edit
	rustup component add clippy
	rustup component add rustfmt

## Watch commands (requires cargo-watch)
watch: ## Watch for changes and rebuild
	cargo watch -x "build --all"

watch-test: ## Watch for changes and run tests
	cargo watch -x "test --all"

watch-run: ## Watch for changes and run the service
	cargo watch -x "run --bin market-chronicle -- config.toml"

## Documentation
docs: ## Build and open documentation
	cargo doc --all --no-deps --open

docs-build: ## Build documentation without opening
	cargo doc --all --no-deps

## Help
help: ## Display this help message
	@echo "Market Chronicle - Cryptocurrency Market Data Archiving Service"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Available targets:"
	@awk 'BEGIN {FS = ":.*##"; printf "\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
	@echo ""

# ============================================================================
# Makefile - Shortcuts for managing the Streaming Data Pipeline
# ============================================================================
#
# Usage:
#   make help          → Show this help
#   make up            → Start all services
#   make test          → Run all pytest tests
#   make producer      → Run the Producer
#
# ============================================================================

.PHONY: help install up down reset logs logs-clickhouse logs-kafka logs-metabase \
        producer check test test-unit test-integration test-cov test-watch \
        sql sql-producer status clean clean-all backup

# --- Colors ------------------------------------------------------------------
GREEN  := $(shell tput -Txterm setaf 2 2>/dev/null || echo "")
YELLOW := $(shell tput -Txterm setaf 3 2>/dev/null || echo "")
RED    := $(shell tput -Txterm setaf 1 2>/dev/null || echo "")
BLUE   := $(shell tput -Txterm setaf 4 2>/dev/null || echo "")
RESET  := $(shell tput -Txterm sgr0 2>/dev/null || echo "")

# --- Default Target ----------------------------------------------------------
.DEFAULT_GOAL := help

# ============================================================================
# HELP
# ============================================================================
help:  ## Show this help message
	@echo ''
	@echo '$(BLUE)═══════════════════════════════════════════════════════════════$(RESET)'
	@echo '$(BLUE)  Streaming Data Pipeline - Available Commands$(RESET)'
	@echo '$(BLUE)═══════════════════════════════════════════════════════════════$(RESET)'
	@echo ''
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(RESET) %s\n", $$1, $$2}'
	@echo ''

# ============================================================================
# SETUP
# ============================================================================
install:  ## Install Python dependencies (creates .venv if missing)
	@if [ ! -d ".venv" ]; then \
		echo "$(YELLOW)Creating virtual environment...$(RESET)"; \
		python3 -m venv .venv; \
	fi
	@echo "$(YELLOW)Installing dependencies...$(RESET)"
	@.venv/bin/pip install -r requirements.txt \
		-i https://mirror-pypi.runflare.com/simple/ \
		--trusted-host mirror-pypi.runflare.com
	@echo "$(GREEN)Dependencies installed.$(RESET)"

# ============================================================================
# INFRASTRUCTURE
# ============================================================================
up:  ## Start all services (ClickHouse, Kafka, Kafka UI, Metabase)
	@echo "$(YELLOW)Starting infrastructure...$(RESET)"
	@docker compose up -d
	@echo "$(YELLOW)Waiting for services to be healthy...$(RESET)"
	@bash -c 'for i in {1..30}; do [ "$$(docker inspect -f "{{.State.Health.Status}}" kafka-broker 2>/dev/null)" = "healthy" ] && break; sleep 2; done'
	@bash -c 'for i in {1..30}; do [ "$$(docker inspect -f "{{.State.Health.Status}}" clickhouse 2>/dev/null)" = "healthy" ] && break; sleep 2; done'
	@echo "$(GREEN)All services are up and running.$(RESET)"
	@echo ""
	@echo "$(GREEN)Access points:$(RESET)"
	@echo "   Kafka UI:    http://localhost:8082"
	@echo "   ClickHouse:  http://localhost:8123"
	@echo "   Metabase:    http://localhost:3000"
	@echo ""
	@echo "$(YELLOW)Tip: Run 'make producer' to start the data producer.$(RESET)"

down:  ## Stop services (data preserved)
	@echo "$(YELLOW)Stopping services...$(RESET)"
	@docker compose down
	@echo "$(GREEN)Services stopped.$(RESET)"

reset:  ## Delete everything and start fresh (⚠️ deletes all data)
	@echo "$(RED)WARNING: This will delete ALL data!$(RESET)"
	@read -p "Are you sure? [y/N] " confirm && [ "$$confirm" = "y" ] || exit 1
	@echo "$(YELLOW)Resetting everything...$(RESET)"
	@docker compose down -v --remove-orphans
	@docker rm -f clickhouse kafka-broker kafka-ui kafka-init metabase 2>/dev/null || true
	@docker volume rm stream-realtime-data_clickhouse-volume 2>/dev/null || true
	@docker volume rm stream-realtime-data_kafka-volume 2>/dev/null || true
	@docker volume rm stream-realtime-data_metabase-data 2>/dev/null || true
	@echo "$(YELLOW)Starting fresh...$(RESET)"
	@$(MAKE) up

# ============================================================================
# LOGS
# ============================================================================
logs:  ## Tail all service logs
	@docker compose logs -f

logs-clickhouse:  ## Tail ClickHouse logs
	@docker logs -f clickhouse

logs-kafka:  ## Tail Kafka logs
	@docker logs -f kafka-broker

logs-metabase:  ## Tail Metabase logs
	@docker logs -f metabase

# ============================================================================
# PRODUCER
# ============================================================================
producer:  ## Run the Wikimedia → Kafka Producer
	@if [ ! -d ".venv" ]; then \
		echo "$(YELLOW)Virtual environment not found. Run 'make install' first.$(RESET)"; \
		exit 1; \
	fi
	@echo "$(YELLOW)Starting Producer... (Ctrl+C to stop)$(RESET)"
	@.venv/bin/python producer.py

# ============================================================================
# TESTS (pytest)
# ============================================================================
test:  ## Run all tests with pytest
	@echo "$(YELLOW)Running all tests...$(RESET)"
	@.venv/bin/pytest tests/ -v
	@echo "$(GREEN)Tests completed.$(RESET)"

test-unit:  ## Run only unit tests (fast)
	@echo "$(YELLOW)Running unit tests...$(RESET)"
	@.venv/bin/pytest tests/ -v -m unit

test-integration:  ## Run only integration tests
	@echo "$(YELLOW)Running integration tests...$(RESET)"
	@.venv/bin/pytest tests/ -v -m integration

test-cov:  ## Run tests with coverage report (HTML + terminal)
	@echo "$(YELLOW)Running tests with coverage...$(RESET)"
	@.venv/bin/pytest tests/ --cov=producer --cov-report=html --cov-report=term
	@echo "$(GREEN)Coverage report: htmlcov/index.html$(RESET)"

test-watch:  ## Run tests in watch mode (requires pytest-watch)
	@.venv/bin/ptw tests/ -- -v

# ============================================================================
# CHECK (Connection verification)
# ============================================================================
check:  ## Verify connections and data flow (no pytest)
	@echo "$(BLUE)═══════════════════════════════════════════════════════════════$(RESET)"
	@echo "$(BLUE)  Connection & Data Verification$(RESET)"
	@echo "$(BLUE)═══════════════════════════════════════════════════════════════$(RESET)"
	@echo ""
	@echo "$(GREEN)1. Kafka topics:$(RESET)"
	@docker exec kafka-broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --list
	@echo ""
	@echo "$(GREEN)2. ClickHouse tables:$(RESET)"
	@docker exec clickhouse clickhouse-client --user admin --password admin123 --query "SHOW TABLES FROM tutorial"
	@echo ""
	@echo "$(GREEN)3. Kafka Consumer status:$(RESET)"
	@docker exec clickhouse clickhouse-client --user admin --password admin123 --query "SELECT database, table, num_messages_read, last_poll_time FROM system.kafka_consumers"
	@echo ""
	@echo "$(GREEN)4. Row counts per layer:$(RESET)"
	@echo -n "   🥉 Bronze: "
	@docker exec clickhouse clickhouse-client --user admin --password admin123 --query "SELECT count() FROM tutorial.bronze_wiki_events"
	@echo -n "   🥈 Silver: "
	@docker exec clickhouse clickhouse-client --user admin --password admin123 --query "SELECT count() FROM tutorial.silver_wiki_events"
	@echo -n "   🥇 Gold:   "
	@docker exec clickhouse clickhouse-client --user admin --password admin123 --query "SELECT count() FROM tutorial.gold_wiki_hourly_stats"
	@echo ""
	@echo "$(GREEN)5. Errors:$(RESET)"
	@echo -n "   Bronze errors: "
	@docker exec clickhouse clickhouse-client --user admin --password admin123 --query "SELECT count() FROM tutorial.bronze_wiki_events_errors"
	@echo ""

# ============================================================================
# SQL CONSOLE
# ============================================================================
sql:  ## Open an interactive ClickHouse SQL console
	@echo "$(YELLOW)Opening ClickHouse SQL console...$(RESET)"
	@docker exec -it clickhouse clickhouse-client --user admin --password admin123

# ============================================================================
# STATUS
# ============================================================================
status:  ## Show status of all containers
	@docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# ============================================================================
# CLEANUP
# ============================================================================
clean:  ## Remove temporary files (logs, caches)
	@echo "$(YELLOW)Cleaning up...$(RESET)"
	@rm -f producer.log
	@rm -rf .pytest_cache
	@rm -rf htmlcov
	@rm -f .coverage
	@find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	@echo "$(GREEN)Cleaned.$(RESET)"

clean-all:  ## Clean everything (⚠️ also stops Docker and removes volumes)
	@echo "$(RED)WARNING: This will stop Docker and delete all data!$(RESET)"
	@read -p "Are you sure? [y/N] " confirm && [ "$$confirm" = "y" ] || exit 1
	@$(MAKE) clean
	@docker compose down -v --remove-orphans
	@docker system prune -f
	@echo "$(GREEN)All cleaned.$(RESET)"

# ============================================================================
# BACKUP
# ============================================================================
backup:  ## Backup ClickHouse tables to ./backups/
	@mkdir -p backups
	@echo "$(YELLOW)Creating backup...$(RESET)"
	@docker exec clickhouse clickhouse-client --user admin --password admin123 \
		--query "SELECT * FROM tutorial.silver_wiki_events FORMAT Native" > backups/silver_$(shell date +%Y%m%d_%H%M%S).native
	@echo "$(GREEN)Backup saved to ./backups/$(RESET)"
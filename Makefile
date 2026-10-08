# ============================================================================
# Makefile - دستورات کوتاه برای مدیریت پروژه
# ============================================================================

.PHONY: help up down reset logs producer test sql status clean logs-clickhouse logs-kafka

GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
RED    := $(shell tput -Txterm setaf 1)
RESET  := $(shell tput -Txterm sgr0)

help:  ## نمایش راهنما
	@echo '$(YELLOW)دستورات موجود:$(RESET)'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-16s$(RESET) %s\n", $$1, $$2}'

up:  ## بالا آوردن همه سرویس‌ها
	@echo "$(YELLOW)Starting infrastructure...$(RESET)"
	docker compose up -d
	@echo "$(YELLOW)Waiting for services to be healthy...$(RESET)"
	@bash -c 'for i in {1..30}; do [ "$$(docker inspect -f "{{.State.Health.Status}}" kafka-broker 2>/dev/null)" = "healthy" ] && break; sleep 2; done'
	@bash -c 'for i in {1..30}; do [ "$$(docker inspect -f "{{.State.Health.Status}}" clickhouse 2>/dev/null)" = "healthy" ] && break; sleep 2; done'
	@echo "$(GREEN)All services are up and running.$(RESET)"
	@echo ""
	@echo "$(GREEN)Access points:$(RESET)"
	@echo "   Kafka UI:   http://localhost:8082"
	@echo "   ClickHouse: http://localhost:8123"
	@echo ""
	@echo "$(YELLOW)Tip: Run 'make producer' to start the data producer.$(RESET)"

down:  ## توقف سرویس‌ها
	@echo "$(YELLOW)Stopping services...$(RESET)"
	docker compose down
	@echo "$(GREEN)Services stopped.$(RESET)"

reset:  ## پاک کردن همه چیز و شروع مجدد
	@echo "$(RED)WARNING: This will delete ALL data!$(RESET)"
	@read -p "Are you sure? [y/N] " confirm && [ "$$confirm" = "y" ] || exit 1
	@echo "$(YELLOW)Resetting everything...$(RESET)"
	docker compose down -v --remove-orphans
	docker rm -f clickhouse kafka-broker kafka-ui kafka-init 2>/dev/null || true
	@echo "$(YELLOW)Starting fresh...$(RESET)"
	$(MAKE) up

logs:  ## مشاهده لاگ‌های همه سرویس‌ها
	docker compose logs -f

logs-clickhouse:  ## لاگ‌های ClickHouse
	docker logs -f clickhouse

logs-kafka:  ## لاگ‌های Kafka
	docker logs -f kafka-broker

producer:  ## اجرای Producer
	@if [ ! -d ".venv" ]; then \
		echo "$(YELLOW)Creating virtual environment...$(RESET)"; \
		python3 -m venv .venv; \
		.venv/bin/pip install -r requirements.txt -i https://mirror-pypi.runflare.com/simple/ --trusted-host mirror-pypi.runflare.com; \
	fi
	.venv/bin/python producer.py

test:  ## تست اتصال‌ها و ساختار
	@echo "$(YELLOW)Testing connections...$(RESET)"
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
	@echo "$(GREEN)4. Bronze rows:$(RESET)"
	@docker exec clickhouse clickhouse-client --user admin --password admin123 --query "SELECT count() FROM tutorial.bronze_wiki_events"
	@echo ""

sql:  ## باز کردن کنسول SQL ClickHouse
	docker exec -it clickhouse clickhouse-client --user admin --password admin123

status:  ## وضعیت کانتینرها
	@docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

clean:  ## پاک کردن فایل‌های موقت
	@echo "$(YELLOW)Cleaning up...$(RESET)"
	rm -f producer.log
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@echo "$(GREEN)Cleaned.$(RESET)"
# 🚀 Streaming Data Pipeline

<div align="center">

**A live data pipeline with Kafka and ClickHouse based on the Medallion Architecture**

[![Tests](https://github.com/HoseinFrhni/stream-realtime-data/actions/workflows/tests.yml/badge.svg)](https://github.com/HoseinFrhni/stream-realtime-data/actions/workflows/tests.yml)
[![Lint](https://github.com/HoseinFrhni/stream-realtime-data/actions/workflows/lint.yml/badge.svg)](https://github.com/HoseinFrhni/stream-realtime-data/actions/workflows/lint.yml)
[![Python](https://img.shields.io/badge/Python-3.10%2B-blue)](https://www.python.org/)
[![Kafka](https://img.shields.io/badge/Kafka-3.9-black)](https://kafka.apache.org/)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-24.10-yellow)](https://clickhouse.com/)
[![Metabase](https://img.shields.io/badge/Metabase-latest-blueviolet)](https://www.metabase.com/)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

</div>

---

## 📖 About the Project

This project is a complete **Streaming Data Pipeline** that consumes live **Wikimedia Recent Changes** data via Kafka and stores/processes it in ClickHouse using the **Medallion Architecture**. The final layer is visualized in **Metabase** with real-time dashboards.

The goal is to practice key data engineering concepts hands-on:

- ✅ Kafka and Kafka Connect
- ✅ ClickHouse and MergeTree engines
- ✅ Materialized View and Kafka Engine
- ✅ Medallion Architecture (Bronze → Silver → Gold)
- ✅ Error handling and Retry
- ✅ Idempotency and Exactly-Once Semantics
- ✅ Docker and container networks
- ✅ Business Intelligence with Metabase

---

## 📸 Dashboard Preview

![Wiki Real-time Analytics Dashboard](docs/images/dashboard-full.png)

*Real-time analytics dashboard built with Metabase showing 2,900+ edits, top users, language distribution, and top pages.*

---

## 🏛️ Architecture

```
┌──────────────────┐     ┌─────────────────┐     ┌──────────────────────────┐
│  Wikimedia SSE   │────▶│  Producer.py    │────▶│  Kafka: wiki-events      │
└──────────────────┘     └─────────────────┘     └────────────┬─────────────┘
                                                              │
                                                              ▼
                                          ┌───────────────────────────────────────┐
                                          │           ClickHouse                  │
                                          │                                        │
                                          │  ┌─────────────────────────────────┐  │
                                          │  │ wiki_events_queue (Kafka Engine)│  │
                                          │  └────┬──────────────────────┬─────┘  │
                                          │       │                      │        │
                                          │       ▼                      ▼        │
                                          │  🥉 BRONZE            🥉 BRONZE       │
                                          │  bronze_wiki_events   errors table    │
                                          │       │                               │
                                          │       ▼                               │
                                          │  🥈 SILVER                            │
                                          │  silver_wiki_events                   │
                                          │       │                               │
                                          │       ▼                               │
                                          │  🥇 GOLD                              │
                                          │  ┌──────────────┬──────────────┐      │
                                          │  │ hourly_stats │ top_users    │      │
                                          │  │ top_pages    │ lang_dist    │      │
                                          │  └──────────────┴──────────────┘      │
                                          └───────────────────────────────────────┘
                                                              │
                                                              ▼
                                                   ┌────────────────────┐
                                                   │     Metabase       │
                                                   │  Real-time BI      │
                                                   └────────────────────┘
```

---

## 🥉🥈🥇 Medallion Architecture

This project follows the **Medallion Architecture**, where data is processed in three sequential layers:

### 🥉 Bronze Layer (Raw)

- **Goal:** Store 100% raw data without any transformation.
- **Tables:** `bronze_wiki_events`, `bronze_wiki_events_errors`
- **Features:** Preserves all event fields + Kafka metadata (topic, partition, offset, timestamp)
- **TTL:** 30 days
- **Consumers:** Data Engineers (for debugging and replay)

### 🥈 Silver Layer (Cleaned)

- **Goal:** Parse raw JSON into structured fields, validate, deduplicate.
- **Tables:** `silver_wiki_events`
- **Features:** Extracted fields + type conversion + enrichment (language derived from wiki)
- **TTL:** 90 days
- **Consumers:** Analysts and Data Scientists

### 🥇 Gold Layer (Business)

- **Goal:** Aggregate data and build KPIs for direct BI consumption.
- **Tables:** `gold_wiki_hourly_stats`, `gold_top_users_daily`, `gold_top_pages_hourly`, `gold_language_hourly`
- **Features:** Aggregated statistics, optimized for fast queries
- **TTL:** 180–365 days
- **Consumers:** Managers, BI, ML

---

## 📁 Project Structure

```
stream-realtime-data/
│
├── 📁 sql/                          # SQL scripts to build layers
│   ├── 01-bronze-layer.sql          # Bronze layer (raw)
│   ├── 02-silver-layer.sql          # Silver layer (cleaned)
│   ├── 03-gold-layer.sql            # Gold layer (aggregated)
│   └── 99-utilities.sql             # Monitoring and debugging queries
│
├── 📁 init-db/                      # Auto-executed SQL on ClickHouse first run
│   ├── 01-bronze-layer.sql
│   ├── 02-silver-layer.sql
│   └── 03-gold-layer.sql
│
├── 📁 docs/                         # Detailed documentation
│   ├── clickhouse-consumer.md       # Guide to ClickHouse as Consumer
│   ├── medallion-architecture.md    # Medallion Architecture explanation
│   ├── metabase-dashboard.md        # Metabase dashboard guide
│   └── images/                      # Screenshots and diagrams
│
├── 📁 metabase-plugins/             # ClickHouse driver for Metabase
│   └── clickhouse.metabase-driver.jar
│
├── 🐍 producer.py                   # Python Producer (Wikimedia → Kafka)
├── 🐳 docker-compose.yaml           # Infrastructure (ClickHouse + Kafka + Kafka UI + Metabase)
├── 📄 Makefile                      # One-command shortcuts
├── 📄 requirements.txt              # Python dependencies
├── 📄 .env.example                  # Environment variables sample
├── 📄 .gitignore                    # Ignored files
├── 📄 README.md                     # This file
└── 📄 sample.json                   # Sample Wikimedia event
```

---

## 🚀 Quick Start

### Prerequisites

| Tool | Recommended Version | Purpose |
|---|---|---|
| **Docker Desktop** | 24.0+ | Run containers |
| **Python** | 3.10+ | Run the Producer |
| **Git** | 2.30+ | Clone the project |
| **DBeaver** or **PyCharm Pro** | Latest | Run SQL queries |
| **Make** | Any | One-command setup (optional) |

### Method 1: Automated Setup with Makefile (Recommended)

```bash
# 1. Clone the project
git clone https://github.com/YOUR-USERNAME/stream-realtime-data.git
cd stream-realtime-data

# 2. Download the ClickHouse driver for Metabase
mkdir -p metabase-plugins
curl -L -o metabase-plugins/clickhouse.metabase-driver.jar \
  https://github.com/ClickHouse/metabase-clickhouse-driver/releases/latest/download/clickhouse.metabase-driver.jar

# 3. Bring everything up (fully automated)
make up

# 4. Run the Producer
make producer
```

**What `make up` does automatically:**

- ✅ Builds ClickHouse (v24.10) with all 15 Bronze/Silver/Gold tables
- ✅ Starts Kafka Broker in KRaft mode (no Zookeeper)
- ✅ Creates the `wiki-events` topic automatically
- ✅ Starts Kafka UI
- ✅ Starts Metabase for BI

**Useful Makefile commands:**

| Command | Description |
|---|---|
| `make install` | Install Python dependencies |
| `make up` | Start all services |
| `make down` | Stop services (data preserved) |
| `make reset` | Delete everything and start fresh |
| `make test` | Run all pytest tests |
| `make test-unit` | Run only unit tests |
| `make test-cov` | Run tests with coverage report |
| `make check` | Verify connections and data flow |
| `make producer` | Run the Producer |
| `make sql` | Open ClickHouse SQL console |
| `make status` | Show container status |
| `make logs` | View all logs |
| `make clean` | Clean temporary files |

### Method 2: Manual Setup with Docker Compose

#### Step 1: Clone the project

```bash
git clone https://github.com/YOUR-USERNAME/stream-realtime-data.git
cd stream-realtime-data
```

#### Step 2: Download the ClickHouse driver

```bash
mkdir -p metabase-plugins
curl -L -o metabase-plugins/clickhouse.metabase-driver.jar \
  https://github.com/ClickHouse/metabase-clickhouse-driver/releases/latest/download/clickhouse.metabase-driver.jar
```

#### Step 3: Bring up the infrastructure

```bash
docker compose up -d
```

**Services:**

| Service | Port | URL |
|---|---|---|
| ClickHouse HTTP | 8123 | http://localhost:8123 |
| ClickHouse Native | 9000 | - |
| Kafka | 9092 | - |
| Kafka UI | 8082 | http://localhost:8082 |
| Metabase | 3000 | http://localhost:3000 |

**Verify:**

```bash
docker ps
```

You should see `clickhouse`, `kafka-broker`, `kafka-ui`, and `metabase` containers.

#### Step 4: Create the Kafka topic

```bash
docker exec -it kafka-broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:29092 \
  --create --topic wiki-events \
  --partitions 3 --replication-factor 1
```

**Verify:**

```bash
docker exec -it kafka-broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:29092 \
  --list
```

You should see `wiki-events`.

#### Step 5: Create ClickHouse layers

In **DBeaver** or **PyCharm**:

1. Connect to ClickHouse:
   - **Host:** `localhost`
   - **Port:** `8123`
   - **User:** `admin`
   - **Password:** `admin123`
   - **Database:** `tutorial`

2. Run files in order:
   - `sql/01-bronze-layer.sql` → Bronze layer
   - `sql/02-silver-layer.sql` → Silver layer
   - `sql/03-gold-layer.sql` → Gold layer

**Verify:**

```sql
SHOW TABLES FROM tutorial;
```

You should see all 15 tables.

#### Step 6: Install Python dependencies

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# Linux/Mac
source .venv/bin/activate

pip install -r requirements.txt
```

> **💡 Tip for Iranian users:** If you face sanctions-related errors, use an Iranian mirror:
> ```bash
> pip install -r requirements.txt \
>   -i https://mirror-pypi.runflare.com/simple/ \
>   --trusted-host mirror-pypi.runflare.com
> ```

#### Step 7: Run the Producer

```bash
python producer.py
```

**Expected output:**

```
2026-10-08 12:00:00,123 [INFO] producer: Starting Wikimedia → Kafka producer.
2026-10-08 12:00:00,456 [INFO] producer: Connected to Kafka at localhost:9092.
2026-10-08 12:00:01,234 [INFO] producer: Connected to Wikimedia. Streaming...
2026-10-08 12:00:01,345 [INFO] producer: Sent -> Python (programming language) | SomeUser
...
```

#### Step 8: Verify data

**In DBeaver:**

```sql
-- Bronze row count
SELECT count() FROM tutorial.bronze_wiki_events;

-- Silver row count
SELECT count() FROM tutorial.silver_wiki_events;

-- Gold row count
SELECT count() FROM tutorial.gold_wiki_hourly_stats;

-- Sample Silver data
SELECT
    title,
    user,
    wiki,
    language,
    length_delta,
    event_timestamp
FROM tutorial.silver_wiki_events
ORDER BY event_timestamp DESC
LIMIT 10;
```

**In Kafka UI:**
- Browser: http://localhost:8082
- Topics → `wiki-events` → Messages

**In Metabase:**
- Browser: http://localhost:3000
- See the "Metabase Setup" section below

---

## 📊 Metabase Setup

### Connecting Metabase to ClickHouse

1. Open http://localhost:3000 and create an admin account.
2. Go to **Admin settings (gear icon) → Databases → Add database**.
3. Select **ClickHouse** and fill in:

   | Field | Value |
   |---|---|
   | Display name | `ClickHouse Tutorial` |
   | Host | `clickhouse` |
   | Port | `8123` |
   | Username | `admin` |
   | Password | `admin123` |
   | Database name | `tutorial` |

4. Click **Connect database**.

> ⚠️ **Note:** Use `clickhouse` as the Host (not `localhost`), because Metabase runs inside a Docker container.

### Dashboard Queries

See [docs/metabase-dashboard.md](docs/metabase-dashboard.md) for full details on building the dashboard with 5 charts:

1. **Total Wiki Edits** (Number)
2. **Hourly Edit Activity** (Line)
3. **Top 10 Human Users** (Bar)
4. **Language Distribution** (Pie)
5. **Top Pages** (Table)

---

## 🔌 Data Sources

**Main source of this project:** [Wikimedia EventStreams](https://stream.wikimedia.org/v2/stream/recentchange)

- Live stream of Wikipedia and sister project edits
- About 40–50 events per second
- Format: Server-Sent Events (SSE) + JSON
- No API Key required

**Alternative sources for practice:**

| Source | Type | API Key Required |
|---|---|---|
| [Coinbase WebSocket](https://docs.cdp.coinbase.com/exchange/docs/websocket-overview) | Cryptocurrency | No |
| [Open-Meteo](https://open-meteo.com/) | Weather | No |
| [AISStream.io](https://aisstream.io/) | Ship positions | Yes (free) |
| [Finnhub](https://finnhub.io/) | Stock market | Yes (free) |

---

## 📚 Documentation

| Document | Description |
|---|---|
| [ClickHouse as a Stream Consumer](docs/clickhouse-consumer.md) | Full guide on using ClickHouse as a Consumer |
| [Medallion Architecture](docs/medallion-architecture.md) | Medallion architecture and design decisions |
| [Metabase Dashboard](docs/metabase-dashboard.md) | Building real-time dashboards with Metabase |

---

## 🛠️ Technologies

| Technology | Version | Purpose |
|---|---|---|
| **Apache Kafka** | 3.9 (KRaft) | Distributed message broker |
| **ClickHouse** | 24.10 | Columnar database |
| **Kafka UI** | latest | Kafka web UI |
| **Metabase** | latest | Business Intelligence / dashboards |
| **Python** | 3.10+ | Producer |
| **Docker** | 24.0+ | Container runtime |
| **kafka-python** | 3.0+ | Python Kafka client |
| **requests** | 2.31+ | Wikimedia SSE client |

---

## 🎯 Concepts You'll Learn

- **Kafka**: Producer, Consumer, Topic, Partition, Consumer Group, KRaft
- **ClickHouse**: MergeTree, ReplacingMergeTree, SummingMergeTree, Kafka Engine, Materialized View
- **Medallion Architecture**: Bronze, Silver, Gold
- **Streaming**: SSE, Backpressure, Idempotency, Exactly-Once
- **Data Engineering**: ETL, Data Pipeline, Orchestration
- **Docker**: Networking, Volumes, Healthcheck, Docker Compose
- **Advanced SQL**: Window Functions, JSON Extraction, Aggregation
- **BI**: Metabase, Dashboards, Questions, Filters

---

## 🧪 Sample Analytical Queries

### Top Human Users

```sql
SELECT user, count() AS edits
FROM tutorial.silver_wiki_events
WHERE user_is_bot = 0 AND user_is_anonymous = 0
GROUP BY user
ORDER BY edits DESC
LIMIT 10;
```

### Bot/Human/Anonymous Ratio

```sql
SELECT
    countIf(user_is_bot = 1) AS bots,
    countIf(user_is_bot = 0 AND user_is_anonymous = 0) AS humans,
    countIf(user_is_anonymous = 1) AS anonymous,
    count() AS total
FROM tutorial.silver_wiki_events;
```

### Language Distribution

```sql
SELECT language, count() AS events
FROM tutorial.silver_wiki_events
GROUP BY language
ORDER BY events DESC
LIMIT 15;
```

### Biggest Page Changes

```sql
SELECT title, user, length_delta, event_timestamp
FROM tutorial.silver_wiki_events
WHERE length_delta > 0
ORDER BY length_delta DESC
LIMIT 10;
```

### Hourly Activity Chart

```sql
SELECT
    hour,
    sum(total_edits) AS edits,
    sum(bot_edits) AS bots,
    sum(human_edits) AS humans
FROM tutorial.gold_wiki_hourly_stats
WHERE hour >= now() - INTERVAL 24 HOUR
GROUP BY hour
ORDER BY hour ASC;
```

---

## 🗺️ Roadmap

- [x] Bronze layer (raw data)
- [x] Silver layer (cleaned data)
- [x] Gold layer (aggregated data)
- [x] Full Medallion Architecture documentation
- [x] Automated setup with Makefile
- [x] Visualization with Metabase
- [ ] Containerize the Producer (Dockerfile)
- [ ] Unit tests with pytest
- [ ] GitHub Actions for CI/CD
- [ ] Alerting with Prometheus + Grafana
- [ ] dbt for Transformation
- [ ] Airflow for Orchestration
- [ ] Deploy to cloud (AWS/GCP)

---

## 🐛 Troubleshooting

| Issue | Solution |
|---|---|
| `Unknown codec family code: 0` | Upgrade to ClickHouse 24.10+ and `make reset` |
| `Connection refused` in Metabase | Use `clickhouse` as Host, not `localhost` |
| `Authentication failed` | Verify credentials: `admin` / `admin123` |
| `No driver for ClickHouse` | Download the driver to `metabase-plugins/` |
| Disk full | Run `docker system prune -a` and check `df -h` |
| Producer stops with `ConnectionAborted` | Restart: it auto-reconnects with exponential backoff |

---

## 🤝 Contributing

If you'd like to contribute:

1. Fork the repo.
2. Create a branch (`git checkout -b feature/amazing-feature`).
3. Commit your changes (`git commit -m 'Add amazing feature'`).
4. Push to the branch (`git push origin feature/amazing-feature`).
5. Open a Pull Request.

---

## 📝 License

This project is released under the **MIT** License. See [LICENSE](LICENSE) for details.

---

## 🙏 Acknowledgements

- [Wikimedia EventStreams](https://stream.wikimedia.org/) for the free live data
- [ClickHouse](https://clickhouse.com/) for the blazing-fast analytical engine
- [Apache Kafka](https://kafka.apache.org/) for the resilient message broker
- [Metabase](https://www.metabase.com/) for the easy-to-use BI platform

---

<div align="center">

**Built with ❤️ for learning data engineering**

</div>
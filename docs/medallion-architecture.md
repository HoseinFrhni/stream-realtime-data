# 🏛️ Medallion Architecture

A comprehensive guide to implementing the Medallion Architecture in the Streaming Data Pipeline project.

**Version:** 1.0.0
**Last Updated:** 2026-10-07
**ClickHouse Version:** 24.8
**Kafka Version:** 3.9 (KRaft mode)

---

## Table of Contents

1. [Introduction to Medallion Architecture](#1-introduction-to-medallion-architecture)
2. [Why Medallion Architecture?](#2-why-medallion-architecture)
3. [Project Architecture Overview](#3-project-architecture-overview)
4. [Bronze Layer](#4-bronze-layer)
5. [Silver Layer](#5-silver-layer)
6. [Gold Layer](#6-gold-layer)
7. [Data Flow Across Layers](#7-data-flow-across-layers)
8. [Design Decisions](#8-design-decisions)
9. [Operational Notes](#9-operational-notes)
10. [References](#10-references)

---

## 1. Introduction to Medallion Architecture

**Medallion Architecture** is a design pattern in data engineering that organizes data into **three sequential layers**. Each layer provides a higher level of **refinement, validation, and business value**.

This architecture was first introduced by **Databricks** and has become an industry standard for building Lakehouses and Data Pipelines.

### The Three Main Layers

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   BRONZE     │───▶│   SILVER     │───▶│    GOLD      │
│   (Raw)      │    │  (Clean)     │    │ (Business)   │
└──────────────┘    └──────────────┘    └──────────────┘

Raw data            Cleaned data         Business data
Exactly as          Validated,           KPIs, reports,
received            enriched             ready for BI/ML
```

### Why "Medallion"?

The word "Medallion" means a **medal** or **badge**. The name comes from the idea that each layer, like a medal, represents a higher level of **value**:

- 🥉 **Bronze**: Lowest business value, but highest detail
- 🥈 **Silver**: Medium value, clean and structured data
- 🥇 **Gold**: Highest business value, ready for decision-making

---

## 2. Why Medallion Architecture?

### Main Benefits

| Benefit | Description |
|---|---|
| **Replayability** | If you change the Silver logic, you can reprocess from Bronze without going back to Kafka. |
| **History Preservation** | Raw data always stays in Bronze. Nothing is lost. |
| **Easy Debugging** | If Silver data has issues, go back to Bronze and find where the transformation went wrong. |
| **Separation of Concerns** | Each layer depends only on the previous one. Changes in one layer don't affect others. |
| **Better Performance** | Gold contains only aggregated data, so queries are very fast. |
| **Standards Compliance** | This architecture is standard in Databricks, Snowflake, BigQuery, etc. |
| **Cost Reduction** | You can define different TTLs for each layer. Bronze expires faster, Gold lives longer. |

### Comparison with the Traditional Approach

**Traditional approach (without Medallion):**
```
Source → ETL → Data Warehouse → BI
```
**Problems:**
- If the ETL is wrong, data is lost.
- You can't reprocess raw data.
- Debugging is hard.

**Medallion approach:**
```
Source → Bronze → Silver → Gold → BI
```
**Benefits:**
- Raw data is always available.
- You can rebuild each layer separately.
- Debugging is easy.

---

## 3. Project Architecture Overview

### Full Diagram

```
┌─────────────────────┐
│  Wikimedia SSE      │  ← Data source (live Wikipedia edit stream)
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Producer.py        │  ← Python + kafka-python
│  (Full JSON event)  │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Kafka: wiki-events │  ← 3 partitions, Replication Factor 1
└──────────┬──────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────┐
│                    ClickHouse                                │
│                                                             │
│  ┌───────────────────────────────────────┐                  │
│  │ wiki_events_queue (Kafka Engine)      │                  │
│  │ - format: JSONAsString                │                  │
│  │ - kafka_num_consumers: 3              │                  │
│  │ - kafka_handle_error_mode: stream     │                  │
│  └────────────┬──────────────────────────┘                  │
│               │                                              │
│               ├── _error=''  → bronze_wiki_events_mv        │
│               │                                              │
│               └── _error!='' → bronze_wiki_events_errors_mv │
│                                                              │
│  ┌────────────────────────────────┐                         │
│  │ 🥉 BRONZE                      │                         │
│  │ ┌──────────────────────────┐   │                         │
│  │ │ bronze_wiki_events       │   │  ← raw_message          │
│  │ │ (MergeTree)              │   │  ← + Kafka metadata     │
│  │ │ TTL: 30 days             │   │                         │
│  │ └────────────┬─────────────┘   │                         │
│  │              │                  │                         │
│  │ ┌────────────▼─────────────┐   │                         │
│  │ │ bronze_wiki_events_errors│   │  ← error messages       │
│  │ │ TTL: 7 days              │   │                         │
│  │ └──────────────────────────┘   │                         │
│  └──────────────┬─────────────────┘                         │
│                 │                                            │
│                 ▼ (JSON parsing)                            │
│  ┌────────────────────────────────┐                         │
│  │ 🥈 SILVER                      │                         │
│  │ ┌──────────────────────────┐   │                         │
│  │ │ silver_wiki_events       │   │  ← extracted fields     │
│  │ │ (ReplacingMergeTree)     │   │  ← validated            │
│  │ │ TTL: 90 days             │   │  ← deduplicated         │
│  │ └────────────┬─────────────┘   │                         │
│  └──────────────┬─────────────────┘                         │
│                 │                                            │
│                 ▼ (aggregation)                             │
│  ┌────────────────────────────────┐                         │
│  │ 🥇 GOLD                        │                         │
│  │ ┌──────────────────────────┐   │                         │
│  │ │ gold_wiki_hourly_stats   │   │  ← hourly stats         │
│  │ │ gold_top_users_daily     │   │  ← top users            │
│  │ │ gold_top_pages_hourly    │   │  ← top pages            │
│  │ │ gold_language_hourly     │   │  ← language distribution│
│  │ │ TTL: 180-365 days        │   │                         │
│  │ └──────────────────────────┘   │                         │
│  └────────────────────────────────┘                         │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │  Metabase/Grafana│
                    │  BI Dashboard    │
                    └──────────────────┘
```

### Tables Summary

| Layer | Table | Engine | TTL | Consumer |
|---|---|---|---|---|
| Bronze | `bronze_wiki_events` | MergeTree | 30 days | Data Engineers |
| Bronze | `bronze_wiki_events_errors` | MergeTree | 7 days | Data Engineers |
| Silver | `silver_wiki_events` | ReplacingMergeTree | 90 days | Analysts |
| Gold | `gold_wiki_hourly_stats` | SummingMergeTree | 1 year | BI |
| Gold | `gold_top_users_daily` | ReplacingMergeTree | 1 year | BI |
| Gold | `gold_top_pages_hourly` | ReplacingMergeTree | 180 days | BI |
| Gold | `gold_language_hourly` | SummingMergeTree | 1 year | BI |

---

## 4. Bronze Layer

### Goal

**Store 100% raw data without any transformation.** This layer is the project's "Source of Truth."

### Design Principles

1. **No field is removed.** The entire JSON event is stored.
2. **No transformation is performed.** No type conversion, no cleaning, no filtering.
3. **Append-only.** Data is never modified or deleted (except by TTL).
4. **Kafka metadata is preserved.** topic, partition, offset, timestamp.

### Tables

#### `bronze_wiki_events`

```sql
CREATE TABLE tutorial.bronze_wiki_events
(
    raw_message     String,           -- Full JSON event
    kafka_topic     String,           -- Topic name
    kafka_partition UInt64,           -- Partition number
    kafka_offset    UInt64,           -- Message offset
    kafka_timestamp DateTime,         -- Kafka ingestion time
    kafka_key       String,           -- Message key
    ingested_at     DateTime DEFAULT now()  -- Bronze ingestion time
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(ingested_at)
ORDER BY (ingested_at, kafka_partition, kafka_offset)
TTL ingested_at + INTERVAL 30 DAY;
```

**Design decisions:**

| Decision | Reason |
|---|---|
| `raw_message String` | Full JSON as a string. No field is lost. |
| `PARTITION BY toYYYYMMDD(ingested_at)` | Daily partitioning by **ingestion** time (not event time). Since Bronze is for debugging, ingestion time matters more. |
| `ORDER BY (ingested_at, kafka_partition, kafka_offset)` | Enables Kafka message tracking and range queries. |
| `TTL 30 days` | Raw data is bulky. 30 days is enough for debugging. |

#### `bronze_wiki_events_errors`

```sql
CREATE TABLE tutorial.bronze_wiki_events_errors
(
    kafka_topic     String,
    kafka_partition UInt64,
    kafka_offset    UInt64,
    raw_message     String,       -- Failed raw message
    error_message   String,       -- Error text
    ingested_at     DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(ingested_at)
ORDER BY (ingested_at, kafka_partition, kafka_offset)
TTL ingested_at + INTERVAL 7 DAY;
```

### Kafka Engine Table

```sql
CREATE TABLE tutorial.wiki_events_queue
(
    raw_message String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-broker:29092',
    kafka_topic_list = 'wiki-events',
    kafka_group_name = 'clickhouse-bronze-consumer-v2',
    kafka_format = 'JSONAsString',         -- ← Full JSON as string
    kafka_num_consumers = 3,
    kafka_handle_error_mode = 'stream';    -- ← Error handling
```

**Important notes:**

- **`JSONAsString`**: Receives the entire JSON as a string. If we used `JSONEachRow`, ClickHouse would parse fields and reject messages if the schema changes.
- **`kafka_handle_error_mode = 'stream'`**: Erroneous messages go to the virtual `_error` column and the stream doesn't stop.
- **`kafka_num_consumers = 3`**: Since the topic has 3 partitions, ClickHouse reads from all 3 in parallel.

### Virtual Columns

When reading from `wiki_events_queue`, these columns are available:

| Column | Type | Description |
|---|---|---|
| `_topic` | String | Topic name |
| `_partition` | UInt64 | Partition number |
| `_offset` | UInt64 | Message offset |
| `_timestamp` | Nullable(DateTime) | Kafka ingestion time |
| `_key` | String | Message key |
| `_error` | String | Error text (if message is broken) |
| `_raw_message` | String | Raw message (if message is broken) |

---

## 5. Silver Layer

### Goal

**Clean, validate, and parse raw data.** Silver is where raw data becomes structured and usable.

### Design Principles

1. **JSON Parsing:** Extract key fields from `raw_message`.
2. **Validation:** Filter out incomplete or invalid events.
3. **Deduplication:** Using `ReplacingMergeTree`.
4. **Enrichment:** Add derived fields (like `language` from `wiki`).
5. **Type Conversion:** Convert strings to DateTime, UInt8, etc.

### Silver Table

```sql
CREATE TABLE tutorial.silver_wiki_events
(
    -- Identifiers
    event_id          UInt64,              -- Event id
    event_uuid        String,              -- meta.id (UUID)

    -- Event type
    event_type        LowCardinality(String),  -- edit, new
    namespace         Int32,               -- 0 = main article

    -- Page
    title             String,
    page_id           UInt64,

    -- User
    user              String,
    user_is_bot       UInt8,               -- 1 if bot
    user_is_anonymous UInt8,               -- 1 if IP

    -- Wiki
    wiki              LowCardinality(String),  -- enwiki, fawiki
    language          LowCardinality(String),  -- en, fa (extracted)
    domain            LowCardinality(String),  -- en.wikipedia.org

    -- Content
    comment           String,              -- Max 500 chars
    is_minor          UInt8,               -- Minor edit
    is_patrolled      UInt8,               -- Patrolled

    -- Changes
    length_old        Int64,
    length_new        Int64,
    length_delta      Int64,               -- new - old
    revision_old      UInt64,
    revision_new      UInt64,

    -- Timestamps
    event_timestamp   DateTime,            -- Event occurrence time
    ingested_at       DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMMDD(event_timestamp)
ORDER BY (event_timestamp, wiki, event_id)
TTL event_timestamp + INTERVAL 90 DAY;
```

**Design decisions:**

| Decision | Reason |
|---|---|
| `ReplacingMergeTree(ingested_at)` | If a duplicate event with the same `event_id` exists, the latest version is kept. |
| `PARTITION BY toYYYYMMDD(event_timestamp)` | Partitioning by **event** time (not ingestion). |
| `ORDER BY (event_timestamp, wiki, event_id)` | Time-based queries and wiki filters become fast. |
| `LowCardinality(String)` | For fields with many duplicate values (wiki, language). |
| `TTL 90 days` | Cleaned data is smaller and useful for medium-term analytics. |

### Materialized View

```sql
CREATE MATERIALIZED VIEW tutorial.silver_wiki_events_mv
TO tutorial.silver_wiki_events AS
SELECT
    JSONExtractUInt(raw_message, 'id') AS event_id,
    JSONExtractString(raw_message, 'meta', 'id') AS event_uuid,
    JSONExtractString(raw_message, 'type') AS event_type,
    JSONExtractInt(raw_message, 'namespace') AS namespace,
    JSONExtractString(raw_message, 'title') AS title,
    JSONExtractUInt(raw_message, 'page_id') AS page_id,
    JSONExtractString(raw_message, 'user') AS user,
    toUInt8(JSONExtractBool(raw_message, 'bot')) AS user_is_bot,
    toUInt8(JSONExtractString(raw_message, 'user') LIKE '~%') AS user_is_anonymous,
    JSONExtractString(raw_message, 'wiki') AS wiki,
    -- Extract language from wiki: enwiki → en, fawiki → fa
    replaceRegexpOne(JSONExtractString(raw_message, 'wiki'), 'wiki$', '') AS language,
    JSONExtractString(raw_message, 'meta', 'domain') AS domain,
    substring(JSONExtractString(raw_message, 'comment'), 1, 500) AS comment,
    toUInt8(JSONExtractBool(raw_message, 'minor')) AS is_minor,
    toUInt8(JSONExtractBool(raw_message, 'patrolled')) AS is_patrolled,
    JSONExtractInt(raw_message, 'length', 'old') AS length_old,
    JSONExtractInt(raw_message, 'length', 'new') AS length_new,
    JSONExtractInt(raw_message, 'length', 'new') - JSONExtractInt(raw_message, 'length', 'old') AS length_delta,
    JSONExtractUInt(raw_message, 'revision', 'old') AS revision_old,
    JSONExtractUInt(raw_message, 'revision', 'new') AS revision_new,
    toDateTime(JSONExtractInt(raw_message, 'timestamp')) AS event_timestamp,
    now() AS ingested_at
FROM tutorial.bronze_wiki_events
WHERE JSONExtractString(raw_message, 'type') IN ('edit', 'new')
  AND JSONExtractString(raw_message, 'title') != ''
  AND JSONExtractString(raw_message, 'user') != '';
```

### Why `language` is Extracted from `wiki`

In Wikimedia data, the `meta.language` field is **empty** (in SSE). The language is in the `wiki` field, which is stored as `enwiki`, `fawiki`, `dewiki`, etc.

**Extraction:**
```sql
replaceRegexpOne('enwiki', 'wiki$', '')  -- → 'en'
replaceRegexpOne('fawiki', 'wiki$', '')  -- → 'fa'
replaceRegexpOne('commonswiki', 'wiki$', '')  -- → 'commons'
```

### Backfill

Since Materialized Views only apply to **new** data, we use this command to migrate historical data:

```sql
INSERT INTO tutorial.silver_wiki_events
SELECT ... FROM tutorial.bronze_wiki_events WHERE ...;
```

---

## 6. Gold Layer

### Goal

**Build KPIs and aggregated statistics for direct BI consumption.** Gold is where data gains real business value.

### Design Principles

1. **Aggregation:** Data is aggregated by time windows (hourly, daily).
2. **Right Engine:** `SummingMergeTree` for numbers, `ReplacingMergeTree` for replacement.
3. **Query-Optimized:** Small tables, fast queries.
4. **Long TTL:** Since data volume is small, longer retention is possible.

### Table 1: `gold_wiki_hourly_stats`

**Business question:** "How active is each wiki per hour?"

```sql
CREATE TABLE tutorial.gold_wiki_hourly_stats
(
    hour                DateTime,
    wiki                LowCardinality(String),
    language            LowCardinality(String),
    total_edits         UInt64,
    bot_edits           UInt64,
    human_edits         UInt64,
    anonymous_edits     UInt64,
    minor_edits         UInt64,
    unique_users        UInt64,
    unique_pages        UInt64,
    total_length_delta  Int64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, wiki)
TTL hour + INTERVAL 365 DAY;
```

### Table 2: `gold_top_users_daily`

**Business question:** "Who are the top users per wiki each day?"

```sql
CREATE TABLE tutorial.gold_top_users_daily
(
    day                 Date,
    wiki                LowCardinality(String),
    user                String,
    user_is_bot         UInt8,
    user_is_anonymous   UInt8,
    edits               UInt64,
    pages_touched       UInt64,
    total_length_delta  Int64,
    first_edit_at       DateTime,
    last_edit_at        DateTime
)
ENGINE = ReplacingMergeTree(last_edit_at)
PARTITION BY toYYYYMM(day)
ORDER BY (day, wiki, user)
TTL day + INTERVAL 365 DAY;
```

### Table 3: `gold_top_pages_hourly`

**Business question:** "Which pages are the hottest each hour?"

```sql
CREATE TABLE tutorial.gold_top_pages_hourly
(
    hour                DateTime,
    wiki                LowCardinality(String),
    title               String,
    edits               UInt64,
    unique_users        UInt64,
    bot_edits           UInt64,
    total_length_delta  Int64,
    last_event_at       DateTime
)
ENGINE = ReplacingMergeTree(last_event_at)
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, wiki, title)
TTL hour + INTERVAL 180 DAY;
```

### Table 4: `gold_language_hourly`

**Business question:** "What's the language distribution of activity each hour?"

```sql
CREATE TABLE tutorial.gold_language_hourly
(
    hour            DateTime,
    language        LowCardinality(String),
    wiki_count      UInt64,
    total_edits     UInt64,
    unique_users    UInt64,
    unique_pages    UInt64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, language)
TTL hour + INTERVAL 365 DAY;
```

### Why `SummingMergeTree` and `ReplacingMergeTree`?

**`SummingMergeTree`** for tables with only numbers:
- When rows with the same key are merged, the numbers are **summed**.
- Example: `gold_wiki_hourly_stats` — if two rows exist for `(hour='12:00', wiki='enwiki')`, `total_edits` is summed.

**`ReplacingMergeTree`** for tables where the latest state matters:
- When rows with the same key are merged, the **latest** version is kept.
- Example: `gold_top_users_daily` — for `(day='2026-10-07', wiki='enwiki', user='Ali')`, only the latest state matters.

---

## 7. Data Flow Across Layers

### Step 1: Wikimedia to Kafka

```
Wikimedia SSE → Producer.py → Kafka topic "wiki-events"
```

- The Producer sends the full JSON event.
- `key = title or meta.id` → preserves event order per page.
- `acks=all` + `enable_idempotence=True` → prevents duplicate sends.

### Step 2: Kafka to Bronze

```
Kafka → wiki_events_queue (Kafka Engine) → bronze_wiki_events
```

- The Kafka Engine Table reads the full JSON as `JSONAsString`.
- Materialized View moves healthy data to `bronze_wiki_events`.
- Broken messages go to `bronze_wiki_events_errors`.

### Step 3: Bronze to Silver

```
bronze_wiki_events → silver_wiki_events_mv → silver_wiki_events
```

- The MV parses the raw data.
- Fields are extracted.
- Validation is performed.
- Language is derived from `wiki`.

### Step 4: Silver to Gold

```
silver_wiki_events → [4 Materialized Views] → [4 Gold Tables]
```

- Each MV performs a specific aggregation.
- Data is stored in Gold tables.

### Step 5: Gold to BI

```
Gold Tables → Metabase/Grafana → Dashboards
```

- BI tools query Gold tables directly.
- Since data is aggregated, queries are very fast.

---

## 8. Design Decisions

### Decision 1: Why `JSONAsString` and not `JSONEachRow`?

**`JSONEachRow`:** ClickHouse parses fields at ingestion. If a field is removed from the event, the message is **rejected**.

**`JSONAsString`:** The entire JSON is stored as a string. Any future field added is available without changes to the Kafka schema.

**Decision:** `JSONAsString` — because Bronze must preserve 100% of the data.

### Decision 2: Why `ReplacingMergeTree` in Silver?

Kafka may deliver messages **duplicates** (At-Least-Once semantics). `ReplacingMergeTree` with `event_id` as key keeps the latest version and merges duplicates in the background.

### Decision 3: Why `PARTITION BY toYYYYMMDD` in Bronze/Silver and `toYYYYMM` in Gold?

- **Bronze/Silver:** High data volume. Daily partitioning is better for TTL and range queries.
- **Gold:** Low data volume. Monthly partitioning reduces the number of partitions and speeds up queries.

### Decision 4: Why `LowCardinality`?

`LowCardinality(String)` is an optimized data type for strings with **many duplicate values**:
- `wiki`: only ~1000 unique values
- `language`: only ~300 unique values
- `event_type`: only 2-3 values

ClickHouse **dictionaries** these values and stores numbers instead of strings. Memory savings: 5-10x.

### Decision 5: Why Different TTLs?

| Layer | TTL | Reason |
|---|---|---|
| Bronze | 30 days | Raw data is bulky. Enough for debugging. |
| Bronze Errors | 7 days | Errors should be investigated quickly. |
| Silver | 90 days | Cleaned data for medium-term analytics. |
| Gold Hourly | 1 year | For long-term trends. |
| Gold Pages | 180 days | Page-level data is bulkier. |

### Decision 6: Why MV over `INSERT INTO ... SELECT`?

MV **works automatically**: whenever new data is added to the source table, the MV forwards it to the target table. This means:
- No Cron Jobs needed.
- Low latency.
- Less code, fewer errors.

However, for **historical data**, a one-time `INSERT INTO ... SELECT` (Backfill) is needed.

---

## 9. Operational Notes

### 1. Never SELECT Directly from the Kafka Table

```sql
-- ❌ Wrong
SELECT * FROM tutorial.wiki_events_queue;
```

This **advances the Offset** and pulls data away from Materialized Views. For debugging, use:

```sql
SET stream_like_engine_allow_direct_select = 1;
SELECT * FROM tutorial.wiki_events_queue LIMIT 5;
```

### 2. Monitor the Kafka Consumer

```sql
SELECT
    database,
    table,
    consumer_id,
    assignments,
    num_messages_read,
    last_poll_time,
    last_exception
FROM system.kafka_consumers;
```

### 3. Check Data Volume per Table

```sql
SELECT
    table,
    formatReadableSize(sum(bytes_on_disk)) AS size,
    sum(rows) AS total_rows
FROM system.parts
WHERE database = 'tutorial' AND active
GROUP BY table
ORDER BY sum(bytes_on_disk) DESC;
```

### 4. Optimize Duplicate Rows

`ReplacingMergeTree` merges duplicate rows in the background. To merge immediately:

```sql
OPTIMIZE TABLE tutorial.silver_wiki_events FINAL;
```

> ⚠️ This is heavy. Not recommended in Production.

### 5. Manual Backfill

If you want to resend historical Silver data to Gold:

```sql
-- 1. Truncate the Gold table
TRUNCATE TABLE tutorial.gold_wiki_hourly_stats;

-- 2. Backfill
INSERT INTO tutorial.gold_wiki_hourly_stats
SELECT ... FROM tutorial.silver_wiki_events ...;
```

### 6. Check Errors

```sql
SELECT
    substring(error_message, 1, 100) AS error_type,
    count() AS cnt
FROM tutorial.bronze_wiki_events_errors
GROUP BY error_type
ORDER BY cnt DESC;
```

### 7. Delete Old Data (Before Auto TTL)

```sql
ALTER TABLE tutorial.bronze_wiki_events
DELETE WHERE ingested_at < now() - INTERVAL 7 DAY;
```

---

## 10. References

### Official Documentation

- [Databricks Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [ClickHouse Kafka Engine](https://clickhouse.com/docs/en/engines/table-engines/integrations/kafka)
- [ClickHouse MergeTree](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree)
- [ClickHouse Materialized View](https://clickhouse.com/docs/en/sql-reference/statements/create/view#materialized-view)
- [Kafka Documentation](https://kafka.apache.org/documentation/)

### Related Articles

- [Building a Data Lakehouse with Medallion Architecture](https://www.databricks.com/blog/2021/08/30/building-a-data-lakehouse-with-medallion-architecture.html)
- [Streaming Data into ClickHouse with Kafka](https://clickhouse.com/blog/clickhouse-kafka-engine-tutorial)

### Similar Projects

- [ClickHouse Kafka Examples](https://github.com/ClickHouse/clickhouse-kafka-examples)
- [Kafka Connect ClickHouse Sink](https://github.com/ClickHouse/clickhouse-kafka-connect)

---

## Appendix: Quick Setup Checklist

- [ ] Bring up infrastructure with `docker compose up -d`
- [ ] Create `wiki-events` topic in Kafka
- [ ] Run `sql/01-bronze-layer.sql`
- [ ] Run `sql/02-silver-layer.sql`
- [ ] Run `sql/03-gold-layer.sql`
- [ ] Run the Producer (`python producer.py`)
- [ ] Check `SELECT count() FROM tutorial.bronze_wiki_events`
- [ ] Check `SELECT count() FROM tutorial.silver_wiki_events`
- [ ] Check `SELECT count() FROM tutorial.gold_wiki_hourly_stats`
- [ ] Run Gold analytical queries
- [ ] Connect Metabase to ClickHouse

---

## License

This document is part of the [stream-realtime-data](https://github.com/YOUR-USERNAME/stream-realtime-data) project.
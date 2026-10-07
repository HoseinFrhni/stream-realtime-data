# ClickHouse as a Stream Consumer

A comprehensive guide on using ClickHouse as a data stream consumer in a Kafka → ClickHouse pipeline.

**Version:** 1.0.0
**Last Updated:** 2026-10-06
**ClickHouse Version:** 24.8
**Kafka Version:** 3.7 (KRaft mode)

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Why ClickHouse as a Consumer?](#2-why-clickhouse-as-a-consumer)
3. [The Five-Table Pattern](#3-the-five-table-pattern)
4. [Prerequisites](#4-prerequisites)
5. [Infrastructure Setup](#5-infrastructure-setup)
6. [Connecting to the Kafka Network](#6-connecting-to-the-kafka-network)
7. [Table Structure](#7-table-structure)
8. [Kafka Engine Settings](#8-kafka-engine-settings)
9. [Error Handling](#9-error-handling)
10. [User Access Management](#10-user-access-management)
11. [Analytical Examples](#11-analytical-examples)
12. [Debugging and Monitoring](#12-debugging-and-monitoring)
13. [Optimization and Scalability](#13-optimization-and-scalability)
14. [Common Errors and Solutions](#14-common-errors-and-solutions)
15. [Operational Notes](#15-operational-notes)
16. [References](#16-references)

---

## 1. Architecture Overview

In this project, ClickHouse plays the role of **Consumer** in an event-driven architecture. Data flows from the live Wikimedia source to Kafka via a Python Producer, and ClickHouse reads it directly without any middleware, storing it in optimized tables.

### Architecture Diagram

```
┌──────────────────┐     ┌─────────────────┐     ┌──────────────────────────┐
│  Wikimedia SSE   │────▶│  Producer.py    │────▶│  Kafka: wiki-events      │
└──────────────────┘     └─────────────────┘     └────────────┬─────────────┘
                                                              │
                                                              │ Stream Consumption
                                                              ▼
                                          ┌───────────────────────────────────────┐
                                          │           ClickHouse                  │
                                          │  ┌─────────────────────────────────┐  │
                                          │  │ wiki_events_queue (Kafka Engine)│  │
                                          │  └────┬──────────────────────┬─────┘  │
                                          │       │ _error=''            │ _error!='' │
                                          │       ▼                      ▼      │
                                          │  wiki_events_mv      wiki_events_errors_mv │
                                          │       │                      │      │
                                          │       ▼                      ▼      │
                                          │  wiki_events          wiki_events_errors │
                                          │  (MergeTree)          (MergeTree)   │
                                          └───────────────────────────────────────┘
                                                              │
                                                              ▼
                                                   ┌────────────────────┐
                                                   │  DBeaver / BI      │
                                                   └────────────────────┘
```

### Step-by-Step Data Flow

1. **Producer** connects to the Wikimedia SSE stream.
2. Events are converted to JSON and sent to the `wiki-events` topic in Kafka.
3. The **Kafka Engine table** in ClickHouse automatically reads these messages.
4. The **main Materialized View** moves healthy messages to the `wiki_events` table.
5. The **error Materialized View** moves broken messages to the `wiki_events_errors` table.
6. BI tools (like DBeaver) query the `wiki_events` table.

---

## 2. Why ClickHouse as a Consumer?

ClickHouse with the **Kafka Engine** can connect directly to Kafka and read data as a stream. Benefits:

| Benefit | Description |
|---|---|
| **No Middleware** | No need for a custom Consumer in Python/Java. |
| **High Throughput** | Can consume millions of events per second. |
| **Columnar Storage** | Data is stored compressed and optimized for analytics. |
| **Built-in Error Handling** | Broken messages go to a separate table and the stream doesn't stop. |
| **SQL Integration** | Fast, standard-SQL analytical queries. |
| **Horizontal Scalability** | Throughput scales with partitions and consumers. |

---

## 3. The Five-Table Pattern

ClickHouse uses the **Five-Table Pattern** for consuming Kafka with error handling. Each table has a specific role:

| Table | Engine | Role |
|---|---|---|
| `wiki_events_queue` | `Kafka` | Kafka connection interface. Reads data but doesn't store it. |
| `wiki_events_mv` | `Materialized View` | Moves healthy messages (`_error=''`) to the main table. |
| `wiki_events` | `MergeTree` | Persistent storage for healthy data. |
| `wiki_events_errors_mv` | `Materialized View` | Moves erroneous messages (`_error!=''`) to the error table. |
| `wiki_events_errors` | `MergeTree` | Persistent storage for broken messages for review and debugging. |

### Data Flow

```
Kafka Topic → wiki_events_queue
                   ├── _error=''  → wiki_events_mv  → wiki_events
                   └── _error!='' → wiki_events_errors_mv → wiki_events_errors
```

> ⚠️ **Critical note:** Never query `wiki_events_queue` directly — it advances the Offset and pulls data away from the Materialized Views.

---

## 4. Prerequisites

| Tool | Recommended Version | Purpose |
|---|---|---|
| Docker | 24.0+ | Run containers |
| Docker Compose | 2.20+ | Manage services |
| Kafka | 3.7+ | Message broker |
| ClickHouse | 24.8+ | Consumer and storage |
| Python | 3.10+ | Run the Producer |
| DBeaver | 23.0+ | Queries and analytics |
| Git Bash / WSL | - | Run commands on Windows |

---

## 5. Infrastructure Setup

### 5.1. Docker Compose

```yaml
services:
  clickhouse:
    image: clickhouse/clickhouse-server:24.8
    container_name: clickhouse
    hostname: clickhouse
    ports:
      - "8123:8123"   # HTTP interface
      - "9000:9000"   # Native TCP interface
    environment:
      CLICKHOUSE_DB: tutorial
      CLICKHOUSE_USER: default
      CLICKHOUSE_PASSWORD: ""
    volumes:
      - clickhouse-volume:/var/lib/clickhouse
      - ./init-db:/docker-entrypoint-initdb.d
    networks:
      - services

  kafka-broker:
    image: apache/kafka:3.9.0
    container_name: kafka-broker
    hostname: kafka-broker
    ports:
      - "9092:9092"
      - "29092:29092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka-broker:29093'
      KAFKA_LISTENERS: 'PLAINTEXT://:29092,CONTROLLER://:29093,EXTERNAL://:9092'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://kafka-broker:29092,EXTERNAL://localhost:9092'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT'
      KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT'
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_LOG_DIRS: '/tmp/kraft-combined-logs'
    healthcheck:
      test: nc -z localhost 9092 || exit -1
      interval: 5s
      timeout: 10s
      retries: 10
    networks:
      - services

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    ports:
      - "8082:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka-broker:29092
    depends_on:
      kafka-broker:
        condition: service_healthy
    networks:
      - services

volumes:
  clickhouse-volume:

networks:
  services:
    name: service_network
```

### 5.2. Docker Run (Alternative)

```bash
docker run -d \
  --name clickhouse \
  --hostname clickhouse \
  -p 8123:8123 -p 9000:9000 \
  -e CLICKHOUSE_DB=tutorial \
  -e CLICKHOUSE_USER=default \
  -e CLICKHOUSE_PASSWORD="" \
  -v clickhouse-volume:/var/lib/clickhouse \
  clickhouse/clickhouse-server:24.8
```

### 5.3. Client Access

```bash
docker exec -it clickhouse clickhouse-client
```

Or with a specific user:

```bash
docker exec -it clickhouse clickhouse-client --user default --password ""
```

---

## 6. Connecting to the Kafka Network

If ClickHouse and Kafka are not on the same Docker network, ClickHouse cannot resolve the name `kafka-broker` and will fail with a `DNS resolution failed` error.

### Connection Steps

```bash
# 1. Find the Kafka network name
docker inspect kafka-broker --format "{{json .NetworkSettings.Networks}}"

# 2. Connect ClickHouse to that network
docker network connect kafka-lab_default clickhouse

# 3. Restart ClickHouse
docker restart clickhouse

# 4. Verify DNS
docker exec -it clickhouse getent hosts kafka-broker
```

### Expected Output

```
172.20.0.2      kafka-broker
```

### Verify ClickHouse Networks

```bash
docker inspect clickhouse --format "{{json .NetworkSettings.Networks}}"
```

You should see two networks: its own and Kafka's.

---

## 7. Table Structure

### 7.1. Kafka Engine Table (Ingestion Interface with Error Handling)

```sql
CREATE TABLE IF NOT EXISTS tutorial.wiki_events_queue (
    title String,
    user String,
    bot UInt8,
    timestamp DateTime,
    comment String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-broker:29092',
    kafka_topic_list = 'wiki-events',
    kafka_group_name = 'clickhouse-consumer-group',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';
```

> 📌 **Important:** `kafka_handle_error_mode = 'stream'` exposes two virtual columns, `_error` and `_raw_message`, to Materialized Views.

### 7.2. MergeTree Table (Healthy Data Storage)

```sql
CREATE TABLE IF NOT EXISTS tutorial.wiki_events (
    title String,
    user String,
    bot UInt8,
    timestamp DateTime,
    comment String,
    ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, title);
```

### 7.3. Main Materialized View (Healthy Data Transfer)

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS tutorial.wiki_events_mv
TO tutorial.wiki_events AS
SELECT
    title,
    user,
    bot,
    timestamp,
    comment
FROM tutorial.wiki_events_queue
WHERE length(_error) = 0;
```

### 7.4. Errors Table (Broken Message Storage)

```sql
CREATE TABLE IF NOT EXISTS tutorial.wiki_events_errors (
    _topic String,
    _partition UInt64,
    _offset UInt64,
    _raw_message String,
    _error String,
    ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (ingested_at, _topic, _partition, _offset);
```

### 7.5. Errors Materialized View

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS tutorial.wiki_events_errors_mv
TO tutorial.wiki_events_errors AS
SELECT
    _topic,
    _partition,
    _offset,
    _raw_message,
    _error,
    now() AS ingested_at
FROM tutorial.wiki_events_queue
WHERE length(_error) > 0;
```

### 7.6. Verify Structure

```sql
SHOW TABLES FROM tutorial;
```

**Expected output:**

```
wiki_events
wiki_events_errors
wiki_events_errors_mv
wiki_events_mv
wiki_events_queue
```

### 7.7. Save as Init File (Optional)

You can place all the commands above in `init-db/001-schema.sql` so they run automatically on the first ClickHouse launch.

> ⚠️ **Note:** ClickHouse runs init scripts only **once** (when the volume is empty). To run them again:
> ```bash
> docker compose down -v
> docker compose up -d
> ```

---

## 8. Kafka Engine Settings

### Full Settings Table

| Setting | Recommended Value | Description |
|---|---|---|
| `kafka_broker_list` | `kafka-broker:29092` | Kafka address. Inside Docker we use the service name. |
| `kafka_topic_list` | `wiki-events` | Topic name. |
| `kafka_group_name` | `clickhouse-consumer-group` | Consumer Group. |
| `kafka_format` | `JSONEachRow` | Input data format. |
| `kafka_num_consumers` | `1` | Number of parallel consumers. |
| `kafka_handle_error_mode` | `stream` | Streaming error handling. |
| `kafka_skip_broken_messages` | `10` | (Optional) Invalid messages before failure. |
| `kafka_max_block_size` | `65536` | (Optional) Max messages per batch. |
| `kafka_poll_max_batch_size` | `1000` | (Optional) Max messages per poll. |
| `kafka_poll_timeout_ms` | `500` | (Optional) Poll wait time. |
| `kafka_flush_interval_ms` | `7500` | (Optional) Auto flush interval. |

### Advanced Configuration Example

```sql
CREATE TABLE IF NOT EXISTS tutorial.wiki_events_queue (
    title String,
    user String,
    bot UInt8,
    timestamp DateTime,
    comment String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-broker:29092',
    kafka_topic_list = 'wiki-events',
    kafka_group_name = 'clickhouse-consumer-group',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 3,
    kafka_handle_error_mode = 'stream',
    kafka_skip_broken_messages = 10,
    kafka_max_block_size = 65536,
    kafka_poll_max_batch_size = 1000,
    kafka_poll_timeout_ms = 500;
```

### ⚠️ Important Limitation: ALTER Settings

**The Kafka engine does not support `ALTER TABLE ... MODIFY SETTING`.** To change a setting, you must **drop and recreate** the table:

```sql
-- 1. Drop dependent MVs
DROP VIEW IF EXISTS tutorial.wiki_events_mv;
DROP VIEW IF EXISTS tutorial.wiki_events_errors_mv;

-- 2. Drop the Kafka table
DROP TABLE IF EXISTS tutorial.wiki_events_queue;

-- 3. Recreate with new settings
CREATE TABLE tutorial.wiki_events_queue (...) ENGINE = Kafka SETTINGS ...;

-- 4. Recreate MVs
CREATE MATERIALIZED VIEW tutorial.wiki_events_mv ...;
CREATE MATERIALIZED VIEW tutorial.wiki_events_errors_mv ...;
```

> ✅ **Note:** Since the Kafka table doesn't store data, dropping and recreating it **loses no data**. Only a few seconds of data may be lost during the rebuild.

---

## 9. Error Handling

### Why Error Handling Matters

In a stream, there's always a chance of broken or incompatible messages. Common causes:

- **Partial connection:** Incomplete JSON.
- **Missing fields:** Events with different structure.
- **Wrong data types:** E.g., `bot` is a string instead of a number.
- **Oversized messages:** Beyond the size limit.

Without error handling, **the entire Consumer halts** and the pipeline breaks.

### `kafka_handle_error_mode` Modes

| Mode | Behavior | Recommendation |
|---|---|---|
| `default` | On error, the entire consumer halts. | ❌ Not recommended |
| `stream` | Errors go to virtual `_error` column and the stream continues. | ✅ Recommended |
| `dead_letter_queue` | (Only ClickHouse 25.8+) Broken messages go to `system.dead_letter_queue`. | ⭐ Advanced option |

### Virtual Columns

When `kafka_handle_error_mode = 'stream'` is enabled, these columns are available automatically:

| Column | Type | Description |
|---|---|---|
| `_error` | `String` | Error text. Empty if the message is healthy. |
| `_raw_message` | `String` | Raw Kafka message (unparsed). |
| `_topic` | `String` | Topic name. |
| `_partition` | `UInt64` | Partition number. |
| `_offset` | `UInt64` | Message offset. |
| `_key` | `String` | Message key. |
| `_timestamp` | `Nullable(DateTime)` | Kafka ingestion time. |

### Check Recorded Errors

```sql
SELECT
    _topic,
    _partition,
    _offset,
    _error,
    substring(_raw_message, 1, 300) AS raw_preview,
    ingested_at
FROM tutorial.wiki_events_errors
ORDER BY ingested_at DESC
LIMIT 10;
```

### Group Errors by Type

```sql
SELECT
    substring(_error, 1, 100) AS error_type,
    count() AS cnt
FROM tutorial.wiki_events_errors
GROUP BY error_type
ORDER BY cnt DESC;
```

### Error Trend Chart (Last 24 Hours)

```sql
SELECT
    toStartOfHour(ingested_at) AS hour,
    count() AS error_count
FROM tutorial.wiki_events_errors
WHERE ingested_at > now() - INTERVAL 24 HOUR
GROUP BY hour
ORDER BY hour DESC;
```

### TTL for the Errors Table

To prevent space being taken up by old broken messages:

```sql
ALTER TABLE tutorial.wiki_events_errors
MODIFY TTL ingested_at + INTERVAL 7 DAY;
```

### Manual Error Cleanup

```sql
TRUNCATE TABLE tutorial.wiki_events_errors;
```

---

## 10. User Access Management

### Why a New User?

The `default` user in ClickHouse uses `users.xml`, which is **read-only**. If you try to set a password, you'll get an `ACCESS_STORAGE_READONLY` error.

### Solution: Create a New User

**Step 1: Enable `access_management` for `default`**

Create `default-access.xml` in `users.d`:

```bash
docker exec -it clickhouse bash -c "cat > /etc/clickhouse-server/users.d/default-access.xml << 'EOF'
<clickhouse>
    <users>
        <default>
            <access_management>1</access_management>
            <named_collection_control>1</named_collection_control>
        </default>
    </users>
</clickhouse>
EOF"
```

**Step 2: Restart ClickHouse**

```bash
docker restart clickhouse
```

**Step 3: Create a New User**

```bash
docker exec -it clickhouse clickhouse-client --query "CREATE USER dbeaver_user IDENTIFIED WITH plaintext_password BY 'dbeaver123'"
```

**Step 4: Grant Privileges**

```bash
docker exec -it clickhouse clickhouse-client --query "GRANT CURRENT GRANTS ON *.* TO dbeaver_user WITH GRANT OPTION"
```

> 📌 **Note:** If `GRANT ALL` fails with `Not enough privileges`, use `GRANT CURRENT GRANTS`. This command transfers all of `default`'s privileges.

**Step 5: Verify Privileges**

```sql
SHOW GRANTS FOR dbeaver_user;
```

**Step 6: Test from CLI**

```bash
docker exec -it clickhouse clickhouse-client --user dbeaver_user --password dbeaver123 --query "SELECT currentUser()"
```

**Expected output:**
```
dbeaver_user
```

### Connecting from DBeaver

| Field | Value |
|---|---|
| **Host** | `localhost` |
| **Port** | `8123` |
| **Database** | `tutorial` |
| **User** | `dbeaver_user` |
| **Password** | `dbeaver123` |

### Set Password for `default` (Optional)

Now that `access_management` is enabled:

```bash
docker exec -it clickhouse clickhouse-client --query "ALTER USER default IDENTIFIED WITH plaintext_password BY 'tutorial123'"
```

---

## 11. Analytical Examples

### Total Events

```sql
SELECT count() AS total_events FROM tutorial.wiki_events;
```

### Top Users

```sql
SELECT user, count() AS edits
FROM tutorial.wiki_events
GROUP BY user
ORDER BY edits DESC
LIMIT 10;
```

### Bot-to-Human Ratio

```sql
SELECT
    bot,
    count() AS cnt,
    round(count() * 100.0 / sum(count()) OVER (), 2) AS pct
FROM tutorial.wiki_events
GROUP BY bot;
```

### Most Edited Pages

```sql
SELECT title, count() AS edits
FROM tutorial.wiki_events
GROUP BY title
ORDER BY edits DESC
LIMIT 10;
```

### Activity Time Chart (Per Minute)

```sql
SELECT
    toStartOfMinute(timestamp) AS minute,
    count() AS edits
FROM tutorial.wiki_events
GROUP BY minute
ORDER BY minute DESC
LIMIT 20;
```

### Registered Users (Without IP)

```sql
SELECT user, count() AS edits
FROM tutorial.wiki_events
WHERE user NOT LIKE '~%'
GROUP BY user
ORDER BY edits DESC
LIMIT 10;
```

### Users with Only Bot Edits

```sql
SELECT user, count() AS edits
FROM tutorial.wiki_events
WHERE bot = 1
GROUP BY user
ORDER BY edits DESC
LIMIT 5;
```

### Ingestion Rate (Events per Second)

```sql
SELECT
    toStartOfSecond(ingested_at) AS second,
    count() AS events
FROM tutorial.wiki_events
WHERE ingested_at > now() - INTERVAL 1 MINUTE
GROUP BY second
ORDER BY second DESC;
```

### Processing Latency (Event Time to Ingestion)

```sql
SELECT
    round(avg(dateDiff('second', timestamp, ingested_at)), 2) AS avg_lag_seconds,
    round(max(dateDiff('second', timestamp, ingested_at)), 2) AS max_lag_seconds
FROM tutorial.wiki_events
WHERE ingested_at > now() - INTERVAL 5 MINUTE;
```

---

## 12. Debugging and Monitoring

### View ClickHouse Logs

```bash
docker logs clickhouse --tail 50
```

### View ClickHouse Error Logs

```bash
docker exec -it clickhouse tail -50 /var/log/clickhouse-server/clickhouse-server.err.log
```

### Kafka Consumer Status

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

**Important columns:**
- `assignments`: Assigned partitions
- `last_poll_time`: Last poll time
- `num_messages_read`: Number of messages read
- `last_exception`: Last exception (if any)

### Check Consumer Lag

```sql
SELECT
    database,
    table,
    consumer_id,
    num_messages_read,
    last_poll_time,
    now() - last_poll_time AS time_since_last_poll
FROM system.kafka_consumers;
```

### Check MV Health

```sql
SELECT
    database,
    view_name,
    dependencies_database,
    dependencies_table
FROM system.view_refreshes;
```

### Direct Read from Kafka Table (Debug Only)

```sql
SET stream_like_engine_allow_direct_select = 1;
SELECT * FROM tutorial.wiki_events_queue LIMIT 5;
```

> ⚠️ **Warning:** This advances the Offset and pulls data away from MVs. Development use only.

### Table Statistics

```sql
SELECT
    table,
    sum(rows) AS total_rows,
    formatReadableSize(sum(bytes_on_disk)) AS size_on_disk
FROM system.parts
WHERE database = 'tutorial' AND active
GROUP BY table;
```

---

## 13. Optimization and Scalability

### Increase the Number of Consumers

If the topic has multiple partitions, increase the number of consumers. **Requires rebuilding the table:**

```sql
-- 1. Drop dependent MVs
DROP VIEW IF EXISTS tutorial.wiki_events_mv;
DROP VIEW IF EXISTS tutorial.wiki_events_errors_mv;

-- 2. Drop the Kafka table
DROP TABLE IF EXISTS tutorial.wiki_events_queue;

-- 3. Rebuild with more consumers
CREATE TABLE tutorial.wiki_events_queue (...)
ENGINE = Kafka
SETTINGS
    ...
    kafka_num_consumers = 3,
    kafka_handle_error_mode = 'stream';

-- 4. Rebuild MVs
CREATE MATERIALIZED VIEW tutorial.wiki_events_mv ...;
CREATE MATERIALIZED VIEW tutorial.wiki_events_errors_mv ...;
```

> 📌 **Rule:** Number of consumers should not exceed the number of partitions.

### Change the Ordering Key

If queries are mostly by `user`:

```sql
ALTER TABLE tutorial.wiki_events
MODIFY ORDER BY (user, timestamp);
```

> ⚠️ This has limitations on existing tables with data. Best practice is to create a new table and migrate data.

### TTL on the Main Table

```sql
ALTER TABLE tutorial.wiki_events
MODIFY TTL toDateTime(timestamp) + INTERVAL 30 DAY;
```

### Add a Projection (for Recurring Queries)

```sql
ALTER TABLE tutorial.wiki_events
ADD PROJECTION user_stats (
    SELECT user, count() GROUP BY user
);
```

### Increase Kafka Retention

If you want data to stay longer in Kafka:

```bash
docker exec -it kafka-broker /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server localhost:29092 \
  --alter --entity-type topics --entity-name wiki-events \
  --add-config retention.ms=604800000
```

(7 days = 604800000 ms)

---

## 14. Common Errors and Solutions

| Error | Cause | Solution |
|---|---|---|
| `DNS resolution failed` | ClickHouse and Kafka not on the same network. | `docker network connect kafka-lab_default clickhouse` |
| `UnknownTopicOrPartition` | Topic not created. | Create the topic in Kafka UI. |
| `Authentication failed` | Wrong user/password. | Create `dbeaver_user`. |
| `Cannot alter settings` | Kafka engine doesn't support ALTER. | Drop and recreate the table. |
| `CANNOT_READ_FROM_FILE_DESCRIPTOR` | Init file empty or a directory. | Check the SQL file. |
| `Direct select is not allowed` | Direct SELECT from Kafka Engine. | Use MV or `stream_like_engine_allow_direct_select=1`. |
| `ACCESS_STORAGE_READONLY` | Trying to modify `default` user. | Create a new user. |
| `ACCESS_DENIED` | User lacks required privileges. | Use `GRANT CURRENT GRANTS`. |
| `Connection refused (localhost:9000)` | ClickHouse not ready yet. | Wait a few seconds and retry. |
| `Only RowBinaryWithNameAndTypes...` | SELECT from Kafka table without proper FORMAT. | Use `FORMAT JSONEachRow`. |

---

## 15. Operational Notes

### 1. Never SELECT Directly from the Kafka Table

It advances the Offset and MVs lose data.

### 2. Match Consumer Count with Partition Count

If you have 3 partitions and 1 consumer, two partitions are idle.

### 3. Monitor Consumer Lag

```sql
SELECT
    database,
    table,
    consumer_id,
    num_messages_read,
    last_poll_time
FROM system.kafka_consumers;
```

### 4. Don't Change the Consumer Group

Changing `kafka_group_name` causes reading from the beginning and storing duplicate data.

### 5. Take MergeTree Partitioning Seriously

`PARTITION BY toYYYYMM(timestamp)` is great for monthly data. If your daily data volume is high, use `toYYYYMMDD(timestamp)`.

### 6. Monitor the Errors Table

If error counts increase, the Producer or data structure has issues.

### 7. Don't Ignore `ingested_at`

This column tells you when data entered ClickHouse, which differs from `timestamp` (event time). Great for Lag calculation and debugging.

### 8. Take Backups

```sql
BACKUP TABLE tutorial.wiki_events TO Disk('backups', 'wiki_events_backup.zip');
```

### 9. Clear the Init File After Every Volume Change

```bash
docker compose down -v
docker compose up -d
```

### 10. Don't Use the `default` User in Production

Create `dbeaver_user` with a clear password and use it in DBeaver and the Producer.

---

## 16. References

- [ClickHouse Kafka Engine Documentation](https://clickhouse.com/docs/en/engines/table-engines/integrations/kafka)
- [ClickHouse MergeTree Documentation](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree)
- [ClickHouse Materialized View Documentation](https://clickhouse.com/docs/en/sql-reference/statements/create/view#materialized-view)
- [Kafka Producer Configuration](https://kafka.apache.org/documentation/#producerconfigs)
- [Kafka KRaft Mode Documentation](https://kafka.apache.org/documentation/#kraft)

---

## Appendix: Quick Setup Checklist

- [ ] Bring up `docker-compose.yaml` with ClickHouse, Kafka, Kafka UI
- [ ] Connect ClickHouse to the Kafka network (`docker network connect`)
- [ ] Create the `tutorial` database
- [ ] Create the `wiki_events_queue` table with `kafka_handle_error_mode = 'stream'`
- [ ] Create the `wiki_events` table
- [ ] Create the main MV `wiki_events_mv`
- [ ] Create the `wiki_events_errors` table
- [ ] Create the errors MV `wiki_events_errors_mv`
- [ ] Create the `wiki-events` topic in Kafka
- [ ] Create `dbeaver_user` and configure `access_management`
- [ ] Connect DBeaver with `dbeaver_user`
- [ ] Run the Producer (`python producer.py`)
- [ ] Check `SELECT count() FROM tutorial.wiki_events`
- [ ] Check `SELECT count() FROM tutorial.wiki_events_errors`
- [ ] Run analytical queries

---

## License

This document is part of the [streaming-data-pipeline](https://github.com/your-username/streaming-data-pipeline) project.
-- ============================================================================
-- BRONZE LAYER - Raw Data Storage
-- ============================================================================
-- این فایل، لایه‌ی Bronze معماری مدالیون را می‌سازد.
--
-- Bronze = داده‌ی خام ۱۰۰٪ بدون دست‌کاری
-- منبع: Kafka topic "wiki-events"
-- فرمت: JSONAsString (کل JSON به صورت رشته)
--
-- اجرا در: ClickHouse (dbeaver / clickhouse-client)
-- ترتیب: این فایل را اول اجرا کنید (قبل از Silver و Gold)
-- ============================================================================

-- ⚠️ پاک‌سازی (اختیاری - فقط اگر می‌خواهید از صفر شروع کنید)
-- DROP VIEW IF EXISTS tutorial.bronze_wiki_events_mv;
-- DROP VIEW IF EXISTS tutorial.bronze_wiki_events_errors_mv;
-- DROP TABLE IF EXISTS tutorial.wiki_events_queue;
-- DROP TABLE IF EXISTS tutorial.bronze_wiki_events;
-- DROP TABLE IF EXISTS tutorial.bronze_wiki_events_errors;

-- ============================================================================
-- ۱. Kafka Engine Table (رابط دریافت از Kafka)
-- ============================================================================
CREATE TABLE IF NOT EXISTS tutorial.wiki_events_queue
(
    raw_message String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-broker:29092',
    kafka_topic_list = 'wiki-events',
    kafka_group_name = 'clickhouse-bronze-consumer-v2',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 3,
    kafka_handle_error_mode = 'stream';

-- ============================================================================
-- ۲. جدول Bronze (ذخیره‌ی داده‌ی خام)
-- ============================================================================
CREATE TABLE IF NOT EXISTS tutorial.bronze_wiki_events
(
    raw_message     String,
    kafka_topic     String,
    kafka_partition UInt64,
    kafka_offset    UInt64,
    kafka_timestamp DateTime,
    kafka_key       String,
    ingested_at     DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(ingested_at)
ORDER BY (ingested_at, kafka_partition, kafka_offset)
TTL ingested_at + INTERVAL 30 DAY;

-- ============================================================================
-- ۳. Materialized View (انتقال خودکار داده‌های سالم)
-- ============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS tutorial.bronze_wiki_events_mv
TO tutorial.bronze_wiki_events AS
SELECT
    raw_message,
    _topic     AS kafka_topic,
    _partition AS kafka_partition,
    _offset    AS kafka_offset,
    _timestamp AS kafka_timestamp,
    _key       AS kafka_key,
    now()      AS ingested_at
FROM tutorial.wiki_events_queue
WHERE length(_error) = 0;

-- ============================================================================
-- ۴. جدول خطاها (پیام‌های خراب)
-- ============================================================================
CREATE TABLE IF NOT EXISTS tutorial.bronze_wiki_events_errors
(
    kafka_topic     String,
    kafka_partition UInt64,
    kafka_offset    UInt64,
    raw_message     String,
    error_message   String,
    ingested_at     DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(ingested_at)
ORDER BY (ingested_at, kafka_partition, kafka_offset)
TTL ingested_at + INTERVAL 7 DAY;

-- ============================================================================
-- ۵. Materialized View خطاها
-- ============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS tutorial.bronze_wiki_events_errors_mv
TO tutorial.bronze_wiki_events_errors AS
SELECT
    _topic       AS kafka_topic,
    _partition   AS kafka_partition,
    _offset      AS kafka_offset,
    _raw_message AS raw_message,
    _error       AS error_message,
    now()        AS ingested_at
FROM tutorial.wiki_events_queue
WHERE length(_error) > 0;

-- ============================================================================
-- بررسی صحت ساختار
-- ============================================================================
-- SHOW TABLES FROM tutorial LIKE 'bronze%';
-- SELECT count() FROM tutorial.bronze_wiki_events;
-- SELECT count() FROM tutorial.bronze_wiki_events_errors;
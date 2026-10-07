-- ============================================================================
-- SILVER LAYER - Cleaned & Parsed Data
-- ============================================================================
-- این فایل، لایه‌ی Silver معماری مدالیون را می‌سازد.
--
-- Silver = داده‌ی پاک‌سازی‌شده و تجزیه‌شده از Bronze
-- منبع: bronze_wiki_events
-- ویژگی: فیلدهای استخراج‌شده + اعتبارسنجی + رفع تکرار
--
-- اجرا در: ClickHouse (dbeaver / clickhouse-client)
-- ترتیب: این فایل را بعد از 01-bronze-layer.sql اجرا کنید
-- ============================================================================

-- ⚠️ پاک‌سازی (اختیاری)
-- DROP VIEW IF EXISTS tutorial.silver_wiki_events_mv;
-- DROP TABLE IF EXISTS tutorial.silver_wiki_events;

-- ============================================================================
-- ۱. جدول Silver (داده‌ی پاک‌سازی‌شده)
-- ============================================================================
CREATE TABLE IF NOT EXISTS tutorial.silver_wiki_events
(
    -- شناسه‌ها
    event_id          UInt64,
    event_uuid        String,

    -- نوع رویداد
    event_type        LowCardinality(String),
    namespace         Int32,

    -- صفحه
    title             String,
    page_id           UInt64,

    -- کاربر
    user              String,
    user_is_bot       UInt8,
    user_is_anonymous UInt8,

    -- ویکی
    wiki              LowCardinality(String),
    language          LowCardinality(String),
    domain            LowCardinality(String),

    -- محتوا
    comment           String,
    is_minor          UInt8,
    is_patrolled      UInt8,

    -- تغییرات
    length_old        Int64,
    length_new        Int64,
    length_delta      Int64,
    revision_old      UInt64,
    revision_new      UInt64,

    -- زمان‌ها
    event_timestamp   DateTime,
    ingested_at       DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMMDD(event_timestamp)
ORDER BY (event_timestamp, wiki, event_id)
TTL event_timestamp + INTERVAL 90 DAY;

-- ============================================================================
-- ۲. Materialized View (Bronze → Silver برای داده‌های جدید)
-- ============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS tutorial.silver_wiki_events_mv
TO tutorial.silver_wiki_events AS
SELECT
    JSONExtractUInt(raw_message, 'id')                                                          AS event_id,
    JSONExtractString(raw_message, 'meta', 'id')                                                AS event_uuid,
    JSONExtractString(raw_message, 'type')                                                      AS event_type,
    JSONExtractInt(raw_message, 'namespace')                                                    AS namespace,
    JSONExtractString(raw_message, 'title')                                                     AS title,
    JSONExtractUInt(raw_message, 'page_id')                                                     AS page_id,
    JSONExtractString(raw_message, 'user')                                                      AS user,
    toUInt8(JSONExtractBool(raw_message, 'bot'))                                                AS user_is_bot,
    toUInt8(JSONExtractString(raw_message, 'user') LIKE '~%')                                   AS user_is_anonymous,
    JSONExtractString(raw_message, 'wiki')                                                      AS wiki,
    replaceRegexpOne(JSONExtractString(raw_message, 'wiki'), 'wiki$', '')                       AS language,
    JSONExtractString(raw_message, 'meta', 'domain')                                            AS domain,
    substring(JSONExtractString(raw_message, 'comment'), 1, 500)                                AS comment,
    toUInt8(JSONExtractBool(raw_message, 'minor'))                                              AS is_minor,
    toUInt8(JSONExtractBool(raw_message, 'patrolled'))                                          AS is_patrolled,
    JSONExtractInt(raw_message, 'length', 'old')                                                AS length_old,
    JSONExtractInt(raw_message, 'length', 'new')                                                AS length_new,
    JSONExtractInt(raw_message, 'length', 'new') - JSONExtractInt(raw_message, 'length', 'old') AS length_delta,
    JSONExtractUInt(raw_message, 'revision', 'old')                                             AS revision_old,
    JSONExtractUInt(raw_message, 'revision', 'new')                                             AS revision_new,
    toDateTime(JSONExtractInt(raw_message, 'timestamp'))                                        AS event_timestamp,
    now()                                                                                       AS ingested_at
FROM tutorial.bronze_wiki_events
WHERE JSONExtractString(raw_message, 'type') IN ('edit', 'new')
  AND JSONExtractString(raw_message, 'title') != ''
  AND JSONExtractString(raw_message, 'user') != '';

-- ============================================================================
-- ۳. Backfill: انتقال داده‌های تاریخی Bronze به Silver
-- ============================================================================
-- این دستور را یک بار بعد از ساخت جدول اجرا کنید تا داده‌های موجود Bronze
-- به Silver منتقل شوند. از این به بعد، MV خودکار عمل می‌کند.
-- ============================================================================
INSERT INTO tutorial.silver_wiki_events
SELECT
    JSONExtractUInt(raw_message, 'id')                                                          AS event_id,
    JSONExtractString(raw_message, 'meta', 'id')                                                AS event_uuid,
    JSONExtractString(raw_message, 'type')                                                      AS event_type,
    JSONExtractInt(raw_message, 'namespace')                                                    AS namespace,
    JSONExtractString(raw_message, 'title')                                                     AS title,
    JSONExtractUInt(raw_message, 'page_id')                                                     AS page_id,
    JSONExtractString(raw_message, 'user')                                                      AS user,
    toUInt8(JSONExtractBool(raw_message, 'bot'))                                                AS user_is_bot,
    toUInt8(JSONExtractString(raw_message, 'user') LIKE '~%')                                   AS user_is_anonymous,
    JSONExtractString(raw_message, 'wiki')                                                      AS wiki,
    replaceRegexpOne(JSONExtractString(raw_message, 'wiki'), 'wiki$', '')                       AS language,
    JSONExtractString(raw_message, 'meta', 'domain')                                            AS domain,
    substring(JSONExtractString(raw_message, 'comment'), 1, 500)                                AS comment,
    toUInt8(JSONExtractBool(raw_message, 'minor'))                                              AS is_minor,
    toUInt8(JSONExtractBool(raw_message, 'patrolled'))                                          AS is_patrolled,
    JSONExtractInt(raw_message, 'length', 'old')                                                AS length_old,
    JSONExtractInt(raw_message, 'length', 'new')                                                AS length_new,
    JSONExtractInt(raw_message, 'length', 'new') - JSONExtractInt(raw_message, 'length', 'old') AS length_delta,
    JSONExtractUInt(raw_message, 'revision', 'old')                                             AS revision_old,
    JSONExtractUInt(raw_message, 'revision', 'new')                                             AS revision_new,
    toDateTime(JSONExtractInt(raw_message, 'timestamp'))                                        AS event_timestamp,
    now()                                                                                       AS ingested_at
FROM tutorial.bronze_wiki_events
WHERE JSONExtractString(raw_message, 'type') IN ('edit', 'new')
  AND JSONExtractString(raw_message, 'title') != ''
  AND JSONExtractString(raw_message, 'user') != '';

-- ============================================================================
-- بررسی صحت
-- ============================================================================
-- SELECT count() FROM tutorial.silver_wiki_events;
-- SELECT * FROM tutorial.silver_wiki_events LIMIT 5;
-- ============================================================================
-- GOLD LAYER - Business-Level Aggregations
-- ============================================================================
-- این فایل، لایه‌ی Gold معماری مدالیون را می‌سازد.
--
-- Gold = داده‌ی تجمیعی و آماده برای مصرف BI
-- منبع: silver_wiki_events
-- ویژگی: KPI، آمار ساعتی/روزانه، جداول بهینه برای کوئری سریع
--
-- اجرا در: ClickHouse (DBeaver / clickhouse-client)
-- ترتیب: این فایل را بعد از 01-bronze و 02-silver اجرا کنید
-- ============================================================================


-- ============================================================================
-- جدول ۱: آمار ساعتی هر ویکی
-- ============================================================================
-- سوال تجاری: "در هر ساعت، هر ویکی چقدر فعالیت دارد؟"
-- موتور: SummingMergeTree (برای تجمیع سریع)
-- ============================================================================

CREATE TABLE IF NOT EXISTS tutorial.gold_wiki_hourly_stats
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
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, wiki)
TTL hour + INTERVAL 365 DAY;


CREATE MATERIALIZED VIEW IF NOT EXISTS tutorial.gold_wiki_hourly_stats_mv
TO tutorial.gold_wiki_hourly_stats AS
SELECT
    toStartOfHour(event_timestamp) AS hour,
    wiki,
    language,
    count() AS total_edits,
    countIf(user_is_bot = 1) AS bot_edits,
    countIf(user_is_bot = 0 AND user_is_anonymous = 0) AS human_edits,
    countIf(user_is_anonymous = 1) AS anonymous_edits,
    countIf(is_minor = 1) AS minor_edits,
    uniqExact(user) AS unique_users,
    uniqExact(title) AS unique_pages,
    sum(length_delta) AS total_length_delta
FROM tutorial.silver_wiki_events
GROUP BY hour, wiki, language;


-- ============================================================================
-- جدول ۲: پرکارترین کاربران روزانه
-- ============================================================================
-- سوال تجاری: "هر روز، پرکارترین کاربران هر ویکی چه کسانی هستند؟"
-- موتور: ReplacingMergeTree (آخرین نسخه را نگه می‌دارد)
-- ============================================================================

CREATE TABLE IF NOT EXISTS tutorial.gold_top_users_daily
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
) ENGINE = ReplacingMergeTree(last_edit_at)
PARTITION BY toYYYYMM(day)
ORDER BY (day, wiki, user)
TTL day + INTERVAL 365 DAY;


CREATE MATERIALIZED VIEW IF NOT EXISTS tutorial.gold_top_users_daily_mv
TO tutorial.gold_top_users_daily AS
SELECT
    toDate(event_timestamp) AS day,
    wiki,
    user,
    any(user_is_bot) AS user_is_bot,
    any(user_is_anonymous) AS user_is_anonymous,
    count() AS edits,
    uniqExact(title) AS pages_touched,
    sum(length_delta) AS total_length_delta,
    min(event_timestamp) AS first_edit_at,
    max(event_timestamp) AS last_edit_at
FROM tutorial.silver_wiki_events
GROUP BY day, wiki, user;


-- ============================================================================
-- جدول ۳: پرجنب‌وجوش‌ترین صفحات ساعتی
-- ============================================================================
-- سوال تجاری: "هر ساعت، پرجنب‌وجوش‌ترین صفحات کدامند؟"
-- موتور: ReplacingMergeTree
-- ============================================================================

CREATE TABLE IF NOT EXISTS tutorial.gold_top_pages_hourly
(
    hour                DateTime,
    wiki                LowCardinality(String),
    title               String,
    edits               UInt64,
    unique_users        UInt64,
    bot_edits           UInt64,
    total_length_delta  Int64,
    last_event_at       DateTime
) ENGINE = ReplacingMergeTree(last_event_at)
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, wiki, title)
TTL hour + INTERVAL 180 DAY;


CREATE MATERIALIZED VIEW IF NOT EXISTS tutorial.gold_top_pages_hourly_mv
TO tutorial.gold_top_pages_hourly AS
SELECT
    toStartOfHour(event_timestamp) AS hour,
    wiki,
    title,
    count() AS edits,
    uniqExact(user) AS unique_users,
    countIf(user_is_bot = 1) AS bot_edits,
    sum(length_delta) AS total_length_delta,
    max(event_timestamp) AS last_event_at
FROM tutorial.silver_wiki_events
GROUP BY hour, wiki, title;


-- ============================================================================
-- جدول ۴: توزیع زبان‌ها (ساعتی)
-- ============================================================================
-- سوال تجاری: "توزیع زبانی فعالیت‌ها در هر ساعت چگونه است؟"
-- موتور: SummingMergeTree
-- ============================================================================

CREATE TABLE IF NOT EXISTS tutorial.gold_language_hourly
(
    hour            DateTime,
    language        LowCardinality(String),
    wiki_count      UInt64,
    total_edits     UInt64,
    unique_users    UInt64,
    unique_pages    UInt64
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, language)
TTL hour + INTERVAL 365 DAY;


CREATE MATERIALIZED VIEW IF NOT EXISTS tutorial.gold_language_hourly_mv
TO tutorial.gold_language_hourly AS
SELECT
    toStartOfHour(event_timestamp) AS hour,
    language,
    uniqExact(wiki) AS wiki_count,
    count() AS total_edits,
    uniqExact(user) AS unique_users,
    uniqExact(title) AS unique_pages
FROM tutorial.silver_wiki_events
GROUP BY hour, language;


-- ============================================================================
-- BACKFILL: انتقال داده‌های تاریخی Silver به Gold
-- ============================================================================
-- این بخش، داده‌های موجود در Silver را به Gold منتقل می‌کند.
-- از این به بعد، MVها خودکار عمل می‌کنند.
--
-- ⚠️ توجه: این بخش را فقط یک بار اجرا کنید. اگر دوباره اجرا شود،
-- داده‌های تکراری به جدول اضافه می‌شوند. برای جلوگیری از این مشکل،
-- از `TRUNCATE` قبل از `INSERT` استفاده کنید (اگر داده‌های قبلی مهم نیستند).
-- ============================================================================

-- ۱. پر کردن آمار ساعتی ویکی‌ها
INSERT INTO tutorial.gold_wiki_hourly_stats
SELECT
    toStartOfHour(event_timestamp) AS hour,
    wiki,
    language,
    count() AS total_edits,
    countIf(user_is_bot = 1) AS bot_edits,
    countIf(user_is_bot = 0 AND user_is_anonymous = 0) AS human_edits,
    countIf(user_is_anonymous = 1) AS anonymous_edits,
    countIf(is_minor = 1) AS minor_edits,
    uniqExact(user) AS unique_users,
    uniqExact(title) AS unique_pages,
    sum(length_delta) AS total_length_delta
FROM tutorial.silver_wiki_events
GROUP BY hour, wiki, language;

-- ۲. پر کردن پرکارترین کاربران روزانه
INSERT INTO tutorial.gold_top_users_daily
SELECT
    toDate(event_timestamp) AS day,
    wiki,
    user,
    any(user_is_bot) AS user_is_bot,
    any(user_is_anonymous) AS user_is_anonymous,
    count() AS edits,
    uniqExact(title) AS pages_touched,
    sum(length_delta) AS total_length_delta,
    min(event_timestamp) AS first_edit_at,
    max(event_timestamp) AS last_edit_at
FROM tutorial.silver_wiki_events
GROUP BY day, wiki, user;

-- ۳. پر کردن پرجنب‌وجوش‌ترین صفحات
INSERT INTO tutorial.gold_top_pages_hourly
SELECT
    toStartOfHour(event_timestamp) AS hour,
    wiki,
    title,
    count() AS edits,
    uniqExact(user) AS unique_users,
    countIf(user_is_bot = 1) AS bot_edits,
    sum(length_delta) AS total_length_delta,
    max(event_timestamp) AS last_event_at
FROM tutorial.silver_wiki_events
GROUP BY hour, wiki, title;

-- ۴. پر کردن توزیع زبان‌ها
INSERT INTO tutorial.gold_language_hourly
SELECT
    toStartOfHour(event_timestamp) AS hour,
    language,
    uniqExact(wiki) AS wiki_count,
    count() AS total_edits,
    uniqExact(user) AS unique_users,
    uniqExact(title) AS unique_pages
FROM tutorial.silver_wiki_events
GROUP BY hour, language;


-- ============================================================================
-- بررسی صحت
-- ============================================================================
-- SELECT 'gold_wiki_hourly_stats' AS t, count() AS rows FROM tutorial.gold_wiki_hourly_stats
-- UNION ALL
-- SELECT 'gold_top_users_daily', count() FROM tutorial.gold_top_users_daily
-- UNION ALL
-- SELECT 'gold_top_pages_hourly', count() FROM tutorial.gold_top_pages_hourly
-- UNION ALL
-- SELECT 'gold_language_hourly', count() FROM tutorial.gold_language_hourly;
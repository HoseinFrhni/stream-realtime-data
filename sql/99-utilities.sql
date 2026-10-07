-- ============================================================================
-- UTILITIES - کوئری‌های کاربردی برای مانیتورینگ و تحلیل
-- ============================================================================
-- این فایل شامل کوئری‌های مفیدی است که در طول کار با پروژه به آن‌ها نیاز
-- پیدا می‌کنید. نیازی به اجرای همه نیست، هر کدام را جداگانه بزنید.
-- ============================================================================

-- ============================================================================
-- بخش ۱: بررسی وضعیت Kafka و Bronze
-- ============================================================================

-- وضعیت Consumer Kafka
SELECT database, table, consumer_id, assignments, num_messages_read, last_poll_time, last_exception
FROM system.kafka_consumers;

-- تعداد کل رکوردهای Bronze
SELECT count() FROM tutorial.bronze_wiki_events;

-- تعداد رکوردهای خطا
SELECT count() FROM tutorial.bronze_wiki_events_errors;

-- بازه‌ی زمانی داده‌ها
SELECT min(ingested_at) AS first_event, max(ingested_at) AS last_event, count() AS total
FROM tutorial.bronze_wiki_events;

-- ============================================================================
-- بخش ۲: بررسی Silver
-- ============================================================================

-- تعداد کل رکوردها
SELECT count() FROM tutorial.silver_wiki_events;

-- آمار کلی
SELECT
    count() AS total_events,
    uniqExact(wiki) AS unique_wikis,
    uniqExact(user) AS unique_users,
    uniqExact(title) AS unique_pages,
    sum(user_is_bot) AS bot_events,
    sum(user_is_anonymous) AS anonymous_events
FROM tutorial.silver_wiki_events;

-- توزیع بر اساس زبان
SELECT language, count() AS events
FROM tutorial.silver_wiki_events
GROUP BY language
ORDER BY events DESC
LIMIT 15;

-- توزیع بر اساس نوع رویداد
SELECT event_type, count() AS cnt
FROM tutorial.silver_wiki_events
GROUP BY event_type
ORDER BY cnt DESC;

-- ============================================================================
-- بخش ۳: کوئری‌های تحلیلی
-- ============================================================================

-- پرکارترین کاربران انسانی (بدون ربات و ناشناس)
SELECT user, count() AS edits
FROM tutorial.silver_wiki_events
WHERE user_is_bot = 0 AND user_is_anonymous = 0
GROUP BY user
ORDER BY edits DESC
LIMIT 10;

-- پرکارترین ویکی‌ها
SELECT wiki, language, count() AS edits
FROM tutorial.silver_wiki_events
GROUP BY wiki, language
ORDER BY edits DESC
LIMIT 10;

-- نسبت ربات/انسان/ناشناس
SELECT
    countIf(user_is_bot = 1) AS bots,
    countIf(user_is_bot = 0 AND user_is_anonymous = 0) AS humans,
    countIf(user_is_anonymous = 1) AS anonymous,
    count() AS total
FROM tutorial.silver_wiki_events;

-- بزرگ‌ترین تغییرات صفحات
SELECT title, user, length_delta, event_timestamp
FROM tutorial.silver_wiki_events
WHERE length_delta > 0
ORDER BY length_delta DESC
LIMIT 10;

-- نمودار ساعتی فعالیت
SELECT
    toStartOfHour(event_timestamp) AS hour,
    count() AS events
FROM tutorial.silver_wiki_events
GROUP BY hour
ORDER BY hour DESC
LIMIT 24;

-- ============================================================================
-- بخش ۴: آمار حجم داده
-- ============================================================================

-- حجم هر جدول
SELECT
    table,
    formatReadableSize(sum(bytes_on_disk)) AS size,
    sum(rows) AS total_rows,
    count() AS parts
FROM system.parts
WHERE database = 'tutorial' AND active
GROUP BY table
ORDER BY sum(bytes_on_disk) DESC;

-- تعداد پارتیشن‌های فعال
SELECT
    table,
    count(DISTINCT partition) AS partitions
FROM system.parts
WHERE database = 'tutorial' AND active
GROUP BY table;

-- ============================================================================
-- بخش ۵: نگهداری و بهینه‌سازی
-- ============================================================================

-- بهینه‌سازی جدول Silver (ادغام رکوردهای تکراری)
OPTIMIZE TABLE tutorial.silver_wiki_events FINAL;

-- حذف داده‌های قدیمی از Bronze (قبل از TTL خودکار)
ALTER TABLE tutorial.bronze_wiki_events DELETE WHERE ingested_at < now() - INTERVAL 7 DAY;

-- پاک کردن جدول خطاها
-- TRUNCATE TABLE tutorial.bronze_wiki_events_errors;

-- ============================================================================
-- بخش ۶: بررسی خطاهای رایج
-- ============================================================================

-- آخرین خطاهای ثبت‌شده
SELECT
    kafka_partition,
    kafka_offset,
    error_message,
    substring(raw_message, 1, 200) AS raw_preview,
    ingested_at
FROM tutorial.bronze_wiki_events_errors
ORDER BY ingested_at DESC
LIMIT 10;

-- گروه‌بندی خطاها
SELECT
    substring(error_message, 1, 100) AS error_type,
    count() AS cnt
FROM tutorial.bronze_wiki_events_errors
GROUP BY error_type
ORDER BY cnt DESC;
# 🏛️ Medallion Architecture

سندی جامع درباره‌ی پیاده‌سازی معماری مدالیون در پروژه‌ی Streaming Data Pipeline.

**نسخه:** 1.0.0  
**آخرین به‌روزرسانی:** 2026-10-07  
**نسخه‌ی ClickHouse:** 24.8  
**نسخه‌ی Kafka:** 3.9 (KRaft mode)

---

## فهرست مطالب

1. [معرفی معماری مدالیون](#۱-معرفی-معماری-مدالیون)
2. [چرا معماری مدالیون؟](#۲-چرا-معماری-مدالیون)
3. [نمای کلی معماری پروژه](#۳-نمای-کلی-معماری-پروژه)
4. [لایه‌ی Bronze](#۴-لایهی-bronze)
5. [لایه‌ی Silver](#۵-لایهی-silver)
6. [لایه‌ی Gold](#۶-لایهی-gold)
7. [جریان داده در هر لایه](#۷-جریان-داده-در-هر-لایه)
8. [تصمیمات طراحی](#۸-تصمیمات-طراحی)
9. [نکات عملیاتی](#۹-نکات-عملیاتی)
10. [مراجع](#۱۰-مراجع)

---

## ۱. معرفی معماری مدالیون

**معماری مدالیون** (Medallion Architecture) یک الگوی طراحی در مهندسی داده است که داده‌ها را در **سه لایه‌ی متوالی** سازماندهی می‌کند. هر لایه سطح بالاتری از **پالایش، اعتبارسنجی و ارزش تجاری** را ارائه می‌دهد.

این معماری اولین بار توسط **Databricks** معرفی شد و امروز به یک استاندارد صنعتی در ساخت Lakehouse و Data Pipeline تبدیل شده است.

### سه لایه‌ی اصلی

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   BRONZE     │───▶│   SILVER     │───▶│    GOLD      │
│   (Raw)      │    │  (Clean)     │    │ (Business)   │
└──────────────┘    └──────────────┘    └──────────────┘

داده خام            داده پاک‌سازی‌شده      داده تجاری
همان‌طور که          معتبر، یکپارچه،       KPI، گزارش،
دریافت شد           غنی‌سازی‌شده            آماده برای BI/ML
```

### چرا "مدالیون"؟

کلمه‌ی Medallion به معنای **مدال** یا **نشان** است. این نام از این ایده می‌آید که هر لایه مانند یک مدال، سطح بالاتری از **ارزش** را نشان می‌دهد:

- 🥉 **Bronze (برنز)**: کمترین ارزش تجاری، اما بیشترین جزئیات
- 🥈 **Silver (نقره)**: ارزش متوسط، داده‌ی پاک و ساخت‌یافته
- 🥇 **Gold (طلا)**: بالاترین ارزش تجاری، آماده برای تصمیم‌گیری

---

## ۲. چرا معماری مدالیون؟

### مزایای اصلی

| مزیت | توضیح |
|---|---|
| **قابلیت بازپخش (Replayability)** | اگر منطق Silver را تغییر دهید، می‌توانید دوباره از Bronze پردازش کنید بدون اینکه به Kafka برگردید. |
| **حفظ تاریخچه** | داده‌ی خام همیشه در Bronze باقی می‌ماند. هیچ چیز از بین نمی‌رود. |
| **دیباگ آسان** | اگر داده‌ی Silver مشکل داشت، به Bronze برگردید و ببینید کجای تبدیل اشتباه بوده. |
| **جداسازی مسئولیت‌ها** | هر لایه فقط به لایه‌ی قبل وابسته است. تغییر در یک لایه، لایه‌های دیگر را تحت تأثیر قرار نمی‌دهد. |
| **عملکرد بهتر** | Gold فقط شامل داده‌ی تجمیعی است، پس کوئری‌ها بسیار سریع هستند. |
| **انطباق با استانداردها** | این معماری در Databricks، Snowflake، BigQuery و... استاندارد است. |
| **کاهش هزینه** | می‌توانید TTLهای متفاوت برای هر لایه تعریف کنید. Bronze سریع‌تر حذف می‌شود، Gold بیشتر می‌ماند. |

### مقایسه با رویکرد سنتی

**رویکرد سنتی (بدون مدالیون):**
```
Source → ETL → Data Warehouse → BI
```
**مشکلات:**
- اگر ETL اشتباه باشد، داده از دست می‌رود.
- نمی‌توانید داده‌ی خام را دوباره پردازش کنید.
- دیباگ سخت است.

**رویکرد مدالیون:**
```
Source → Bronze → Silver → Gold → BI
```
**مزایا:**
- داده‌ی خام همیشه در دسترس است.
- می‌توانید هر لایه را جداگانه بازسازی کنید.
- دیباگ آسان است.

---

## ۳. نمای کلی معماری پروژه

### نمودار کامل

```
┌─────────────────────┐
│  Wikimedia SSE      │  ← منبع داده (جریان زنده‌ی ویرایش‌های ویکی‌پدیا)
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Producer.py        │  ← Python + kafka-python
│  (کل رویداد JSON)   │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Kafka: wiki-events │  ← 3 پارتیشن، Replication Factor 1
└──────────┬──────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────┐
│                    ClickHouse                                │
│                                                             │
│  ┌───────────────────────────────────────┐                  │
│  │ wiki_events_queue (Kafka Engine)      │                  │
│  │ - فرمت: JSONAsString                  │                  │
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
│  │ │ (MergeTree)              │   │  ← + کafka metadata     │
│  │ │ TTL: 30 روز              │   │                         │
│  │ └────────────┬─────────────┘   │                         │
│  │              │                  │                         │
│  │ ┌────────────▼─────────────┐   │                         │
│  │ │ bronze_wiki_events_errors│   │  ← پیام‌های خطادار      │
│  │ │ TTL: 7 روز               │   │                         │
│  │ └──────────────────────────┘   │                         │
│  └──────────────┬─────────────────┘                         │
│                 │                                            │
│                 ▼ (تجزیه JSON)                              │
│  ┌────────────────────────────────┐                         │
│  │ 🥈 SILVER                      │                         │
│  │ ┌──────────────────────────┐   │                         │
│  │ │ silver_wiki_events       │   │  ← فیلدهای استخراج‌شده   │
│  │ │ (ReplacingMergeTree)     │   │  ← اعتبارسنجی‌شده        │
│  │ │ TTL: 90 روز              │   │  ← رفع تکرار              │
│  │ └────────────┬─────────────┘   │                         │
│  └──────────────┬─────────────────┘                         │
│                 │                                            │
│                 ▼ (تجمیع)                                   │
│  ┌────────────────────────────────┐                         │
│  │ 🥇 GOLD                        │                         │
│  │ ┌──────────────────────────┐   │                         │
│  │ │ gold_wiki_hourly_stats   │   │  ← آمار ساعتی           │
│  │ │ gold_top_users_daily     │   │  ← پرکارترین کاربران    │
│  │ │ gold_top_pages_hourly    │   │  ← پرجنب‌وجوش‌ترین صفحات  │
│  │ │ gold_language_hourly     │   │  ← توزیع زبانی           │
│  │ │ TTL: 180-365 روز         │   │                         │
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

### جدول خلاصه‌ی جداول

| لایه | جدول | موتور | TTL | مصرف‌کننده |
|---|---|---|---|---|
| Bronze | `bronze_wiki_events` | MergeTree | ۳۰ روز | مهندسین داده |
| Bronze | `bronze_wiki_events_errors` | MergeTree | ۷ روز | مهندسین داده |
| Silver | `silver_wiki_events` | ReplacingMergeTree | ۹۰ روز | تحلیل‌گران |
| Gold | `gold_wiki_hourly_stats` | SummingMergeTree | ۱ سال | BI |
| Gold | `gold_top_users_daily` | ReplacingMergeTree | ۱ سال | BI |
| Gold | `gold_top_pages_hourly` | ReplacingMergeTree | ۱۸۰ روز | BI |
| Gold | `gold_language_hourly` | SummingMergeTree | ۱ سال | BI |

---

## ۴. لایه‌ی Bronze

### هدف

**ذخیره‌ی داده‌ی خام ۱۰۰٪ بدون هیچ تبدیلی.** این لایه، "حقیقت مطلق" (Source of Truth) پروژه است.

### اصول طراحی

1. **هیچ فیلدی حذف نمی‌شود.** کل JSON رویداد ذخیره می‌شود.
2. **هیچ تبدیلی انجام نمی‌شود.** نه تبدیل نوع، نه پاک‌سازی، نه فیلتر.
3. **فقط Append.** داده‌ها هرگز تغییر نمی‌کنند یا حذف نمی‌شوند (به جز TTL).
4. **متادیتای Kafka حفظ می‌شود.** topic، partition، offset، timestamp.

### جداول

#### `bronze_wiki_events`

```sql
CREATE TABLE tutorial.bronze_wiki_events
(
    raw_message     String,           -- کل JSON رویداد
    kafka_topic     String,           -- نام تاپیک
    kafka_partition UInt64,           -- شماره پارتیشن
    kafka_offset    UInt64,           -- Offset پیام
    kafka_timestamp DateTime,         -- زمان ثبت در Kafka
    kafka_key       String,           -- کلید پیام
    ingested_at     DateTime DEFAULT now()  -- زمان ورود به Bronze
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(ingested_at)
ORDER BY (ingested_at, kafka_partition, kafka_offset)
TTL ingested_at + INTERVAL 30 DAY;
```

**توضیح تصمیمات:**

| تصمیم | دلیل |
|---|---|
| `raw_message String` | کل JSON به صورت یک رشته. هیچ فیلدی از دست نمی‌رود. |
| `PARTITION BY toYYYYMMDD(ingested_at)` | پارتیشن‌بندی روزانه بر اساس زمان **ورود** (نه زمان رویداد). چون Bronze برای دیباگ است، زمان ورود مهم‌تر است. |
| `ORDER BY (ingested_at, kafka_partition, kafka_offset)` | امکان ردیابی پیام‌های Kafka و کوئری‌های بازه‌ای. |
| `TTL 30 روز` | داده‌ی خام حجیم است. ۳۰ روز برای دیباگ کافی است. |

#### `bronze_wiki_events_errors`

```sql
CREATE TABLE tutorial.bronze_wiki_events_errors
(
    kafka_topic     String,
    kafka_partition UInt64,
    kafka_offset    UInt64,
    raw_message     String,       -- پیام خام خطادار
    error_message   String,       -- متن خطا
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
    kafka_format = 'JSONAsString',         -- ← کل JSON به صورت رشته
    kafka_num_consumers = 3,
    kafka_handle_error_mode = 'stream';    -- ← مدیریت خطا
```

**نکات مهم:**

- **`JSONAsString`**: کل JSON را به صورت یک رشته دریافت می‌کند. اگر از `JSONEachRow` استفاده کنیم، ClickHouse فیلدها را تجزیه می‌کند و اگر ساختار عوض شود، پیام رد می‌شود.
- **`kafka_handle_error_mode = 'stream'`**: پیام‌های خطادار به ستون مجازی `_error` می‌روند و جریان متوقف نمی‌شود.
- **`kafka_num_consumers = 3`**: چون تاپیک ۳ پارتیشن دارد، ClickHouse از هر ۳ پارتیشن همزمان می‌خواند.

### ستون‌های مجازی

هنگام خواندن از `wiki_events_queue`، این ستون‌ها در دسترس هستند:

| ستون | نوع | توضیح |
|---|---|---|
| `_topic` | String | نام تاپیک |
| `_partition` | UInt64 | شماره پارتیشن |
| `_offset` | UInt64 | Offset پیام |
| `_timestamp` | Nullable(DateTime) | زمان ثبت در Kafka |
| `_key` | String | کلید پیام |
| `_error` | String | متن خطا (اگر پیام خراب باشد) |
| `_raw_message` | String | پیام خام (اگر پیام خراب باشد) |

---

## ۵. لایه‌ی Silver

### هدف

**پاک‌سازی، اعتبارسنجی و تجزیه‌ی داده‌ی خام.** Silver جایی است که داده‌ی خام به داده‌ی ساخت‌یافته و قابل استفاده تبدیل می‌شود.

### اصول طراحی

1. **تجزیه JSON:** استخراج فیلدهای کلیدی از `raw_message`.
2. **اعتبارسنجی:** فیلتر کردن رویدادهای ناقص یا نامعتبر.
3. **رفع تکرار:** با `ReplacingMergeTree`.
4. **غنی‌سازی:** اضافه کردن فیلدهای مشتق‌شده (مثل `language` از `wiki`).
5. **تبدیل نوع:** تبدیل رشته به DateTime، UInt8، و...

### جدول Silver

```sql
CREATE TABLE tutorial.silver_wiki_events
(
    -- شناسه‌ها
    event_id          UInt64,              -- id رویداد
    event_uuid        String,              -- meta.id (UUID)

    -- نوع رویداد
    event_type        LowCardinality(String),  -- edit, new
    namespace         Int32,               -- 0 = مقاله اصلی

    -- صفحه
    title             String,
    page_id           UInt64,

    -- کاربر
    user              String,
    user_is_bot       UInt8,               -- 1 اگر ربات
    user_is_anonymous UInt8,               -- 1 اگر IP

    -- ویکی
    wiki              LowCardinality(String),  -- enwiki, fawiki
    language          LowCardinality(String),  -- en, fa (استخراج‌شده)
    domain            LowCardinality(String),  -- en.wikipedia.org

    -- محتوا
    comment           String,              -- حداکثر ۵۰۰ کاراکتر
    is_minor          UInt8,               -- ویرایش جزئی
    is_patrolled      UInt8,               -- گشت‌زده‌شده

    -- تغییرات
    length_old        Int64,
    length_new        Int64,
    length_delta      Int64,               -- new - old
    revision_old      UInt64,
    revision_new      UInt64,

    -- زمان‌ها
    event_timestamp   DateTime,            -- زمان وقوع رویداد
    ingested_at       DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMMDD(event_timestamp)
ORDER BY (event_timestamp, wiki, event_id)
TTL event_timestamp + INTERVAL 90 DAY;
```

**توضیح تصمیمات:**

| تصمیم | دلیل |
|---|---|
| `ReplacingMergeTree(ingested_at)` | اگر رویداد تکراری با `event_id` یکسان بود، آخرین نسخه نگه داشته می‌شود. |
| `PARTITION BY toYYYYMMDD(event_timestamp)` | پارتیشن‌بندی بر اساس زمان **رویداد** (نه ورود). |
| `ORDER BY (event_timestamp, wiki, event_id)` | کوئری‌های زمانی و فیلتر بر اساس ویکی سریع می‌شوند. |
| `LowCardinality(String)` | برای فیلدهایی که مقادیر تکراری زیادی دارند (wiki, language). |
| `TTL 90 روز` | داده‌ی پاک‌شده، حجم کمتری دارد و برای تحلیل‌های بلندمدت مفید است. |

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
    -- استخراج زبان از wiki: enwiki → en, fawiki → fa
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

### چرا `language` از `wiki` استخراج می‌شود؟

در داده‌ی ویکی‌مدیا، فیلد `meta.language` **خالی** است (در SSE). زبان در فیلد `wiki` است که به شکل `enwiki`, `fawiki`, `dewiki` و... ذخیره می‌شود.

**استخراج:**
```sql
replaceRegexpOne('enwiki', 'wiki$', '')  -- → 'en'
replaceRegexpOne('fawiki', 'wiki$', '')  -- → 'fa'
replaceRegexpOne('commonswiki', 'wiki$', '')  -- → 'commons'
```

### Backfill

از آنجا که MV فقط به داده‌های **جدید** اعمال می‌شود، برای انتقال داده‌های تاریخی از این دستور استفاده می‌کنیم:

```sql
INSERT INTO tutorial.silver_wiki_events
SELECT ... FROM tutorial.bronze_wiki_events WHERE ...;
```

---

## ۶. لایه‌ی Gold

### هدف

**ساخت KPI و آمار تجمیعی برای مصرف مستقیم BI.** Gold جایی است که داده ارزش تجاری واقعی پیدا می‌کند.

### اصول طراحی

1. **تجمیع:** داده‌ها بر اساس بازه‌های زمانی (ساعت، روز) تجمیع می‌شوند.
2. **موتور مناسب:** `SummingMergeTree` برای اعداد، `ReplacingMergeTree` برای جایگزینی.
3. **بهینه برای کوئری:** جداول کوچک، کوئری سریع.
4. **TTL بلندمدت:** چون حجم داده کم است، می‌توان نگهداری طولانی‌تر داشت.

### جدول ۱: `gold_wiki_hourly_stats`

**سوال تجاری:** "در هر ساعت، هر ویکی چقدر فعالیت دارد؟"

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

### جدول ۲: `gold_top_users_daily`

**سوال تجاری:** "هر روز، پرکارترین کاربران هر ویکی چه کسانی هستند؟"

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

### جدول ۳: `gold_top_pages_hourly`

**سوال تجاری:** "هر ساعت، پرجنب‌وجوش‌ترین صفحات کدامند؟"

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

### جدول ۴: `gold_language_hourly`

**سوال تجاری:** "توزیع زبانی فعالیت‌ها در هر ساعت چگونه است؟"

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

### چرا `SummingMergeTree` و `ReplacingMergeTree`؟

**`SummingMergeTree`** برای جداولی که فقط اعداد دارند:
- وقتی رکوردهای با کلید یکسان ادغام می‌شوند، اعداد **جمع** می‌شوند.
- مثال: `gold_wiki_hourly_stats` — اگر دو رکورد برای `(hour='12:00', wiki='enwiki')` باشد، `total_edits` جمع می‌شود.

**`ReplacingMergeTree`** برای جداولی که آخرین وضعیت مهم است:
- وقتی رکوردهای با کلید یکسان ادغام می‌شوند، **آخرین** نسخه نگه داشته می‌شود.
- مثال: `gold_top_users_daily` — برای `(day='2026-10-07', wiki='enwiki', user='Ali')` فقط آخرین وضعیت مهم است.

---

## ۷. جریان داده در هر لایه

### مرحله‌ی ۱: از ویکی‌مدیا به Kafka

```
Wikimedia SSE → Producer.py → Kafka topic "wiki-events"
```

- Producer کل رویداد JSON را می‌فرستد.
- `key = title or meta.id` → حفظ ترتیب رویدادهای یک صفحه.
- `acks=all` + `enable_idempotence=True` → جلوگیری از ارسال تکراری.

### مرحله‌ی ۲: از Kafka به Bronze

```
Kafka → wiki_events_queue (Kafka Engine) → bronze_wiki_events
```

- Kafka Engine Table با فرمت `JSONAsString` کل JSON را می‌خواند.
- Materialized View داده‌های سالم را به `bronze_wiki_events` منتقل می‌کند.
- پیام‌های خراب به `bronze_wiki_events_errors` می‌روند.

### مرحله‌ی ۳: از Bronze به Silver

```
bronze_wiki_events → silver_wiki_events_mv → silver_wiki_events
```

- MV داده‌ی خام را تجزیه می‌کند.
- فیلدها استخراج می‌شوند.
- اعتبارسنجی انجام می‌شود.
- زبان از `wiki` استخراج می‌شود.

### مرحله‌ی ۴: از Silver به Gold

```
silver_wiki_events → [4 Materialized View] → [4 Gold Tables]
```

- هر MV یک نوع تجمیع انجام می‌دهد.
- داده‌ها در جداول Gold ذخیره می‌شوند.

### مرحله‌ی ۵: از Gold به BI

```
Gold Tables → Metabase/Grafana → Dashboards
```

- ابزارهای BI مستقیماً از جدول‌های Gold کوئری می‌زنند.
- چون داده تجمیعی است، کوئری‌ها بسیار سریع هستند.

---

## ۸. تصمیمات طراحی

### تصمیم ۱: چرا `JSONAsString` و نه `JSONEachRow`؟

**`JSONEachRow`:** ClickHouse فیلدها را در زمان دریافت تجزیه می‌کند. اگر فیلدی از رویداد حذف شود، پیام **رد** می‌شود.

**`JSONAsString`:** کل JSON به صورت رشته ذخیره می‌شود. هر فیلدی که در آینده اضافه شود، بدون تغییر در ساختار Kafka، در دسترس است.

**تصمیم:** `JSONAsString` — چون Bronze باید ۱۰۰٪ داده را حفظ کند.

### تصمیم ۲: چرا `ReplacingMergeTree` در Silver؟

Kafka ممکن است پیام‌ها را **تکراری** ارسال کند (At-Least-Once semantics). `ReplacingMergeTree` با کلید `event_id`، آخرین نسخه را نگه می‌دارد و تکراری‌ها را در پس‌زمینه ادغام می‌کند.

### تصمیم ۳: چرا `PARTITION BY toYYYYMMDD` در Bronze/Silver و `toYYYYMM` در Gold؟

- **Bronze/Silver:** حجم داده زیاد است. پارتیشن‌بندی روزانه برای TTL و کوئری‌های بازه‌ای بهتر است.
- **Gold:** حجم داده کم است. پارتیشن ماهانه تعداد پارتیشن‌ها را کم می‌کند و کوئری‌ها را سریع‌تر.

### تصمیم ۴: چرا `LowCardinality`؟

`LowCardinality(String)` یک نوع داده‌ی بهینه برای رشته‌هایی است که **مقادیر تکراری زیادی** دارند:
- `wiki`: فقط حدود ۱۰۰۰ مقدار یکتا
- `language`: فقط حدود ۳۰۰ مقدار یکتا
- `event_type`: فقط ۲-۳ مقدار

ClickHouse این مقادیر را **دیکشنری** می‌کند و به جای رشته، عدد ذخیره می‌کند. صرفه‌جویی حافظه: ۵-۱۰ برابر.

### تصمیم ۵: چرا TTLهای مختلف؟

| لایه | TTL | دلیل |
|---|---|---|
| Bronze | ۳۰ روز | داده خام حجیم. برای دیباگ کافی است. |
| Bronze Errors | ۷ روز | خطاها باید سریع بررسی شوند. |
| Silver | ۹۰ روز | داده پاک‌شده، برای تحلیل‌های میان‌مدت. |
| Gold Hourly | ۱ سال | برای روند بلندمدت. |
| Gold Pages | ۱۸۰ روز | صفحه‌ای حجیم‌تر است. |

### تصمیم ۶: چرا MV به جای `INSERT INTO ... SELECT`؟

MV **خودکار** عمل می‌کند: هر بار داده‌ی جدید به جدول منبع اضافه شود، MV آن را به جدول مقصد می‌فرستد. این باعث می‌شود:
- نیازی به Cron Job نباشد.
- تأخیر (Latency) کم باشد.
- کد کمتر و خطای کمتر.

اما برای **داده‌های تاریخی**، باید یک بار `INSERT INTO ... SELECT` اجرا شود (Backfill).

---

## ۹. نکات عملیاتی

### ۱. هرگز مستقیماً از جدول Kafka SELECT نزنید

```sql
-- ❌ اشتباه
SELECT * FROM tutorial.wiki_events_queue;
```

این کار **Offset را جلو می‌برد** و داده را از Materialized Viewها خارج می‌کند. برای دیباگ، از تنظیم زیر استفاده کنید:

```sql
SET stream_like_engine_allow_direct_select = 1;
SELECT * FROM tutorial.wiki_events_queue LIMIT 5;
```

### ۲. مانیتور کردن Consumer Kafka

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

### ۳. بررسی حجم داده‌ی هر جدول

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

### ۴. بهینه‌سازی رکوردهای تکراری

`ReplacingMergeTree` در پس‌زمینه رکوردهای تکراری را ادغام می‌کند. برای ادغام فوری:

```sql
OPTIMIZE TABLE tutorial.silver_wiki_events FINAL;
```

> ⚠️ این کار سنگین است. در محیط Production توصیه نمی‌شود.

### ۵. Backfill دستی

اگر می‌خواهید داده‌های تاریخی Silver را دوباره به Gold بفرستید:

```sql
-- ۱. پاک کردن جدول Gold
TRUNCATE TABLE tutorial.gold_wiki_hourly_stats;

-- ۲. Backfill
INSERT INTO tutorial.gold_wiki_hourly_stats
SELECT ... FROM tutorial.silver_wiki_events ...;
```

### ۶. بررسی خطاها

```sql
SELECT
    substring(error_message, 1, 100) AS error_type,
    count() AS cnt
FROM tutorial.bronze_wiki_events_errors
GROUP BY error_type
ORDER BY cnt DESC;
```

### ۷. پاک کردن داده‌های قدیمی (قبل از TTL خودکار)

```sql
ALTER TABLE tutorial.bronze_wiki_events
DELETE WHERE ingested_at < now() - INTERVAL 7 DAY;
```

---

## ۱۰. مراجع

### مستندات رسمی

- [Databricks Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [ClickHouse Kafka Engine](https://clickhouse.com/docs/en/engines/table-engines/integrations/kafka)
- [ClickHouse MergeTree](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree)
- [ClickHouse Materialized View](https://clickhouse.com/docs/en/sql-reference/statements/create/view#materialized-view)
- [Kafka Documentation](https://kafka.apache.org/documentation/)

### مقالات مرتبط

- [Building a Data Lakehouse with Medallion Architecture](https://www.databricks.com/blog/2021/08/30/building-a-data-lakehouse-with-medallion-architecture.html)
- [Streaming Data into ClickHouse with Kafka](https://clickhouse.com/blog/clickhouse-kafka-engine-tutorial)

### پروژه‌های مشابه

- [ClickHouse Kafka Examples](https://github.com/ClickHouse/clickhouse-kafka-examples)
- [Kafka Connect ClickHouse Sink](https://github.com/ClickHouse/clickhouse-kafka-connect)

---

## پیوست: چک‌لیست راه‌اندازی سریع

- [ ] راه‌اندازی زیرساخت با `docker compose up -d`
- [ ] ساخت تاپیک `wiki-events` در Kafka
- [ ] اجرای `sql/01-bronze-layer.sql`
- [ ] اجرای `sql/02-silver-layer.sql`
- [ ] اجرای `sql/03-gold-layer.sql`
- [ ] اجرای Producer (`python producer.py`)
- [ ] بررسی `SELECT count() FROM tutorial.bronze_wiki_events`
- [ ] بررسی `SELECT count() FROM tutorial.silver_wiki_events`
- [ ] بررسی `SELECT count() FROM tutorial.gold_wiki_hourly_stats`
- [ ] اجرای کوئری‌های تحلیلی Gold
- [ ] اتصال Metabase به ClickHouse

---

## مجوز

این مستند بخشی از پروژه [stream-realtime-data](https://github.com/YOUR-USERNAME/stream-realtime-data) است.
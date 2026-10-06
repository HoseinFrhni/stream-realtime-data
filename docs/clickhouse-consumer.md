# ClickHouse as a Stream Consumer

سندی جامع درباره‌ی استفاده از ClickHouse به عنوان مصرف‌کننده (Consumer) جریان داده در خط لوله‌ی Kafka → ClickHouse.

**نسخه:** 1.0.0
**آخرین به‌روزرسانی:** 2026-10-06
**نسخه‌ی ClickHouse:** 24.8
**نسخه‌ی Kafka:** 3.7 (KRaft mode)

---

## فهرست مطالب

1. [معرفی معماری](#۱-معرفی-معماری)
2. [چرا ClickHouse به عنوان Consumer؟](#۲-چرا-clickhouse-به-عنوان-consumer)
3. [الگوی پنج‌جدولی](#۳-الگوی-پنججدولی)
4. [پیش‌نیازها](#۴-پیشنیازها)
5. [راه‌اندازی زیرساخت](#۵-راهاندازی-زیرساخت)
6. [اتصال به شبکه Kafka](#۶-اتصال-به-شبکه-kafka)
7. [ساختار جداول](#۷-ساختار-جداول)
8. [تنظیمات Kafka Engine](#۸-تنظیمات-kafka-engine)
9. [مدیریت خطا](#۹-مدیریت-خطا)
10. [مدیریت دسترسی کاربران](#۱۰-مدیریت-دسترسی-کاربران)
11. [مثال‌های تحلیلی](#۱۱-مثالهای-تحلیلی)
12. [دیباگ و مانیتورینگ](#۱۲-دیباگ-و-مانیتورینگ)
13. [بهینه‌سازی و مقیاس‌پذیری](#۱۳-بهینهسازی-و-مقیاسپذیری)
14. [خطاهای رایج و راه‌حل‌ها](#۱۴-خطاهای-رایج-و-راهحلها)
15. [نکات عملیاتی](#۱۵-نکات-عملیاتی)
16. [مراجع](#۱۶-مراجع)

---

## ۱. معرفی معماری

در این پروژه، ClickHouse نقش **Consumer** را در یک معماری رویدادمحور (Event-Driven) ایفا می‌کند. داده‌ها از منبع زنده‌ی ویکی‌مدیا توسط Producer پایتون به Kafka ارسال می‌شوند و ClickHouse مستقیماً و بدون واسطه آن‌ها را می‌خواند و در جداول بهینه ذخیره می‌کند.

### نمودار معماری

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

### جریان داده‌ی گام‌به‌گام

1. **Producer** به جریان SSE ویکی‌مدیا وصل می‌شود.
2. رویدادها به JSON تبدیل و به تاپیک `wiki-events` در Kafka ارسال می‌شوند.
3. **جدول Kafka Engine** در ClickHouse به صورت خودکار این پیام‌ها را می‌خواند.
4. **Materialized View اصلی** پیام‌های سالم را به جدول `wiki_events` منتقل می‌کند.
5. **Materialized View خطاها** پیام‌های خراب را به جدول `wiki_events_errors` منتقل می‌کند.
6. ابزارهای BI (مثل DBeaver) روی جدول `wiki_events` کوئری می‌زنند.

---

## ۲. چرا ClickHouse به عنوان Consumer؟

ClickHouse با موتور **Kafka Engine** این امکان را فراهم می‌کند که مستقیماً به Kafka متصل شود و داده را جریانی بخواند. مزایا:

| مزیت | توضیح |
|---|---|
| **بدون کد واسط** | نیازی به Consumer سفارشی در Python/Java نیست. |
| **Throughput بالا** | قادر است میلیون‌ها رویداد در ثانیه را مصرف کند. |
| **ذخیره‌سازی ستونی** | داده‌ها در قالب فشرده و بهینه برای تحلیل ذخیره می‌شوند. |
| **مدیریت خطای داخلی** | پیام‌های خراب به جدول جداگانه می‌روند و جریان متوقف نمی‌شود. |
| **یکپارچگی با SQL** | کوئری‌های تحلیلی با SQL استاندارد و بسیار سریع. |
| **مقیاس‌پذیری افقی** | با افزایش پارتیشن و Consumer، Throughput بالا می‌رود. |

---

## ۳. الگوی پنج‌جدولی

ClickHouse برای مصرف Kafka با مدیریت خطا از **الگوی پنج‌جدولی** استفاده می‌کند. هر جدول نقش مشخصی دارد:

| جدول | موتور | نقش |
|---|---|---|
| `wiki_events_queue` | `Kafka` | رابط اتصال به Kafka. داده را می‌خواند اما ذخیره نمی‌کند. |
| `wiki_events_mv` | `Materialized View` | پیام‌های سالم (`_error=''`) را به جدول اصلی منتقل می‌کند. |
| `wiki_events` | `MergeTree` | ذخیره‌سازی دائمی داده‌های سالم. |
| `wiki_events_errors_mv` | `Materialized View` | پیام‌های خطادار (`_error!=''`) را به جدول خطا منتقل می‌کند. |
| `wiki_events_errors` | `MergeTree` | ذخیره‌سازی پیام‌های خراب برای بررسی و دیباگ. |

### جریان داده

```
Kafka Topic → wiki_events_queue
                   ├── _error=''  → wiki_events_mv  → wiki_events
                   └── _error!='' → wiki_events_errors_mv → wiki_events_errors
```

> ⚠️ **نکته حیاتی:** هرگز مستقیماً از `wiki_events_queue` کوئری نگیرید، چون Offset را جلو می‌برد و داده را از دسترس Materialized Viewها خارج می‌کند.

---

## ۴. پیش‌نیازها

| ابزار | نسخه پیشنهادی | کاربرد |
|---|---|---|
| Docker | 24.0+ | اجرای کانتینرها |
| Docker Compose | 2.20+ | مدیریت سرویس‌ها |
| Kafka | 3.7+ | پیام‌رسان |
| ClickHouse | 24.8+ | Consumer و ذخیره‌سازی |
| Python | 3.10+ | اجرای Producer |
| DBeaver | 23.0+ | کوئری و تحلیل |
| Git Bash / WSL | - | اجرای دستورات در ویندوز |

---

## ۵. راه‌اندازی زیرساخت

### ۵.۱. Docker Compose

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

### ۵.۲. Docker Run (جایگزین)

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

### ۵.۳. دسترسی به کلاینت

```bash
docker exec -it clickhouse clickhouse-client
```

یا با کاربر خاص:

```bash
docker exec -it clickhouse clickhouse-client --user default --password ""
```

---

## ۶. اتصال به شبکه Kafka

اگر ClickHouse و Kafka در یک شبکه Docker نباشند، ClickHouse نمی‌تواند نام `kafka-broker` را resolve کند و خطای `DNS resolution failed` می‌دهد.

### گام‌های اتصال

```bash
# ۱. نام شبکه Kafka را پیدا کنید
docker inspect kafka-broker --format "{{json .NetworkSettings.Networks}}"

# ۲. ClickHouse را به آن شبکه وصل کنید
docker network connect kafka-lab_default clickhouse

# ۳. ClickHouse را ری‌استارت کنید
docker restart clickhouse

# ۴. تایید DNS
docker exec -it clickhouse getent hosts kafka-broker
```

### خروجی مورد انتظار

```
172.20.0.2      kafka-broker
```

### بررسی شبکه‌های ClickHouse

```bash
docker inspect clickhouse --format "{{json .NetworkSettings.Networks}}"
```

باید دو شبکه را ببینید: شبکه اصلی خودش و شبکه Kafka.

---

## ۷. ساختار جداول

### ۷.۱. جدول Kafka Engine (رابط دریافت با مدیریت خطا)

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

> 📌 **مهم:** تنظیم `kafka_handle_error_mode = 'stream'` دو ستون مجازی `_error` و `_raw_message` را در دسترس Materialized Viewها قرار می‌دهد.

### ۷.۲. جدول MergeTree (ذخیره داده‌های سالم)

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

### ۷.۳. Materialized View اصلی (انتقال داده‌های سالم)

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

### ۷.۴. جدول خطاها (ذخیره پیام‌های خراب)

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

### ۷.۵. Materialized View خطاها

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

### ۷.۶. بررسی صحت ساختار

```sql
SHOW TABLES FROM tutorial;
```

**خروجی مورد انتظار:**

```
wiki_events
wiki_events_errors
wiki_events_errors_mv
wiki_events_mv
wiki_events_queue
```

### ۷.۷. ذخیره در فایل init (اختیاری)

می‌توانید همه دستورات بالا را در `init-db/001-schema.sql` قرار دهید تا در اولین اجرای ClickHouse به طور خودکار اجرا شوند.

> ⚠️ **توجه:** ClickHouse فقط **یک بار** (وقتی volume خالی است) اسکریپت‌های init را اجرا می‌کند. برای اجرای مجدد:
> ```bash
> docker compose down -v
> docker compose up -d
> ```

---

## ۸. تنظیمات Kafka Engine

### جدول کامل تنظیمات

| تنظیم | مقدار پیشنهادی | توضیح |
|---|---|---|
| `kafka_broker_list` | `kafka-broker:29092` | آدرس Kafka. داخل Docker از نام سرویس استفاده می‌کنیم. |
| `kafka_topic_list` | `wiki-events` | نام تاپیک. |
| `kafka_group_name` | `clickhouse-consumer-group` | Consumer Group. |
| `kafka_format` | `JSONEachRow` | فرمت داده ورودی. |
| `kafka_num_consumers` | `1` | تعداد Consumerهای موازی. |
| `kafka_handle_error_mode` | `stream` | مدیریت خطا در جریان. |
| `kafka_skip_broken_messages` | `10` | (اختیاری) تعداد پیام نامعتبر قبل از خطا. |
| `kafka_max_block_size` | `65536` | (اختیاری) حداکثر پیام در هر batch. |
| `kafka_poll_max_batch_size` | `1000` | (اختیاری) حداکثر پیام در هر poll. |
| `kafka_poll_timeout_ms` | `500` | (اختیاری) زمان انتظار برای poll. |
| `kafka_flush_interval_ms` | `7500` | (اختیاری) بازه flush خودکار. |

### نمونه پیکربندی پیشرفته

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

### ⚠️ محدودیت مهم: تغییر تنظیمات با ALTER

**موتور Kafka از `ALTER TABLE ... MODIFY SETTING` پشتیبانی نمی‌کند.** اگر بخواهید تنظیمی را تغییر دهید، باید جدول را **حذف و بازسازی** کنید:

```sql
-- ۱. حذف MVهای وابسته
DROP VIEW IF EXISTS tutorial.wiki_events_mv;
DROP VIEW IF EXISTS tutorial.wiki_events_errors_mv;

-- ۲. حذف جدول Kafka
DROP TABLE IF EXISTS tutorial.wiki_events_queue;

-- ۳. بازسازی با تنظیمات جدید
CREATE TABLE tutorial.wiki_events_queue (...) ENGINE = Kafka SETTINGS ...;

-- ۴. بازسازی MVها
CREATE MATERIALIZED VIEW tutorial.wiki_events_mv ...;
CREATE MATERIALIZED VIEW tutorial.wiki_events_errors_mv ...;
```

> ✅ **نکته:** چون جدول Kafka داده ذخیره نمی‌کند، حذف و بازسازی آن **داده‌ای از دست نمی‌دهد**. فقط ممکن است چند ثانیه داده در حین بازسازی از دست برود.

---

## ۹. مدیریت خطا

### چرا مدیریت خطا مهم است؟

در جریان داده، همیشه احتمال دارد پیام‌های خراب یا ناسازگار وارد شوند. دلایل رایج:

- **قطع اتصال نیمه‌کاره:** پیام ناقص JSON.
- **فیلد گمشده:** رویدادهایی با ساختار متفاوت.
- **نوع داده اشتباه:** مثلاً `bot` به جای عدد، رشته.
- **پیام‌های خیلی بزرگ:** بیشتر از حد مجاز.

بدون مدیریت خطا، **کل Consumer متوقف می‌شود** و پایپ‌لاین از کار می‌افتد.

### حالت‌های `kafka_handle_error_mode`

| حالت | رفتار | توصیه |
|---|---|---|
| `default` | در صورت خطا، کل مصرف‌کننده متوقف می‌شود. | ❌ توصیه نمی‌شود |
| `stream` | خطاها در ستون مجازی `_error` قرار می‌گیرند و جریان ادامه می‌یابد. | ✅ توصیه‌شده |
| `dead_letter_queue` | (فقط ClickHouse 25.8+) پیام‌های خراب به `system.dead_letter_queue` می‌روند. | ⭐ گزینه پیشرفته |

### ستون‌های مجازی

وقتی `kafka_handle_error_mode = 'stream'` فعال باشد، این ستون‌ها به طور خودکار در دسترس هستند:

| ستون | نوع | توضیح |
|---|---|---|
| `_error` | `String` | متن خطا. اگر پیام سالم باشد، خالی است. |
| `_raw_message` | `String` | پیام خام Kafka (بدون تجزیه). |
| `_topic` | `String` | نام تاپیک. |
| `_partition` | `UInt64` | شماره پارتیشن. |
| `_offset` | `UInt64` | Offset پیام. |
| `_key` | `String` | کلید پیام. |
| `_timestamp` | `Nullable(DateTime)` | زمان ثبت در Kafka. |

### بررسی خطاهای ثبت‌شده

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

### گروه‌بندی خطاها بر اساس نوع

```sql
SELECT
    substring(_error, 1, 100) AS error_type,
    count() AS cnt
FROM tutorial.wiki_events_errors
GROUP BY error_type
ORDER BY cnt DESC;
```

### نمودار زمانی خطاها (۲۴ ساعت اخیر)

```sql
SELECT
    toStartOfHour(ingested_at) AS hour,
    count() AS error_count
FROM tutorial.wiki_events_errors
WHERE ingested_at > now() - INTERVAL 24 HOUR
GROUP BY hour
ORDER BY hour DESC;
```

### TTL برای جدول خطاها

برای جلوگیری از اشغال فضا توسط پیام‌های خراب قدیمی:

```sql
ALTER TABLE tutorial.wiki_events_errors
MODIFY TTL ingested_at + INTERVAL 7 DAY;
```

### پاک‌سازی دستی خطاها

```sql
TRUNCATE TABLE tutorial.wiki_events_errors;
```

---

## ۱۰. مدیریت دسترسی کاربران

### چرا کاربر جدید؟

کاربر `default` در ClickHouse از فایل `users.xml` استفاده می‌کند که **فقط خواندنی** است. اگر بخواهید رمز برای آن تعیین کنید، با خطای `ACCESS_STORAGE_READONLY` مواجه می‌شوید.

### راه‌حل: ساخت کاربر جدید

**گام ۱: فعال‌سازی `access_management` برای `default`**

فایل `default-access.xml` را در `users.d` بسازید:

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

**گام ۲: ری‌استارت ClickHouse**

```bash
docker restart clickhouse
```

**گام ۳: ساخت کاربر جدید**

```bash
docker exec -it clickhouse clickhouse-client --query "CREATE USER dbeaver_user IDENTIFIED WITH plaintext_password BY 'dbeaver123'"
```

**گام ۴: اعطای دسترسی‌ها**

```bash
docker exec -it clickhouse clickhouse-client --query "GRANT CURRENT GRANTS ON *.* TO dbeaver_user WITH GRANT OPTION"
```

> 📌 **نکته:** اگر `GRANT ALL` با خطای `Not enough privileges` مواجه شد، از `GRANT CURRENT GRANTS` استفاده کنید. این دستور تمام دسترسی‌های کاربر `default` را منتقل می‌کند.

**گام ۵: تایید دسترسی‌ها**

```sql
SHOW GRANTS FOR dbeaver_user;
```

**گام ۶: تست از CLI**

```bash
docker exec -it clickhouse clickhouse-client --user dbeaver_user --password dbeaver123 --query "SELECT currentUser()"
```

**خروجی مورد انتظار:**
```
dbeaver_user
```

### اتصال از DBeaver

| فیلد | مقدار |
|---|---|
| **Host** | `localhost` |
| **Port** | `8123` |
| **Database** | `tutorial` |
| **User** | `dbeaver_user` |
| **Password** | `dbeaver123` |

### تنظیم رمز برای کاربر `default` (اختیاری)

حالا که `access_management` فعال است:

```bash
docker exec -it clickhouse clickhouse-client --query "ALTER USER default IDENTIFIED WITH plaintext_password BY 'tutorial123'"
```

---

## ۱۱. مثال‌های تحلیلی

### تعداد کل رویدادها

```sql
SELECT count() AS total_events FROM tutorial.wiki_events;
```

### پرکارترین کاربران

```sql
SELECT user, count() AS edits
FROM tutorial.wiki_events
GROUP BY user
ORDER BY edits DESC
LIMIT 10;
```

### نسبت ربات‌ها به انسان‌ها

```sql
SELECT
    bot,
    count() AS cnt,
    round(count() * 100.0 / sum(count()) OVER (), 2) AS pct
FROM tutorial.wiki_events
GROUP BY bot;
```

### پرجنب‌وجوش‌ترین صفحات

```sql
SELECT title, count() AS edits
FROM tutorial.wiki_events
GROUP BY title
ORDER BY edits DESC
LIMIT 10;
```

### نمودار زمانی فعالیت (به تفکیک دقیقه)

```sql
SELECT
    toStartOfMinute(timestamp) AS minute,
    count() AS edits
FROM tutorial.wiki_events
GROUP BY minute
ORDER BY minute DESC
LIMIT 20;
```

### کاربران ثبت‌نام‌شده (بدون IP)

```sql
SELECT user, count() AS edits
FROM tutorial.wiki_events
WHERE user NOT LIKE '~%'
GROUP BY user
ORDER BY edits DESC
LIMIT 10;
```

### کاربرانی که فقط ویرایش‌های رباتیک دارند

```sql
SELECT user, count() AS edits
FROM tutorial.wiki_events
WHERE bot = 1
GROUP BY user
ORDER BY edits DESC
LIMIT 5;
```

### نرخ ورود داده (Events per Second)

```sql
SELECT
    toStartOfSecond(ingested_at) AS second,
    count() AS events
FROM tutorial.wiki_events
WHERE ingested_at > now() - INTERVAL 1 MINUTE
GROUP BY second
ORDER BY second DESC;
```

### تأخیر پردازش (زمان بین رویداد و ورود به ClickHouse)

```sql
SELECT
    round(avg(dateDiff('second', timestamp, ingested_at)), 2) AS avg_lag_seconds,
    round(max(dateDiff('second', timestamp, ingested_at)), 2) AS max_lag_seconds
FROM tutorial.wiki_events
WHERE ingested_at > now() - INTERVAL 5 MINUTE;
```

---

## ۱۲. دیباگ و مانیتورینگ

### مشاهده لاگ ClickHouse

```bash
docker logs clickhouse --tail 50
```

### مشاهده لاگ خطاهای ClickHouse

```bash
docker exec -it clickhouse tail -50 /var/log/clickhouse-server/clickhouse-server.err.log
```

### وضعیت Kafka Consumers

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

**ستون‌های مهم:**
- `assignments`: پارتیشن‌های تخصیص‌یافته
- `last_poll_time`: آخرین زمان poll
- `num_messages_read`: تعداد پیام‌های خوانده‌شده
- `last_exception`: آخرین استثنا (در صورت وجود)

### بررسی Lag مصرف‌کننده

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

### بررسی سلامت MVها

```sql
SELECT
    database,
    view_name,
    dependencies_database,
    dependencies_table
FROM system.view_refreshes;
```

### خواندن مستقیم از جدول Kafka (فقط برای دیباگ)

```sql
SET stream_like_engine_allow_direct_select = 1;
SELECT * FROM tutorial.wiki_events_queue LIMIT 5;
```

> ⚠️ **هشدار:** این کار Offset را جلو می‌برد و داده را از MVها خارج می‌کند. فقط در محیط توسعه استفاده کنید.

### آمار جدول‌ها

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

## ۱۳. بهینه‌سازی و مقیاس‌پذیری

### افزایش تعداد Consumer

اگر تاپیک چند پارتیشن دارد، تعداد Consumer را افزایش دهید. **نیاز به بازسازی جدول دارد:**

```sql
-- ۱. حذف MVهای وابسته
DROP VIEW IF EXISTS tutorial.wiki_events_mv;
DROP VIEW IF EXISTS tutorial.wiki_events_errors_mv;

-- ۲. حذف جدول Kafka
DROP TABLE IF EXISTS tutorial.wiki_events_queue;

-- ۳. بازسازی با Consumer بیشتر
CREATE TABLE tutorial.wiki_events_queue (...)
ENGINE = Kafka
SETTINGS
    ...
    kafka_num_consumers = 3,
    kafka_handle_error_mode = 'stream';

-- ۴. بازسازی MVها
CREATE MATERIALIZED VIEW tutorial.wiki_events_mv ...;
CREATE MATERIALIZED VIEW tutorial.wiki_events_errors_mv ...;
```

> 📌 **قانون:** تعداد Consumer نباید از تعداد پارتیشن‌ها بیشتر باشد.

### تغییر Ordering Key

اگر کوئری‌ها بیشتر بر اساس `user` است:

```sql
ALTER TABLE tutorial.wiki_events
MODIFY ORDER BY (user, timestamp);
```

> ⚠️ این دستور روی جداول موجود با داده محدودیت دارد. بهترین کار ساخت جدول جدید و انتقال داده است.

### TTL روی جدول اصلی

```sql
ALTER TABLE tutorial.wiki_events
MODIFY TTL toDateTime(timestamp) + INTERVAL 30 DAY;
```

### افزودن Projection (برای کوئری‌های تکراری)

```sql
ALTER TABLE tutorial.wiki_events
ADD PROJECTION user_stats (
    SELECT user, count() GROUP BY user
);
```

### افزایش Retention در Kafka

اگر می‌خواهید داده‌ها بیشتر در Kafka بمانند:

```bash
docker exec -it kafka-broker /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server localhost:29092 \
  --alter --entity-type topics --entity-name wiki-events \
  --add-config retention.ms=604800000
```

(۷ روز = 604800000 میلی‌ثانیه)

---

## ۱۴. خطاهای رایج و راه‌حل‌ها

| خطا | علت | راه‌حل |
|---|---|---|
| `DNS resolution failed` | ClickHouse و Kafka در یک شبکه نیستند. | `docker network connect kafka-lab_default clickhouse` |
| `UnknownTopicOrPartition` | تاپیک ساخته نشده. | تاپیک را در Kafka UI بسازید. |
| `Authentication failed` | کاربر/رمز اشتباه. | کاربر `dbeaver_user` بسازید. |
| `Cannot alter settings` | موتور Kafka از ALTER پشتیبانی نمی‌کند. | جدول را حذف و بازسازی کنید. |
| `CANNOT_READ_FROM_FILE_DESCRIPTOR` | فایل init خالی یا دایرکتوری. | فایل SQL را بررسی کنید. |
| `Direct select is not allowed` | SELECT مستقیم از Kafka Engine. | از MV یا `stream_like_engine_allow_direct_select=1` استفاده کنید. |
| `ACCESS_STORAGE_READONLY` | تلاش برای تغییر کاربر `default`. | کاربر جدید بسازید. |
| `ACCESS_DENIED` | کاربر دسترسی لازم را ندارد. | `GRANT CURRENT GRANTS` استفاده کنید. |
| `Connection refused (localhost:9000)` | ClickHouse هنوز آماده نیست. | چند ثانیه صبر کنید و دوباره تلاش کنید. |
| `Only RowBinaryWithNameAndTypes...` | SELECT از جدول Kafka بدون FORMAT مناسب. | از `FORMAT JSONEachRow` استفاده کنید. |

---

## ۱۵. نکات عملیاتی

### ۱. هرگز مستقیماً از جدول Kafka SELECT نزنید

Offset را جلو می‌برد و MVها داده را از دست می‌دهند.

### ۲. تعداد Consumer را با پارتیشن هماهنگ کنید

اگر ۳ پارتیشن دارید و ۱ Consumer، دو پارتیشن بیکار می‌مانند.

### ۳. Lag مصرف‌کننده را مانیتور کنید

```sql
SELECT
    database,
    table,
    consumer_id,
    num_messages_read,
    last_poll_time
FROM system.kafka_consumers;
```

### ۴. گروه مصرف‌کننده را عوض نکنید

تغییر `kafka_group_name` باعث خواندن از ابتدای تاپیک و ذخیره‌ی داده‌ی تکراری می‌شود.

### ۵. پارتیشن‌بندی MergeTree را جدی بگیرید

`PARTITION BY toYYYYMM(timestamp)` برای داده‌های ماهانه عالی است. اگر داده‌های شما روزانه زیاد است، از `toYYYYMMDD(timestamp)` استفاده کنید.

### ۶. جدول خطاها را مانیتور کنید

اگر تعداد خطاها زیاد شد، یعنی Producer یا ساختار داده مشکل دارد.

### ۷. از `ingested_at` غافل نشوید

این ستون به شما می‌گوید داده چه زمانی وارد ClickHouse شده، که با `timestamp` (زمان وقوع رویداد) متفاوت است. برای محاسبه‌ی Lag و دیباگ عالی است.

### ۸. Backup بگیرید

```sql
BACKUP TABLE tutorial.wiki_events TO Disk('backups', 'wiki_events_backup.zip');
```

### ۹. فایل init را بعد از هر تغییر volume پاک کنید

```bash
docker compose down -v
docker compose up -d
```

### ۱۰. برای محیط Production، از کاربر `default` استفاده نکنید

کاربر `dbeaver_user` با رمز واضح بسازید و آن را در DBeaver و Producer استفاده کنید.

---

## ۱۶. مراجع

- [ClickHouse Kafka Engine Documentation](https://clickhouse.com/docs/en/engines/table-engines/integrations/kafka)
- [ClickHouse MergeTree Documentation](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree)
- [ClickHouse Materialized View Documentation](https://clickhouse.com/docs/en/sql-reference/statements/create/view#materialized-view)
- [Kafka Producer Configuration](https://kafka.apache.org/documentation/#producerconfigs)
- [Kafka KRaft Mode Documentation](https://kafka.apache.org/documentation/#kraft)

---

## پیوست: چک‌لیست راه‌اندازی سریع

- [ ] راه‌اندازی `docker-compose.yaml` با ClickHouse، Kafka، Kafka UI
- [ ] اتصال ClickHouse به شبکه Kafka (`docker network connect`)
- [ ] ساخت دیتابیس `tutorial`
- [ ] ساخت جدول `wiki_events_queue` با `kafka_handle_error_mode = 'stream'`
- [ ] ساخت جدول `wiki_events`
- [ ] ساخت MV اصلی `wiki_events_mv`
- [ ] ساخت جدول `wiki_events_errors`
- [ ] ساخت MV خطاها `wiki_events_errors_mv`
- [ ] ساخت تاپیک `wiki-events` در Kafka
- [ ] ساخت کاربر `dbeaver_user` و تنظیم `access_management`
- [ ] اتصال DBeaver با `dbeaver_user`
- [ ] اجرای Producer (`python producer.py`)
- [ ] بررسی `SELECT count() FROM tutorial.wiki_events`
- [ ] بررسی `SELECT count() FROM tutorial.wiki_events_errors`
- [ ] اجرای کوئری‌های تحلیلی

---

## مجوز

این مستند بخشی از پروژه [streaming-data-pipeline](https://github.com/your-username/streaming-data-pipeline) است.
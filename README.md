# 🚀 Streaming Data Pipeline

<div align="center">

**یک خط لوله‌ی داده‌ی زنده با Kafka و ClickHouse بر اساس معماری مدالیون**

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue)](https://www.python.org/)
[![Kafka](https://img.shields.io/badge/Kafka-3.9-black)](https://kafka.apache.org/)
[![ClickHouse](https://img.shields.io/badge/ClickHouse-24.8-yellow)](https://clickhouse.com/)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

</div>

---

## 📖 درباره‌ی پروژه

این پروژه یک **خط لوله‌ی داده‌ی جریانی (Streaming Data Pipeline)** کامل است که داده‌های زنده‌ی **تغییرات اخیر ویکی‌مدیا** را از طریق Kafka دریافت کرده و در ClickHouse با **معماری مدالیون (Medallion Architecture)** ذخیره و پردازش می‌کند.

هدف این پروژه، تمرین عملی مفاهیم کلیدی مهندسی داده است:

- ✅ Kafka و Kafka Connect
- ✅ ClickHouse و موتورهای MergeTree
- ✅ Materialized View و Kafka Engine
- ✅ معماری مدالیون (Bronze → Silver → Gold)
- ✅ مدیریت خطا و Retry
- ✅ Idempotency و Exactly-Once Semantics
- ✅ Docker و شبکه‌های کانتینری

---

## 🏛️ معماری

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
                                                   │  BI / Metabase     │
                                                   └────────────────────┘
```

---

## 🥉🥈🥇 معماری مدالیون

این پروژه از **معماری مدالیون** پیروی می‌کند که در آن داده‌ها در سه لایه‌ی متوالی پردازش می‌شوند:

### 🥉 لایه‌ی Bronze (خام)

- **هدف:** ذخیره‌ی داده‌ی خام ۱۰۰٪ بدون هیچ تبدیلی.
- **جداول:** `bronze_wiki_events`, `bronze_wiki_events_errors`
- **ویژگی:** حفظ تمام فیلدهای اصلی رویداد + متادیتای Kafka (topic, partition, offset, timestamp)
- **TTL:** ۳۰ روز
- **مصرف‌کننده:** مهندسین داده (برای دیباگ و Replay)

### 🥈 لایه‌ی Silver (پاک‌سازی‌شده)

- **هدف:** تجزیه‌ی JSON خام به فیلدهای ساخت‌یافته، اعتبارسنجی، رفع تکرار.
- **جداول:** `silver_wiki_events`
- **ویژگی:** فیلدهای استخراج‌شده + تبدیل نوع + غنی‌سازی (استخراج زبان از ویکی)
- **TTL:** ۹۰ روز
- **مصرف‌کننده:** تحلیل‌گران و دانشمندان داده

### 🥇 لایه‌ی Gold (تجاری)

- **هدف:** تجمیع داده و ساخت KPI برای مصرف مستقیم BI.
- **جداول:** `gold_wiki_hourly_stats`, `gold_top_users_daily`, `gold_top_pages_hourly`, `gold_language_hourly`
- **ویژگی:** آمار تجمیعی، بهینه برای کوئری‌های سریع
- **TTL:** ۱۸۰-۳۶۵ روز
- **مصرف‌کننده:** مدیران، BI، ML

---

## 📁 ساختار پروژه

```
stream-realtime-data/
│
├── 📁 sql/                          # اسکریپت‌های SQL برای ساخت لایه‌ها
│   ├── 01-bronze-layer.sql          # لایه‌ی Bronze (خام)
│   ├── 02-silver-layer.sql          # لایه‌ی Silver (پاک‌سازی‌شده)
│   ├── 03-gold-layer.sql            # لایه‌ی Gold (تجمیعی) [به‌زودی]
│   └── 99-utilities.sql             # کوئری‌های مانیتورینگ و دیباگ
│
├── 📁 docs/                         # مستندات تفصیلی
│   ├── clickhouse-consumer.md       # راهنمای ClickHouse به عنوان Consumer
│   └── medallion-architecture.md    # توضیح کامل معماری مدالیون [به‌زودی]
│
├── 📁 init-db/                      # (حذف شده - با sql/ جایگزین شد)
│
├── 🐍 producer.py                   # Producer پایتون (ویکی‌مدیا → Kafka)
├── 🐳 docker-compose.yaml           # زیرساخت (ClickHouse + Kafka + Kafka UI)
├── 📄 requirements.txt              # وابستگی‌های پایتون
├── 📄 .env.example                  # نمونه‌ی متغیرهای محیطی
├── 📄 .gitignore                    # فایل‌های نادیده‌گرفته‌شده
├── 📄 README.md                     # همین فایل
└── 📄 sample.json                   # نمونه‌ی داده‌ی ویکی‌مدیا
```

---

## 🚀 راه‌اندازی سریع

### پیش‌نیازها

| ابزار | نسخه‌ی پیشنهادی | توضیح |
|---|---|---|
| **Docker Desktop** | 24.0+ | برای اجرای کانتینرها |
| **Python** | 3.10+ | برای اجرای Producer |
| **Git** | 2.30+ | برای Clone کردن پروژه |
| **DBeaver** یا **PyCharm Pro** | آخرین نسخه | برای اجرای کوئری‌های SQL |

### گام ۱: Clone کردن پروژه

```bash
git clone https://github.com/YOUR-USERNAME/stream-realtime-data.git
cd stream-realtime-data
```

### گام ۲: بالا آوردن زیرساخت

```bash
docker compose up -d
```

**سرویس‌های بالا آمده:**

| سرویس | پورت | آدرس |
|---|---|---|
| ClickHouse HTTP | 8123 | http://localhost:8123 |
| ClickHouse Native | 9000 | - |
| Kafka | 9092 | - |
| Kafka UI | 8082 | http://localhost:8082 |

**تایید:**

```bash
docker ps
```

باید سه کانتینر `clickhouse`, `kafka-broker`, `kafka-ui` را ببینید.

### گام ۳: ساخت تاپیک Kafka

```bash
docker exec -it kafka-broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:29092 \
  --create --topic wiki-events \
  --partitions 3 --replication-factor 1
```

**تایید:**

```bash
docker exec -it kafka-broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:29092 \
  --list
```

باید `wiki-events` را ببینید.

### گام ۴: ساخت لایه‌های ClickHouse

در **DBeaver** یا **PyCharm**:

1. اتصال به ClickHouse:
   - **Host:** `localhost`
   - **Port:** `8123`
   - **User:** `admin`
   - **Password:** `admin123`
   - **Database:** `tutorial`

2. اجرای فایل‌ها به ترتیب:
   - `sql/01-bronze-layer.sql` → لایه‌ی Bronze
   - `sql/02-silver-layer.sql` → لایه‌ی Silver
   - `sql/03-gold-layer.sql` → لایه‌ی Gold (به‌زودی)

**تایید:**

```sql
SHOW TABLES FROM tutorial;
```

باید ببینید:
```
bronze_wiki_events
bronze_wiki_events_errors
bronze_wiki_events_errors_mv
bronze_wiki_events_mv
silver_wiki_events
silver_wiki_events_mv
wiki_events_queue
```

### گام ۵: نصب وابستگی‌های پایتون

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# Linux/Mac
source .venv/bin/activate

pip install -r requirements.txt
```

> **💡 نکته برای کاربران ایرانی:** اگر با خطای تحریم مواجه شدید، از میرور داخلی استفاده کنید:
> ```bash
> pip install -r requirements.txt \
>   -i https://mirror-pypi.runflare.com/simple/ \
>   --trusted-host mirror-pypi.runflare.com
> ```

### گام ۶: اجرای Producer

```bash
python producer.py
```

**خروجی مورد انتظار:**

```
2026-10-07 12:00:00,123 [INFO] producer: Starting Wikimedia → Kafka producer.
2026-10-07 12:00:00,456 [INFO] producer: Connected to Kafka at localhost:9092.
2026-10-07 12:00:01,234 [INFO] producer: Connected to Wikimedia. Streaming...
2026-10-07 12:00:01,345 [INFO] producer: Sent -> Python (programming language) | SomeUser
...
```

### گام ۷: بررسی داده‌ها

**در DBeaver:**

```sql
-- تعداد رکوردهای Bronze
SELECT count() FROM tutorial.bronze_wiki_events;

-- تعداد رکوردهای Silver
SELECT count() FROM tutorial.silver_wiki_events;

-- نمونه داده‌ها
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

**در Kafka UI:**
- مرورگر: http://localhost:8082
- Topics → `wiki-events` → Messages

---

## 🔌 منابع داده

**منبع اصلی این پروژه:** [Wikimedia EventStreams](https://stream.wikimedia.org/v2/stream/recentchange)

- جریان زنده‌ی ویرایش‌های ویکی‌پدیا و پروژه‌های خواهر
- حدود ۴۰-۵۰ رویداد در ثانیه
- فرمت: Server-Sent Events (SSE) + JSON
- بدون نیاز به API Key

**منابع جایگزین برای تمرین:**

| منبع | نوع | نیاز به Key |
|---|---|---|
| [Coinbase WebSocket](https://docs.cdp.coinbase.com/exchange/docs/websocket-overview) | ارز دیجیتال | خیر |
| [Open-Meteo](https://open-meteo.com/) | آب و هوا | خیر |
| [AISStream.io](https://aisstream.io/) | موقعیت کشتی‌ها | بله (رایگان) |
| [Finnhub](https://finnhub.io/) | بازار سهام | بله (رایگان) |

---

## 📚 مستندات

| مستند | توضیح |
|---|---|
| [ClickHouse as a Stream Consumer](docs/clickhouse-consumer.md) | راهنمای کامل استفاده از ClickHouse به عنوان Consumer |
| [Medallion Architecture](docs/medallion-architecture.md) | توضیح معماری مدالیون و تصمیمات طراحی |

---

## 🛠️ تکنولوژی‌ها

| تکنولوژی | نسخه | کاربرد |
|---|---|---|
| **Apache Kafka** | 3.9 (KRaft) | پیام‌رسان توزیع‌شده |
| **ClickHouse** | 24.8 | پایگاه داده‌ی ستونی |
| **Kafka UI** | latest | رابط کاربری Kafka |
| **Python** | 3.10+ | Producer |
| **Docker** | 24.0+ | اجرای کانتینرها |
| **kafka-python** | 3.0+ | کلاینت Kafka در پایتون |
| **requests** | 2.31+ | اتصال به Wikimedia SSE |

---

## 🎯 مفاهیمی که یاد می‌گیرید

- **Kafka**: Producer, Consumer, Topic, Partition, Consumer Group, KRaft
- **ClickHouse**: MergeTree, ReplacingMergeTree, SummingMergeTree, Kafka Engine, Materialized View
- **معماری مدالیون**: Bronze, Silver, Gold
- **داده‌های جریانی**: SSE, Backpressure, Idempotency, Exactly-Once
- **مهندسی داده**: ETL, Data Pipeline, Orchestration
- **Docker**: Networking, Volumes, Healthcheck, Docker Compose
- **SQL پیشرفته**: Window Functions, JSON Extraction, Aggregation

---

## 🧪 کوئری‌های تحلیلی نمونه

### پرکارترین کاربران انسانی

```sql
SELECT user, count() AS edits
FROM tutorial.silver_wiki_events
WHERE user_is_bot = 0 AND user_is_anonymous = 0
GROUP BY user
ORDER BY edits DESC
LIMIT 10;
```

### نسبت ربات/انسان/ناشناس

```sql
SELECT
    countIf(user_is_bot = 1) AS bots,
    countIf(user_is_bot = 0 AND user_is_anonymous = 0) AS humans,
    countIf(user_is_anonymous = 1) AS anonymous,
    count() AS total
FROM tutorial.silver_wiki_events;
```

### توزیع زبانی

```sql
SELECT language, count() AS events
FROM tutorial.silver_wiki_events
GROUP BY language
ORDER BY events DESC
LIMIT 15;
```

### بزرگ‌ترین تغییرات صفحات

```sql
SELECT title, user, length_delta, event_timestamp
FROM tutorial.silver_wiki_events
WHERE length_delta > 0
ORDER BY length_delta DESC
LIMIT 10;
```

---

## 🗺️ نقشه‌ی راه (Roadmap)

- [x] لایه‌ی Bronze (داده‌ی خام)
- [x] لایه‌ی Silver (داده‌ی پاک‌سازی‌شده)
- [ ] لایه‌ی Gold (داده‌ی تجمیعی)
- [ ] مستندات کامل معماری مدالیون
- [ ] مصورسازی با Metabase یا Grafana
- [ ] هشدار با Prometheus
- [ ] Containerize کردن Producer (Dockerfile)
- [ ] GitHub Actions برای CI/CD
- [ ] تست‌های خودکار با pytest
- [ ] dbt برای Transformation
- [ ] Airflow برای Orchestration

---

## 🤝 مشارکت

اگر می‌خواهید در این پروژه مشارکت کنید:

1. Fork کنید.
2. یک برنچ جدید بسازید (`git checkout -b feature/amazing-feature`).
3. تغییرات را Commit کنید (`git commit -m 'Add amazing feature'`).
4. Push کنید (`git push origin feature/amazing-feature`).
5. یک Pull Request باز کنید.

---

## 📝 مجوز

این پروژه تحت مجوز **MIT** منتشر شده است. برای جزئیات، فایل [LICENSE](LICENSE) را ببینید.

---

## 🙏 تشکر

- [Wikimedia EventStreams](https://stream.wikimedia.org/) برای داده‌ی زنده‌ی رایگان
- [ClickHouse](https://clickhouse.com/) برای موتور تحلیلی فوق‌سریع
- [Apache Kafka](https://kafka.apache.org/) برای پیام‌رسان مقاوم

---

<div align="center">

**ساخته شده با ❤️ برای یادگیری مهندسی داده**

</div>
"""
Wikimedia Recent Changes → Kafka Producer (Bronze Layer)

این اسکریپت به جریان زنده‌ی ویکی‌مدیا متصل می‌شود و کل رویداد JSON
را بدون هیچ تبدیلی به تاپیک Kafka ارسال می‌کند.

طراحی برای معماری مدالیون:
    - لایه‌ی Bronze: ذخیره‌ی داده‌ی خام ۱۰۰٪ بدون دست‌کاری
    - هیچ فیلدی حذف نمی‌شود، هیچ تبدیلی انجام نمی‌شود
    - پردازش و استخراج فیلدها در لایه‌های Silver و Gold انجام می‌شود

قابلیت‌ها:
    - اتصال خودکار مجدد در صورت قطع شدن جریان (Exponential Backoff)
    - Idempotent Producer برای جلوگیری از ارسال تکراری
    - مدیریت تمیز Ctrl+C و SIGTERM
    - لاگ‌گیری همزمان در کنسول و فایل
"""

import json
import logging
import signal
import sys
import time
from typing import Optional

import requests
from kafka import KafkaProducer
from kafka.serializer import Serializer


# ============================================================================
# تنظیمات
# ============================================================================

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "wiki-events"
WIKI_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

WIKI_HEADERS = {
    "User-Agent": "StreamingDataPipeline/1.0 (https://example.com; your-email@example.com) Tutorial",
    "Accept": "text/event-stream",
}

# --- Retry و Reconnect ---
RECONNECT_DELAY_SECONDS = 60          # تأخیر پایه قبل از اتصال مجدد
MAX_RECONNECT_DELAY_SECONDS = 300     # سقف تأخیر (۵ دقیقه)
KAFKA_RETRY_DELAY_SECONDS = 5         # تأخیر بین تلاش‌های اتصال به Kafka
KAFKA_MAX_RETRIES = 10                # حداکثر تلاش برای اتصال اولیه به Kafka
SEND_TIMEOUT_SECONDS = 10             # تایم‌اوت انتظار برای تأیید ارسال هر پیام

# --- لاگ ---
LOG_FILE = "producer.log"
LOG_LEVEL = logging.INFO
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


# ============================================================================
# پیکربندی لاگ
# ============================================================================

logging.basicConfig(
    level=LOG_LEVEL,
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
logger = logging.getLogger("producer")


# ============================================================================
# مدیریت خروج تمیز
# ============================================================================

shutdown_requested = False


def _handle_shutdown(signum, frame) -> None:
    """Signal handler برای Ctrl+C و SIGTERM."""
    global shutdown_requested
    logger.info("Shutdown requested (signal %s). Finishing current batch...", signum)
    shutdown_requested = True


signal.signal(signal.SIGINT, _handle_shutdown)
signal.signal(signal.SIGTERM, _handle_shutdown)


# ============================================================================
# Serializerها (سازگار با kafka-python 3.0+)
# ============================================================================

class JsonSerializer(Serializer):
    """تبدیل dict پایتون به JSON bytes با پشتیبانی از Unicode."""

    def serialize(self, topic, headers, data):
        return json.dumps(data, ensure_ascii=False).encode("utf-8")


class KeySerializer(Serializer):
    """تبدیل کلید رشته‌ای به bytes."""

    def serialize(self, topic, headers, data):
        return data.encode("utf-8") if data else None


# ============================================================================
# ساخت Kafka Producer
# ============================================================================

def create_producer(retries: int = KAFKA_MAX_RETRIES) -> Optional[KafkaProducer]:
    """
    اتصال به Kafka با تلاش مجدد.

    Args:
        retries: حداکثر تعداد تلاش.

    Returns:
        KafkaProducer در صورت موفقیت، None در صورت شکست کامل.
    """
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=JsonSerializer(),
                key_serializer=KeySerializer(),
                # --- تنظیمات دوام و Idempotency ---
                acks="all",
                retries=5,
                enable_idempotence=True,
                max_in_flight_requests_per_connection=5,
                # --- بهینه‌سازی ---
                linger_ms=100,
                compression_type="gzip",
                # --- Timeoutها ---
                max_block_ms=30000,
                request_timeout_ms=30000,
            )
            logger.info("Connected to Kafka at %s.", KAFKA_BOOTSTRAP)
            return producer
        except Exception as exc:
            logger.warning(
                "Kafka connection attempt %d/%d failed: %s. Retrying in %ds...",
                attempt, retries, exc, KAFKA_RETRY_DELAY_SECONDS,
            )
            time.sleep(KAFKA_RETRY_DELAY_SECONDS)

    logger.error("Could not connect to Kafka after %d attempts.", retries)
    return None


# ============================================================================
# استخراج کلید پیام
# ============================================================================

def extract_key(event: dict) -> str:
    """
    کلید پیام را از رویداد استخراج می‌کند.

    ترتیب اولویت:
        1. meta.id  (شناسه منحصربه‌فرد رویداد در ویکی‌مدیا)
        2. title    (عنوان صفحه)
        3. id       (شناسه قدیمی)
        4. رشته خالی

    نکته: استفاده از کلید باعث می‌شود پیام‌های یک صفحه به یک پارتیشن
    بروند و ترتیب رویدادها برای آن صفحه حفظ شود.
    """
    meta = event.get("meta") or {}
    if isinstance(meta, dict) and meta.get("id"):
        return str(meta["id"])

    if event.get("title"):
        return str(event["title"])

    if event.get("id"):
        return str(event["id"])

    return ""


# ============================================================================
# پردازش یک رویداد
# ============================================================================

def process_event(producer: KafkaProducer, line: bytes, stats: dict) -> None:
    """
    یک خط از جریان SSE را پردازش و کل رویداد را به Kafka ارسال می‌کند.

    در این نسخه (Bronze)، هیچ تبدیلی انجام نمی‌شود و کل رویداد
    به صورت JSON خام به Kafka فرستاده می‌شود.
    """
    decoded = line.decode("utf-8")

    # جریان SSE: فقط خطوطی که با "data: " شروع می‌شوند حاوی رویداد هستند
    if not decoded.startswith("data: "):
        return

    try:
        event = json.loads(decoded[6:])  # حذف پیشوند "data: "
    except json.JSONDecodeError:
        stats["skipped"] += 1
        logger.debug("Skipped malformed JSON line.")
        return

    # استخراج کلید پیام
    key = extract_key(event)

    try:
        future = producer.send(TOPIC, key=key, value=event)
        future.get(timeout=SEND_TIMEOUT_SECONDS)
        stats["sent"] += 1

        # لاگ خلاصه هر ۱۰۰ پیام
        if stats["sent"] % 100 == 0:
            logger.info(
                "Progress: %d sent, %d skipped, %d failed.",
                stats["sent"], stats["skipped"], stats["failed"],
            )
        else:
            # لاگ خلاصه برای هر پیام (فقط عنوان و کاربر)
            title = event.get("title") or "(no title)"
            user = event.get("user") or "(no user)"
            logger.info("Sent -> %s | %s", title, user)

    except Exception as exc:
        stats["failed"] += 1
        logger.error("Send failed for key=%r: %s", key, exc)


# ============================================================================
# حلقه اصلی اتصال به ویکی‌مدیا
# ============================================================================

def _sleep_with_interrupt(seconds: int) -> None:
    """خواب تکه‌تکه تا Ctrl+C فوراً پاسخ دهد."""
    logger.info("Sleeping for %d seconds before reconnect...", seconds)
    for _ in range(seconds):
        if shutdown_requested:
            return
        time.sleep(1)


def stream_from_wikimedia(producer: KafkaProducer) -> None:
    """
    به جریان SSE ویکی‌مدیا وصل می‌شود و در صورت قطع، دوباره تلاش می‌کند.
    """
    attempt = 0
    delay = RECONNECT_DELAY_SECONDS

    while not shutdown_requested:
        attempt += 1
        logger.info("Connecting to Wikimedia stream (attempt #%d)...", attempt)

        try:
            with requests.get(
                WIKI_STREAM_URL,
                headers=WIKI_HEADERS,
                stream=True,
                timeout=(10, 90),   # (connect timeout, read timeout)
            ) as response:
                response.raise_for_status()
                logger.info("Connected to Wikimedia. Streaming...")

                # بعد از اتصال موفق، تأخیر را ریست کن
                delay = RECONNECT_DELAY_SECONDS
                stats = {"sent": 0, "skipped": 0, "failed": 0}

                for line in response.iter_lines():
                    if shutdown_requested:
                        break
                    if not line:
                        continue
                    process_event(producer, line, stats)

                logger.info(
                    "Stream closed. Session stats: %d sent, %d skipped, %d failed.",
                    stats["sent"], stats["skipped"], stats["failed"],
                )

        except requests.exceptions.HTTPError as exc:
            logger.error("HTTP error: %s. Retrying in %ds...", exc, delay)
        except requests.exceptions.ChunkedEncodingError as exc:
            logger.warning("Chunked encoding broken: %s. Retrying in %ds...", exc, delay)
        except requests.exceptions.ConnectionError as exc:
            logger.warning("Connection error: %s. Retrying in %ds...", exc, delay)
        except requests.exceptions.Timeout:
            logger.warning("Request timeout. Retrying in %ds...", delay)
        except Exception as exc:
            logger.exception("Unexpected error: %s. Retrying in %ds...", exc, delay)

        if shutdown_requested:
            break

        _sleep_with_interrupt(delay)

        # Exponential backoff با سقف
        delay = min(delay * 2, MAX_RECONNECT_DELAY_SECONDS)


# ============================================================================
# تابع اصلی
# ============================================================================

def main() -> int:
    """نقطه ورود برنامه."""
    logger.info("Starting Wikimedia → Kafka producer (Bronze layer).")

    producer = create_producer()
    if producer is None:
        logger.error("Exiting: could not connect to Kafka.")
        return 1

    try:
        stream_from_wikimedia(producer)
    finally:
        logger.info("Flushing and closing Kafka producer...")
        try:
            producer.flush(timeout=30)
            producer.close(timeout=30)
        except Exception as exc:
            logger.error("Error while closing producer: %s", exc)
        logger.info("Producer stopped. Goodbye.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
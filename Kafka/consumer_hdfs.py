"""
Consumer — чтение сообщений из Kafka и локальное сохранение JSON.

Читает сообщения из топика raw-data и сохраняет их как JSON-файлы
в локальную папку temp_hdfs/ с разделением по источнику (batch/realtime).

После завершения:
    1. Скопируйте temp_hdfs/ в общую папку VirtualBox
    2. На Cloudera VM запустите: hadoop/upload_to_hdfs.sh
"""

import json
import logging
import os
import signal
from datetime import datetime
from pathlib import Path

from kafka import KafkaConsumer
from kafka.errors import KafkaError

# --- Настройки ---
KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
TOPIC = "raw-data"
LOCAL_OUTPUT_DIR = "temp_hdfs"
LOG_DIR = "logs"
BATCH_SIZE = 50  # Логировать каждые N сообщений

# --- Настройка логирования ---
Path(LOG_DIR).mkdir(exist_ok=True)

log_filename = f"consumer_hdfs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, log_filename), encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Флаг для корректного завершения
running = True


def signal_handler(sig, frame):
    """Обработчик Ctrl+C / SIGTERM для корректного завершения."""
    global running
    logger.info("Получен сигнал завершения. Останавливаю consumer...")
    running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def create_consumer() -> KafkaConsumer:
    """Создаёт и возвращает KafkaConsumer.

    Returns:
        Настроенный экземпляр KafkaConsumer.

    Raises:
        KafkaError: При невозможности подключиться к Kafka.
    """
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            consumer_timeout_ms=5000  # Таймаут для проверки флага running
        )
        logger.info(f"Consumer подключён к топику '{TOPIC}'")
        return consumer
    except KafkaError as e:
        logger.error(f"Не удалось подключиться к Kafka: {e}")
        raise


def get_local_path(source: str, rating: str, index: int) -> Path:
    """Формирует локальный путь для сохранения файла.

    Структура повторяет будущую структуру в HDFS:
        temp_hdfs/source=batch/rating=5/date=2026-04-09/review_123456_000001.json

    Args:
        source: Источник данных (batch/realtime).
        rating: Оценка отзыва (1-5).
        index: Порядковый номер сообщения.

    Returns:
        Путь к файлу для сохранения.
    """
    today = datetime.now().strftime("%Y-%m-%d")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    dir_path = Path(LOCAL_OUTPUT_DIR) / f"source={source}" / f"rating={rating}" / f"date={today}"
    dir_path.mkdir(parents=True, exist_ok=True)

    return dir_path / f"review_{timestamp}_{index:06d}.json"


def process_messages(consumer: KafkaConsumer):
    """Читает сообщения из Kafka и сохраняет локально.

    Args:
        consumer: Экземпляр KafkaConsumer.
    """
    global running

    Path(LOCAL_OUTPUT_DIR).mkdir(exist_ok=True)

    total_processed = 0
    total_errors = 0

    logger.info("=" * 60)
    logger.info("Ожидание сообщений из Kafka...")
    logger.info("Для остановки нажмите Ctrl+C")
    logger.info("=" * 60)

    while running:
        try:
            for message in consumer:
                if not running:
                    break

                try:
                    review = message.value
                    source = review.get("source", "unknown")
                    rating = review.get("rating", "unknown")

                    # Сохраняем в локальный файл
                    local_path = get_local_path(source, rating, total_processed)
                    with open(local_path, "w", encoding="utf-8") as f:
                        json.dump(review, f, ensure_ascii=False, indent=2)

                    total_processed += 1

                    if total_processed % BATCH_SIZE == 0:
                        logger.info(f"Сохранено: {total_processed} отзывов → {LOCAL_OUTPUT_DIR}/")

                except json.JSONDecodeError as e:
                    logger.error(f"Ошибка декодирования JSON: {e}")
                    total_errors += 1
                except Exception as e:
                    logger.error(f"Ошибка обработки сообщения: {e}")
                    total_errors += 1

        except Exception as e:
            logger.error(f"Ошибка при чтении из Kafka: {e}")

    # Итоговая статистика
    logger.info("")
    logger.info("=" * 60)
    logger.info("ЗАВЕРШЕНО")
    logger.info("=" * 60)
    logger.info(f"Сохранено:    {total_processed} отзывов")
    logger.info(f"Ошибок:       {total_errors}")
    logger.info(f"Папка:        {Path(LOCAL_OUTPUT_DIR).resolve()}")
    logger.info("")
    logger.info("СЛЕДУЮЩИЕ ШАГИ:")
    logger.info("1. Скопируйте папку temp_hdfs/ в общую папку VirtualBox")
    logger.info("2. На Cloudera VM:")
    logger.info("   hdfs dfs -mkdir -p /user/cloudera/raw_data/")
    logger.info("   hdfs dfs -put -f /media/sf_*/temp_hdfs/* /user/cloudera/raw_data/")
    logger.info("")
    logger.info("3. Проверьте загрузку:")
    logger.info("   hdfs dfs -ls -R /user/cloudera/raw_data/ | grep review | wc -l")
    logger.info("=" * 60)


def main():
    """Точка входа."""
    logger.info("Запуск Consumer (локальное сохранение)...")
    logger.info(f"Топик: {TOPIC}")
    logger.info(f"Папка сохранения: {LOCAL_OUTPUT_DIR}")

    try:
        consumer = create_consumer()
        process_messages(consumer)
    except KeyboardInterrupt:
        logger.info("Прервано пользователем (Ctrl+C)")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
    finally:
        if "consumer" in locals():
            consumer.close()
            logger.info("Consumer закрыт")


if __name__ == "__main__":
    main()

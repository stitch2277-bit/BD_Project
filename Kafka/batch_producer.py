"""
Batch Producer — отправка отзывов из датасета в Apache Kafka.

Читает текстовые файлы отзывов из папок dataset/<rating>/ и отправляет
каждый отзыв как JSON-сообщение в топик raw-data.

Использование:
    python batch_producer.py              # Отправить все отзывы
    python batch_producer.py --limit 10   # Отправить первые 10 (для теста)
"""

import argparse
import json
import logging
import os
from datetime import datetime
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

# --- Настройки по умолчанию ---
KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
TOPIC = "raw-data"
DATASET_PATH = "../../dataset"  # Относительный путь от kafka/
LOG_DIR = "logs"

# --- Настройка логирования ---
Path(LOG_DIR).mkdir(exist_ok=True)

log_filename = f"batch_producer_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, log_filename), encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def parse_review_file(file_path: Path, rating: str) -> dict:
    """Парсит текстовый файл отзыва и возвращает словарь с данными.

    Args:
        file_path: Путь к файлу отзыва.
        rating: Оценка из имени папки (1-5).

    Returns:
        Словарь с полями отзыва или None при ошибке.
    """
    try:
        content = file_path.read_text(encoding="utf-8")

        title = ""
        author = ""
        date = ""
        review_rating = rating
        link = ""
        text_parts = []

        lines = content.split("\n")
        current_section = None

        for line in lines:
            line = line.strip()

            if "ЗАГОЛОВОК:" in line:
                title = line.replace("ЗАГОЛОВОК:", "").strip()
            elif "Автор:" in line:
                author = line.replace("Автор:", "").strip()
            elif "Дата:" in line:
                date = line.replace("Дата:", "").strip()
            elif "Рейтинг:" in line:
                review_rating = line.replace("Рейтинг:", "").strip().split()[0]
            elif "Ссылка:" in line:
                link = line.replace("Ссылка:", "").strip()
            elif "ТЕКСТ ОТЗЫВА:" in line:
                current_section = "text"
                continue
            elif current_section == "text" and line:
                text_parts.append(line)
            elif "Сохранено:" in line:
                break

        return {
            "source": "batch",
            "rating": review_rating,
            "title": title,
            "author": author,
            "date": date,
            "link": link,
            "text": " ".join(text_parts).strip(),
            "filename": file_path.name,
            "class_folder": rating
        }

    except Exception as e:
        logger.error(f"Ошибка парсинга файла {file_path}: {e}")
        return None


def create_producer() -> KafkaProducer:
    """Создаёт и возвращает KafkaProducer.

    Returns:
        Настроенный экземпляр KafkaProducer.

    Raises:
        KafkaError: При невозможности подключиться к Kafka.
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
            max_in_flight_requests_per_connection=1
        )
        logger.info(f"Producer подключён к {KAFKA_BOOTSTRAP_SERVER}")
        return producer
    except KafkaError as e:
        logger.error(f"Не удалось подключиться к Kafka: {e}")
        raise


def send_reviews(producer: KafkaProducer, dataset_path: str, limit: int = None):
    """Читает отзывы из датасета и отправляет в Kafka.

    Args:
        producer: Экземпляр KafkaProducer.
        dataset_path: Путь к папке с датасетом.
        limit: Максимальное количество отзывов (None = без ограничений).
    """
    dataset = Path(dataset_path)
    if not dataset.exists():
        logger.error(f"Папка датасета не найдена: {dataset_path}")
        logger.info(f"Текущая рабочая директория: {os.getcwd()}")
        return

    total_sent = 0
    total_errors = 0

    for rating_folder in sorted(dataset.iterdir()):
        if not rating_folder.is_dir():
            continue

        rating = rating_folder.name
        logger.info(f"Обработка папки с оценками: {rating}")

        files = sorted(rating_folder.glob("*.txt"))
        logger.info(f"  Найдено файлов: {len(files)}")

        for file_path in files:
            # Проверка лимита
            if limit is not None and total_sent >= limit:
                logger.info(f"Достигнут лимит: {limit} отзывов")
                producer.flush()
                logger.info("=" * 60)
                logger.info(f"Готово! Отправлено: {total_sent}, Ошибок: {total_errors}")
                return

            review = parse_review_file(file_path, rating)
            if review is None:
                total_errors += 1
                continue

            try:
                future = producer.send(
                    TOPIC,
                    key="source_batch",
                    value=review
                )
                # Ждём подтверждения отправки
                future.get(timeout=10)
                total_sent += 1

                if total_sent % 100 == 0:
                    logger.info(f"Отправлено: {total_sent} отзывов...")

            except KafkaError as e:
                logger.error(f"Ошибка отправки {file_path.name}: {e}")
                total_errors += 1

    # Финальная отправка всех буферизованных сообщений
    producer.flush()
    logger.info("=" * 60)
    logger.info(f"Готово! Отправлено: {total_sent}, Ошибок: {total_errors}")


def parse_args():
    """Парсит аргументы командной строки."""
    parser = argparse.ArgumentParser(description="Batch Producer для отправки отзывов в Kafka")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Максимальное количество отзывов для отправки (для тестирования)"
    )
    parser.add_argument(
        "--dataset",
        type=str,
        default=DATASET_PATH,
        help=f"Путь к датасету (по умолчанию: {DATASET_PATH})"
    )
    return parser.parse_args()


def main():
    """Точка входа."""
    args = parse_args()

    logger.info("Запуск Batch Producer...")
    logger.info(f"Датасет: {args.dataset}")
    logger.info(f"Топик: {TOPIC}")
    if args.limit:
        logger.info(f"Лимит: {args.limit} отзывов (тестовый режим)")

    try:
        producer = create_producer()
        send_reviews(producer, args.dataset, args.limit)
    except KeyboardInterrupt:
        logger.info("Прервано пользователем (Ctrl+C)")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
    finally:
        if "producer" in locals():
            producer.close()
            logger.info("Producer закрыт")


if __name__ == "__main__":
    main()

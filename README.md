## Описание

Проект реализует полный конвейер обработки больших данных:

```
Web-Scraping → Kafka → HDFS → MapReduce/Hive/Pig → Визуализация
```

**Данные:** Отзывы с сайта Otzovik, разделённые на 5 классов (оценки 1–5).

---

## Архитектура

```
┌──────────────────────────────────────────────────────┐
│  Windows (хост)                                      │
│                                                      │
│  ┌──────────────┐    ┌────────────────────────────┐  │
│  │  Docker      │    │  Python скрипты            │  │
│  │  Kafka :9092 │◄──►│  batch_producer.py         │  │
│  │              │    │  consumer_hdfs.py          │  │
│  │              │    │                            │  │
│  │              │    │  temp_hdfs/ ← JSON файлы   │  │
│  └──────────────┘    └──────────┬─────────────────┘  │
└─────────────────────────────────┼────────────────────┘
                                  │
                         VirtualBox Shared Folder
                                  ▼
┌──────────────────────────────────────────────────────┐
│  Cloudera VM (Oracle VirtualBox)                     │
│                                                      │
│  hdfs dfs -put → HDFS: /user/cloudera/raw_data/      │
│                                                      │
│  Далее: MapReduce / Hive / Pig                       │
└──────────────────────────────────────────────────────┘
```

---

## Структура проекта

```
BD_Project/
├── Kafka/
│   ├── batch_producer.py   # Отправка отзывов в Kafka
│   ├── consumer_hdfs.py    # Чтение из Kafka → локальные JSON
│   ├── logs/               # Логи запусков
│   ├── temp_hdfs/          # Временные JSON-файлы (не для Git)
│   └── README.md           # Инструкции по запуску
├── dataset/                # Датасет отзывов (не для Git)
│   ├── 1/                  # Отзывы с оценкой 1
│   ├── 2/                  # Отзывы с оценкой 2
│   ├── 3/                  # Отзывы с оценкой 3
│   ├── 4/                  # Отзывы с оценкой 4
│   └── 5/                  # Отзывы с оценкой 5
├── .gitignore              # Исключения для Git
└── README.md               # Этот файл
```

---

## Быстрый старт

### 1. Установка зависимостей

```bash
pip install kafka-python
```

### 2. Запуск Kafka (Docker)

```bash
docker start kafka
```

### 3. Создание топика

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic raw-data \
  --partitions 1 --replication-factor 1
```

### 4. Запуск конвейера

**Терминал 1 (Consumer):**
```bash
cd Kafka
python consumer_hdfs.py
```

**Терминал 2 (Producer):**
```bash
cd Kafka
python batch_producer.py
```

### 5. Перенос в Cloudera VM

Скопируйте `Kafka/temp_hdfs/` в общую папку VirtualBox, затем на VM:

```bash
hdfs dfs -mkdir -p /user/cloudera/raw_data/
hdfs dfs -put -f /media/sf_*/temp_hdfs/* /user/cloudera/raw_data/
```

### 6. Проверка в HDFS

```bash
hdfs dfs -ls -R /user/cloudera/raw_data/ | grep review | wc -l
```

---

## Технологии

| Компонент | Технология | Где работает |
|-----------|-----------|-------------|
| Потоковая передача | Apache Kafka | Docker (Windows) |
| Обработка | Python 3 | Windows |
| Хранение | HDFS | Cloudera VM |
| Аналитика | MapReduce, Hive, Pig | Cloudera VM |
| Визуализация | Matplotlib, Seaborn | Windows |

---

## Данные

| Параметр | Значение |
|----------|----------|
| Источник | Otzovik.com |
| Тип | Текстовые отзывы |
| Классы | 5 (оценки 1–5) |
| Формат | TXT с метаданными |

# config.py
"""Конфигурационные параметры для веб-скрейпинга"""

# Настройки WebDriver
HEADLESS_MODE = True
PAGE_LOAD_TIMEOUT = 15  # Увеличено для надёжности
IMPLICIT_WAIT = 10

# Настройки задержек (КРИТИЧЕСКИ ВАЖНО для otzovik.com)
MIN_DELAY = 8   # Минимум 8 секунд
MAX_DELAY = 12  # Максимум 12 секунд

# Пути к данным
BASE_DATA_DIR = "dataset"
LOG_FILE = "scraper.log"

# URL для парсинга 
BASE_URL = "https://otzovik.com/reviews/sberbank_rossii/"
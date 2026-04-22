import os
import time
import random
import logging
import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class OtzovikScraper:
    def __init__(self, base_url, output_dir, pages_per_rating=2, max_reviews=100):
        self.base_url = base_url
        self.output_dir = output_dir
        self.pages_per_rating = pages_per_rating
        self.max_reviews = max_reviews
        self.stats = {'total': 0, 'saved': 0, 'errors': 0}

    def _setup_dirs(self):
        """Создаёт структуру: temp_hdfs/dataset/{1..5}/"""
        os.makedirs(self.output_dir, exist_ok=True)
        for r in range(1, 6):
            os.makedirs(os.path.join(self.output_dir, str(r)), exist_ok=True)

    def _get_html(self, url, retries=3):
        """Загрузка HTML с задержками и ретраями"""
        headers = {'User-Agent': random.choice([
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
        ])}
        for attempt in range(1, retries + 1):
            try:
                time.sleep(random.uniform(10.0, 12.0))
                resp = requests.get(url, headers=headers, timeout=30)
                resp.raise_for_status()
                if 'captcha' in resp.text.lower() or 'доступ запрещен' in resp.text.lower():
                    logger.warning(f"Защита сайта. Попытка {attempt}/{retries}")
                    time.sleep(15)
                    continue
                return resp.text
            except Exception as e:
                logger.error(f"Ошибка запроса (попытка {attempt}): {e}")
                if attempt == retries: return None
        return None

    def _parse_page(self, html):
        """Извлечение отзывов из HTML"""
        if not html: return []
        soup = BeautifulSoup(html, 'html.parser')
        reviews = []
        
        blocks = soup.find_all('div', class_=['item status4', 'item status10'])
        if not blocks:
            blocks = [item for item in soup.find_all('div', class_='item')
                      if any(c in item.get('class', []) for c in ['status4', 'status10'])]

        for block in blocks:
            try:
                rating_el = block.find('div', class_='rating-score')
                rating = int(''.join(filter(str.isdigit, rating_el.get_text()))) if rating_el else None

                title_el = block.find('a', class_='review-title')
                title = title_el.get_text(strip=True) if title_el else "Без названия"
                link = title_el['href'] if title_el and title_el.has_attr('href') else ""
                if link and not link.startswith('http'):
                    link = f"https://otzovik.com{link}"

                author_el = block.find('span', itemprop='name')
                author = author_el.get_text(strip=True) if author_el else "Аноним"

                date_el = block.find('div', class_='review-postdate')
                date = date_el.get_text(strip=True) if date_el else "Не указана"

                teaser_el = block.find('div', class_='review-teaser')
                teaser = teaser_el.get_text(strip=True) if teaser_el else "Текст отсутствует"

                reviews.append({
                    'rating': rating, 'title': title, 'author': author,
                    'date': date, 'link': link, 'teaser': teaser
                })
            except Exception as e:
                logger.warning(f"Ошибка парсинга блока: {e}")
                self.stats['errors'] += 1
        return reviews

    def _save_review(self, review, rating, idx):
        """Сохранение отзыва в .txt файл"""
        if not rating or rating < 1 or rating > 5:
            return False
            
        path = os.path.join(self.output_dir, str(rating), f"{idx:04d}.txt")
        try:
            text = str(review.get('teaser', 'Нет текста'))
            with open(path, 'w', encoding='utf-8') as f:
                f.write(f"{'='*60}\n")
                f.write(f"ЗАГОЛОВОК: {review['title']}\n")
                f.write(f"{'='*60}\n\n")
                f.write(f"Автор: {review['author']}\nДата: {review['date']}\n")
                f.write(f"Рейтинг: {rating} звезд(ы)\n")
                if review['link']: f.write(f"Ссылка: {review['link']}\n")
                f.write(f"\n{'-'*60}\nТЕКСТ ОТЗЫВА:\n{'-'*60}\n\n{text}\n\n")
                f.write(f"{'='*60}\nСохранено: {time.strftime('%Y-%m-%d %H:%M:%S')}\n{'='*60}\n")
            self.stats['saved'] += 1
            return True
        except Exception as e:
            logger.error(f"Ошибка сохранения {path}: {e}")
            self.stats['errors'] += 1
            return False

    def scrape_rating(self, rating_val):
        """Парсинг страниц для конкретной оценки"""
        logger.info(f"Сбор отзывов с рейтингом: {rating_val}")
        all_reviews = []
        for page in range(1, self.pages_per_rating + 1):
            url = f"{self.base_url}?ratio={rating_val}" if page == 1 else f"{self.base_url}{page}/?ratio={rating_val}"
            html = self._get_html(url)
            if html:
                reviews = self._parse_page(html)
                all_reviews.extend(reviews)
                self.stats['total'] += len(reviews)
            if len(all_reviews) >= self.max_reviews: break
            if page < self.pages_per_rating: time.sleep(random.uniform(8.0, 10.0))
        return all_reviews[:self.max_reviews]

    def run(self):
        """Запуск полного цикла парсинга"""
        self._setup_dirs()
        logger.info(f"Старт парсера. Путь сохранения: {self.output_dir}")
        start_time = time.time()
        
        for r in range(1, 6):
            reviews = self.scrape_rating(r)
            for i, rev in enumerate(reviews, 1):
                self._save_review(rev, r, i)
                
        elapsed = time.time() - start_time
        logger.info(f"Готово за {elapsed:.1f} сек. Собрано: {self.stats['total']}, Сохранено: {self.stats['saved']}, Ошибок: {self.stats['errors']}")

if __name__ == "__main__":
    BASE_PATH = r"D:\Desktop\Корнеева\Корнеева_4 семестр\Основы работы с БД\Проект\BD_Project\Kafka\temp_hdfs"
    OUTPUT_DIR = os.path.join(BASE_PATH, "dataset")
    BASE_URL = "https://otzovik.com/reviews/sberbank_rossii/"
    
    scraper = OtzovikScraper(BASE_URL, OUTPUT_DIR, pages_per_rating=2, max_reviews=100)
    scraper.run()
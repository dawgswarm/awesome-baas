"""
Массовый сбор отзывов с Ozon.

Берёт поисковую выдачу, собирает ссылки на карточки товаров,
затем параллельно парсит отзывы для каждого товара.

Использование:
    venv/bin/python parsers/ozon/ozon_bulk_reviews.py "iphone 15"
    venv/bin/python parsers/ozon/ozon_bulk_reviews.py "iphone 15" --limit 10 --workers 3
    venv/bin/python parsers/ozon/ozon_bulk_reviews.py "https://www.ozon.ru/search/?text=iphone+15"
"""

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from urllib.parse import quote_plus

from dawg_baas import Baas
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

from ozon_reviews import scrape_ozon_reviews, Review


# --- Конфигурация ---
API_KEY = "your_api_key"
PROXY = None  # "http://user:pass@host:port" — если нужен прокси
TARGET_PRODUCTS = 30
WORKERS = 5

# JavaScript для извлечения ссылок на карточки товаров.
EXTRACT_URLS_JS = """(targetCount) => {
    const cards = document.querySelectorAll('[data-index]');
    const urls = [];

    for (const card of cards) {
        if (urls.length >= targetCount) break;

        const link = card.querySelector('a.tile-clickable-element');
        if (!link) continue;

        const href = link.getAttribute('href');
        if (!href || !href.includes('/product/')) continue;

        // Собираем абсолютный URL
        const url = href.startsWith('http')
            ? href
            : 'https://www.ozon.ru' + href;

        // Убираем query-параметры для чистоты
        const clean = url.split('?')[0];
        if (!urls.includes(clean)) {
            urls.push(clean);
        }
    }

    return urls;
}"""


def collect_product_urls(
    api_key: str,
    search_url: str,
    proxy: str | None = None,
    limit: int = 30,
    scroll_pause: float = 2.0,
) -> list[str]:
    """Собирает URL карточек товаров из поисковой выдачи Ozon."""

    baas = Baas(api_key=api_key)

    try:
        print("Создаём браузер для сбора ссылок...")
        ws_url = baas.create(proxy=proxy) if proxy else baas.create()
        print(f"Браузер создан: {baas.browser_id}")

        with sync_playwright() as p:
            browser = p.chromium.connect_over_cdp(ws_url)
            time.sleep(2)

            page = browser.contexts[0].pages[0]

            print(f"Загрузка: {search_url}")
            try:
                page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
            except PlaywrightTimeout:
                print("Таймаут загрузки страницы")
                browser.close()
                return []

            time.sleep(3)

            # Проверка блокировки
            content = page.content()
            text_lower = content.lower()
            blocked_words = ["captcha", "подтвердите", "робот", "заблокирован", "recaptcha"]
            if any(w in text_lower for w in blocked_words):
                print("БЛОКИРОВКА! Captcha или бан.")
                browser.close()
                return []

            # Ожидание карточек
            try:
                page.wait_for_selector("[data-index]", timeout=15000)
            except PlaywrightTimeout:
                print("Карточки не найдены.")
                browser.close()
                return []

            # Скролл для подгрузки
            print(f"Подгрузка карточек (цель: {limit})...")
            for _ in range(10):
                current_count = page.evaluate(
                    "document.querySelectorAll('[data-index]').length"
                )
                print(f"  ...{current_count} карточек")
                if current_count >= limit:
                    break
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(scroll_pause)

            # Извлечение ссылок
            urls = page.evaluate(EXTRACT_URLS_JS, limit)
            print(f"Собрано ссылок: {len(urls)}")

            browser.close()

        return urls

    except Exception as e:
        print(f"ОШИБКА при сборе ссылок: {e}")
        raise

    finally:
        baas.release()
        print("Браузер освобождён")


def scrape_one(index: int, url: str, proxy: str | None = None) -> dict:
    """Парсит отзывы одного товара. Обёртка для параллельного запуска."""
    print(f"[{index}] Старт: {url}")
    try:
        kwargs = dict(api_key=API_KEY, product_url=url, max_reviews=100, scroll_pause=2.0)
        if proxy:
            kwargs["proxy"] = proxy
        reviews = scrape_ozon_reviews(**kwargs)
        print(f"[{index}] Готово: {len(reviews)} отзывов")
        return {
            "product_url": url,
            "reviews_count": len(reviews),
            "reviews": [asdict(r) for r in reviews],
        }
    except Exception as e:
        print(f"[{index}] Ошибка: {e}")
        return {
            "product_url": url,
            "reviews_count": 0,
            "reviews": [],
            "error": str(e),
        }


def main():
    parser = argparse.ArgumentParser(description="Массовый сбор отзывов с Ozon")
    parser.add_argument("query", help="Поисковый запрос или URL выдачи Ozon")
    parser.add_argument("--limit", type=int, default=TARGET_PRODUCTS, help="Кол-во товаров (по умолчанию %(default)s)")
    parser.add_argument("--workers", type=int, default=WORKERS, help="Параллельных воркеров (по умолчанию %(default)s)")
    parser.add_argument("-o", "--output", default="ozon_bulk_reviews.json", help="Файл для результатов")
    args = parser.parse_args()

    query = args.query.replace("\\", "")

    if query.startswith("http://") or query.startswith("https://"):
        search_url = query
    else:
        search_url = f"https://www.ozon.ru/search/?text={quote_plus(query)}"

    print("=== Массовый сбор отзывов Ozon ===")
    print(f"Запрос: {search_url}")
    print(f"Прокси: {PROXY.split('@')[-1] if PROXY else 'нет'}")
    print(f"Товаров: {args.limit}, воркеров: {args.workers}")
    print()

    # 1. Собираем ссылки на товары
    urls = collect_product_urls(
        api_key=API_KEY,
        search_url=search_url,
        proxy=PROXY,
        limit=args.limit,
    )

    if not urls:
        print("Ссылки не найдены, выход.")
        return

    print(f"\n--- Парсинг отзывов для {len(urls)} товаров ({args.workers} воркеров) ---\n")

    # 2. Параллельный сбор отзывов
    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(scrape_one, i, url, PROXY): i
            for i, url in enumerate(urls, 1)
        }
        for future in as_completed(futures):
            results.append(future.result())

    # Сортируем по порядку URL (as_completed возвращает в порядке завершения)
    url_order = {url: i for i, url in enumerate(urls)}
    results.sort(key=lambda r: url_order.get(r["product_url"], 0))

    total_reviews = sum(r["reviews_count"] for r in results)
    errors = sum(1 for r in results if "error" in r)

    print(f"\n=== Итого ===")
    print(f"Товаров: {len(results)}")
    print(f"Отзывов: {total_reviews}")
    if errors:
        print(f"Ошибок: {errors}")

    # Сохранение
    output = {
        "search_url": search_url,
        "products_count": len(results),
        "total_reviews": total_reviews,
        "products": results,
    }

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"Сохранено в {args.output}")


if __name__ == "__main__":
    start_t=time.time()
    main()
    print(f"Занято времени:{time.time()-start_t}")

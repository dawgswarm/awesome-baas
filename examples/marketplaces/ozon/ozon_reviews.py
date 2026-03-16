"""
Парсинг отзывов с Ozon через dawg-baas.

Использование:
    venv/bin/python parsers/ozon_reviews.py "https://www.ozon.ru/product/..."
"""

import json
import sys
import time
from dataclasses import dataclass, asdict
from urllib.parse import urlparse
from dawg_baas import Baas
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout


# --- Конфигурация ---
API_KEY = "you_api_key"
PROXY = "http://user:pass@ip:port"


@dataclass
class Review:
    """Отзыв с Ozon."""
    author: str
    rating: int
    date: str
    text: str


def normalize_reviews_url(product_url: str) -> str:
    """
    Преобразует любой URL товара Ozon в чистый URL страницы отзывов.

    Убирает query-параметры (?at=..., ?utm_...), которые могут вызвать блокировку,
    и добавляет /reviews/.
    """
    parsed = urlparse(product_url)
    # Берём только scheme + host + path, без query и fragment
    path = parsed.path.strip("/\\")
    # Убираем /reviews если уже есть, чтобы не задвоить
    if not path.endswith("/reviews"):
        path += "/reviews"
    return f"{parsed.scheme}://{parsed.netloc}/{path}/"


# JavaScript для извлечения отзывов.
# Структура отзыва на Ozon:
#   - Имя автора: "Дмитрий К."
#   - Дата: "изменен 24 декабря 2024"
#   - Рейтинг: оранжевые звёзды (SVG)
#   - Текст комментария
EXTRACT_REVIEWS_JS = """() => {
    let containers = document.querySelectorAll('[data-review-uuid]');

    if (containers.length === 0) {
        const widget = document.querySelector('[data-widget="webReviewProductList"]');
        if (widget) {
            containers = widget.querySelectorAll(':scope > div > div');
        }
    }

    if (containers.length === 0) {
        containers = document.querySelectorAll('[itemtype*="Review"]');
    }

    const results = [];

    for (const el of containers) {
        const innerText = el.innerText || '';
        if (innerText.length < 10) continue;

        // --- Рейтинг ---
        // Контейнер звёзд содержит всегда 5 SVG.
        // Заполненные: style="color:var(--graphicRating)"
        // Пустые:      style="color:var(--layerActiveSurface)"
        // Считаем только SVG с --graphicRating.
        let rating = 0;
        const allSvgs = el.querySelectorAll('svg');
        for (const svg of allSvgs) {
            const style = svg.getAttribute('style') || '';
            if (style.includes('graphicRating')) {
                rating++;
            }
        }

        // --- Автор и дата через TreeWalker ---
        const textNodes = [];
        const walker = document.createTreeWalker(el, NodeFilter.SHOW_TEXT, null);
        while (walker.nextNode()) {
            const t = walker.currentNode.textContent.trim();
            if (t.length > 0) textNodes.push(t);
        }

        let author = '';
        let date = '';
        let textParts = [];

        const monthRe = /(января|февраля|марта|апреля|мая|июня|июля|августа|сентября|октября|ноября|декабря)/i;
        const skipRe = /^(Вам помог|Да \\d|Нет \\d|Показать|Ответить|Пожаловаться|\\d{2}:\\d{2}$)/i;

        for (const t of textNodes) {
            if (skipRe.test(t)) continue;

            if (!date && (monthRe.test(t) || /назад/i.test(t))) {
                date = t;
                continue;
            }

            if (!author && t.length >= 2 && t.length <= 50 && !/^\\d+$/.test(t)) {
                author = t;
                continue;
            }

            if (t.length > 1) {
                textParts.push(t);
            }
        }

        const reviewText = textParts.join('\\n').substring(0, 3000);
        if (!reviewText && rating === 0) continue;

        results.push({
            author: author || 'Аноним',
            rating: rating,
            date: date,
            text: reviewText
        });
    }

    return results;
}"""


def scrape_ozon_reviews(
    api_key: str,
    product_url: str,
    proxy: str | None = None,
    max_reviews: int = 100,
    scroll_pause: float = 2.0,
) -> list[Review]:
    """Парсит отзывы с карточки товара на Ozon."""
    reviews_url = normalize_reviews_url(product_url)

    baas = Baas(api_key=api_key)

    try:
        print("Создаём браузер...")
        ws_url = baas.create(proxy=proxy) if proxy else baas.create()
        print(f"Браузер создан: {baas.browser_id}")

        with sync_playwright() as p:
            browser = p.chromium.connect_over_cdp(ws_url)
            time.sleep(2)  # Ждём инициализации (из docs/Cookbook.md)

            page = browser.contexts[0].pages[0]

            # Навигация
            print(f"Загрузка: {reviews_url}")
            try:
                page.goto(reviews_url, wait_until="domcontentloaded", timeout=60000)
            except PlaywrightTimeout:
                print("Таймаут загрузки страницы")
                page.screenshot(path="ozon_timeout.png")
                print("Скриншот: ozon_timeout.png")
                browser.close()
                return []

            time.sleep(3)  # Даём JS отрендериться
            page.screenshot(path="ozon_loaded.png")
            print("Скриншот после загрузки: ozon_loaded.png")

            # Проверка блокировки
            content = page.content()
            text_lower = content.lower()
            blocked_words = ["captcha", "подтвердите", "робот", "заблокирован", "recaptcha"]
            if any(w in text_lower for w in blocked_words):
                print("БЛОКИРОВКА! Captcha или бан. Скриншот: ozon_loaded.png")
                browser.close()
                return []

            # Ожидание отзывов
            selectors = [
                "[data-review-uuid]",
                "[data-widget='webReviewProductList']",
                "[itemtype*='Review']",
            ]
            reviews_found = False
            for selector in selectors:
                try:
                    page.wait_for_selector(selector, timeout=10000)
                    print(f"Отзывы найдены: {selector}")
                    reviews_found = True
                    break
                except PlaywrightTimeout:
                    continue

            if not reviews_found:
                print("Отзывы не найдены на странице.")
                page.screenshot(path="ozon_no_reviews.png")
                print("Скриншот: ozon_no_reviews.png")
                browser.close()
                return []

            # Скролл для подгрузки отзывов
            previous_count = 0
            no_new_count = 0

            print("Загрузка отзывов через скролл...")

            while True:
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(scroll_pause)

                # Пробуем кнопку "Показать ещё"
                try:
                    btn = page.query_selector('button:has-text("Показать ещё")')
                    if btn and btn.is_visible():
                        btn.click()
                        time.sleep(scroll_pause)
                except Exception:
                    pass

                current_count = page.evaluate("""() => {
                    const r = document.querySelectorAll('[data-review-uuid]').length;
                    if (r > 0) return r;
                    const w = document.querySelector('[data-widget="webReviewProductList"]');
                    return w ? w.querySelectorAll(':scope > div > div').length : 0;
                }""")

                if current_count == previous_count:
                    no_new_count += 1
                    if no_new_count >= 3:
                        print(f"Конец ленты ({current_count} элементов)")
                        break
                else:
                    no_new_count = 0
                    print(f"  ...{current_count} отзывов")

                previous_count = current_count
                if current_count >= max_reviews:
                    print(f"Лимит: {max_reviews}")
                    break

            page.screenshot(path="ozon_before_extract.png")
            print("Скриншот перед извлечением: ozon_before_extract.png")

            # Извлечение данных
            raw_reviews = page.evaluate(EXTRACT_REVIEWS_JS)
            print(f"Извлечено из DOM: {len(raw_reviews)} записей")

            browser.close()

        # Конвертация
        reviews = []
        for r in raw_reviews[:max_reviews]:
            if not r.get("text") and not r.get("rating"):
                continue
            reviews.append(Review(
                author=r.get("author", "Аноним"),
                rating=r.get("rating", 0),
                date=r.get("date", ""),
                text=r.get("text", "").strip(),
            ))

        return reviews

    except Exception as e:
        print(f"ОШИБКА: {e}")
        try:
            page.screenshot(path="ozon_error.png")
            print("Скриншот ошибки: ozon_error.png")
        except Exception:
            pass
        raise

    finally:
        baas.release()
        print("Браузер освобождён")


def main():
    if len(sys.argv) < 2:
        print("Использование:")
        print('  venv/bin/python parsers/ozon_reviews.py "https://www.ozon.ru/product/..."')
        return

    product_url = sys.argv[1]

    print("=== Парсинг отзывов Ozon ===")
    print(f"URL: {product_url}")
    print(f"Прокси: {PROXY.split('@')[-1]}")
    print()

    reviews = scrape_ozon_reviews(
        api_key=API_KEY,
        product_url=product_url,
        proxy=PROXY,
        max_reviews=100,
        scroll_pause=2.0,
    )

    print(f"\nСобрано: {len(reviews)} отзывов\n")

    for i, review in enumerate(reviews[:5], 1):
        stars = "★" * review.rating + "☆" * (5 - review.rating)
        print(f"{i}. [{stars}] {review.author} ({review.date})")
        if review.text:
            print(f"   {review.text[:120]}")
        print()

    # Сохранение в JSON
    output = {
        "product_url": product_url,
        "reviews_count": len(reviews),
        "reviews": [asdict(r) for r in reviews],
    }

    output_file = "ozon_reviews.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"Сохранено в {output_file}")


if __name__ == "__main__":
    main()

"""
Мониторинг цен на Ozon — сбор карточек товаров из поисковой выдачи.

На вход: URL страницы поиска Ozon (например https://www.ozon.ru/search/?text=iphone+15)
На выход: первые N карточек с названием, ценой, старой ценой, рейтингом и кол-вом отзывов.

Использование:
    venv/bin/python parsers/ozon/ozon_prices.py "https://www.ozon.ru/search/?text=iphone+15"
"""

import json
import sys
import time
from dataclasses import dataclass, asdict
from urllib.parse import quote_plus
from dawg_baas import Baas
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout


# --- Конфигурация ---
API_KEY = "your_api_key"
TARGET_CARDS = 100


@dataclass
class ProductCard:
    """Карточка товара из поисковой выдачи Ozon."""
    title: str
    price: str
    old_price: str
    rating: float
    reviews_count: int


# Собирает карточки, которые СЕЙЧАС есть в DOM.
# Ozon виртуализирует список — в DOM одновременно ~36-54 карточки,
# старые удаляются при скролле. Поэтому собираем при каждом скролле.
EXTRACT_VISIBLE_CARDS_JS = """() => {
    const cards = document.querySelectorAll('[data-index]');
    const results = [];

    for (const card of cards) {
        const text = card.innerText || '';
        if (text.length < 10) continue;

        const titleEl = card.querySelector('a.tile-clickable-element .tsBody500Medium');
        const title = titleEl?.innerText?.trim() || '';
        if (!title) continue;

        const textNodes = [];
        const walker = document.createTreeWalker(card, NodeFilter.SHOW_TEXT, null);
        while (walker.nextNode()) {
            const t = walker.currentNode.textContent.trim();
            if (t.length > 0) textNodes.push(t);
        }

        let price = '';
        let oldPrice = '';
        for (const t of textNodes) {
            if (t.includes('₽')) {
                if (!price) price = t;
                else if (!oldPrice) oldPrice = t;
            }
        }

        let rating = 0;
        let reviewsCount = 0;
        for (const t of textNodes) {
            if (!rating) {
                const ratingMatch = t.match(/^(\\d\\.\\d)$/);
                if (ratingMatch) { rating = parseFloat(ratingMatch[1]); continue; }
            }
            const reviewMatch = t.match(/([\\d\\s]+)\\s*отзыв/i);
            if (reviewMatch) { reviewsCount = parseInt(reviewMatch[1].replace(/\\s/g, ''), 10) || 0; continue; }
            if (rating && !reviewsCount) {
                const numMatch = t.match(/^([\\d\\s]+)$/);
                if (numMatch) {
                    const num = parseInt(numMatch[1].replace(/\\s/g, ''), 10);
                    if (num > 0 && num < 1000000) reviewsCount = num;
                }
            }
        }

        results.push({
            title: title,
            price: price,
            old_price: oldPrice,
            rating: rating,
            reviews_count: reviewsCount
        });
    }

    return results;
}"""


def _fmt_elapsed(seconds: float) -> str:
    """Форматирует секунды в читаемый вид."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    m, s = divmod(seconds, 60)
    return f"{int(m)}m {s:.1f}s"


def scrape_ozon_prices(
    api_key: str,
    search_url: str,
    proxy: str,
    target_cards: int = 30,
) -> list[ProductCard]:
    """Собирает карточки товаров из поисковой выдачи Ozon."""

    t_start = time.time()
    baas = Baas(api_key=api_key)

    try:
        print("Создаём браузер с прокси...")
        ws_url = baas.create(proxy=proxy)
        t_browser = time.time()
        print(f"Браузер создан: {baas.browser_id} ... {_fmt_elapsed(t_browser - t_start)}")

        with sync_playwright() as p:
            browser = p.chromium.connect_over_cdp(ws_url)
            time.sleep(2)

            page = browser.contexts[0].pages[0]

            print(f"Загрузка: {search_url}")
            try:
                page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
            except PlaywrightTimeout:
                print("Таймаут загрузки страницы")
                page.screenshot(path="ozon_prices_timeout.png")
                print("Скриншот: ozon_prices_timeout.png")
                browser.close()
                return []

            time.sleep(3)
            page.screenshot(path="ozon_prices_loaded.png")
            t_loaded = time.time()
            print(f"Страница загружена ... {_fmt_elapsed(t_loaded - t_start)}")

            # Проверка блокировки
            content = page.content()
            text_lower = content.lower()
            blocked_words = ["captcha", "подтвердите", "робот", "заблокирован", "recaptcha"]
            if any(w in text_lower for w in blocked_words):
                print("БЛОКИРОВКА! Captcha или бан.")
                browser.close()
                return []

            # Ожидание карточек товаров
            try:
                page.wait_for_selector("[data-index]", timeout=15000)
                print("Карточки товаров найдены")
            except PlaywrightTimeout:
                print("Карточки не найдены.")
                page.screenshot(path="ozon_prices_no_cards.png")
                print("Скриншот: ozon_prices_no_cards.png")
                browser.close()
                return []

            # --- Агрессивный инкрементальный сбор ---
            # Ozon виртуализирует DOM — одновременно видно ~36-54 карточек,
            # при скролле старые удаляются. Собираем на каждом шаге, дедуплицируем.
            print(f"Скроллинг и сбор карточек (цель: {target_cards})...")

            all_cards = {}  # title -> card dict
            max_scrolls = 100
            stale_rounds = 0
            max_stale = 6
            prev_collected = 0

            # Агрессивный скролл: быстро крутим страницу серией scrollBy,
            # потом короткая пауза, собираем карточки, повторяем.
            for i in range(max_scrolls):
                # Серия из 3 быстрых скроллов подряд без пауз
                page.evaluate("""() => {
                    const step = Math.floor(window.innerHeight * 0.6);
                    window.scrollBy(0, step);
                }""")
                time.sleep(0.15)
                page.evaluate("""() => {
                    const step = Math.floor(window.innerHeight * 0.6);
                    window.scrollBy(0, step);
                }""")
                time.sleep(0.15)
                page.evaluate("""() => {
                    const step = Math.floor(window.innerHeight * 0.6);
                    window.scrollBy(0, step);
                }""")
                # Пауза для подгрузки контента
                time.sleep(0.7)

                # Собираем карточки из текущего DOM
                visible = page.evaluate(EXTRACT_VISIBLE_CARDS_JS)
                for card in visible:
                    title = card.get("title", "")
                    if title and title not in all_cards:
                        all_cards[title] = card

                collected = len(all_cards)

                if collected > prev_collected:
                    elapsed = _fmt_elapsed(time.time() - t_start)
                    print(f"  ...{collected} карточек [{elapsed}]")
                    stale_rounds = 0
                    prev_collected = collected
                else:
                    stale_rounds += 1

                if collected >= target_cards:
                    print(f"  Цель достигнута: {collected} >= {target_cards}")
                    break

                if stale_rounds >= max_stale:
                    print(f"  Новые карточки не появляются ({stale_rounds} попыток), стоп.")
                    break

                # При застое — дёрнуть скролл туда-сюда
                if stale_rounds >= 2:
                    page.evaluate("window.scrollBy(0, -800)")
                    time.sleep(0.3)
                    page.evaluate("window.scrollBy(0, 1200)")
                    time.sleep(0.8)

            page.screenshot(path="ozon_prices_before_extract.png")

            raw_cards = list(all_cards.values())[:target_cards]
            t_done = time.time()
            print(f"Итого собрано: {len(raw_cards)} карточек ... {_fmt_elapsed(t_done - t_start)}")

            browser.close()

        # Конвертация
        cards = []
        for r in raw_cards:
            cards.append(ProductCard(
                title=r.get("title", ""),
                price=r.get("price", ""),
                old_price=r.get("old_price", ""),
                rating=r.get("rating", 0),
                reviews_count=r.get("reviews_count", 0),
            ))

        return cards

    except Exception as e:
        print(f"ОШИБКА: {e}")
        try:
            page.screenshot(path="ozon_prices_error.png")
            print("Скриншот ошибки: ozon_prices_error.png")
        except Exception:
            pass
        raise

    finally:
        baas.release()
        print(f"Браузер освобождён ... {_fmt_elapsed(time.time() - t_start)}")


def main():
    if len(sys.argv) < 2:
        print("Использование:")
        print('  venv/bin/python parsers/ozon/ozon_prices.py "iphone 15"')
        print('  venv/bin/python parsers/ozon/ozon_prices.py "https://www.ozon.ru/category/..."')
        return

    query = sys.argv[1].replace("\\", "")

    # Если передан URL — используем как есть, иначе строим поисковый URL
    if query.startswith("http://") or query.startswith("https://"):
        search_url = query
    else:
        search_url = f"https://www.ozon.ru/search/?text={quote_plus(query)}"

    print("=== Мониторинг цен Ozon ===")
    print(f"URL: {search_url}")
    print(f"Прокси: {PROXY.split('@')[-1]}")
    print()

    t0 = time.time()

    cards = scrape_ozon_prices(
        api_key=API_KEY,
        search_url=search_url,
        proxy=PROXY,
        target_cards=TARGET_CARDS,
    )

    print(f"\nСобрано: {len(cards)} карточек\n")

    for i, card in enumerate(cards, 1):
        old = f" (было {card.old_price})" if card.old_price else ""
        rating_str = f"★ {card.rating}" if card.rating else "нет оценки"
        print(f"{i:2d}. {card.title[:60]}")
        print(f"    Цена: {card.price}{old}")
        print(f"    {rating_str} | {card.reviews_count} отзывов")
        print()

    # Сохранение в JSON
    output = {
        "search_url": search_url,
        "cards_count": len(cards),
        "cards": [asdict(c) for c in cards],
    }

    output_file = "ozon_prices.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"Сохранено в {output_file}")
    print(f"Общее время: {_fmt_elapsed(time.time() - t0)}")


if __name__ == "__main__":
    main()

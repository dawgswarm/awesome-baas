"""
Мульти-гео мониторинг цен на Ozon — сравнение цен товара по городам.

На вход: поисковый запрос (или URL) + список городов
На выход: 30 карточек на город + таблица мин/макс/авг по городам

Использование:
    venv/bin/python parsers/ozon/ozon_prices_geo.py "iphone 15"
    venv/bin/python parsers/ozon/ozon_prices_geo.py "https://www.ozon.ru/search/?text=iphone+15"
"""

import json
import re
import sys
import time
from dataclasses import dataclass, asdict
from urllib.parse import quote_plus
from dawg_baas import Baas
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout


# --- Конфигурация ---
API_KEY = "your_api_key"
TARGET_CARDS = 100

DEFAULT_CITIES = [
    ("Москва", "moskva"),
    ("Санкт-Петербург", "sankt-peterburg"),
    ("Новосибирск", "novosibirsk"),
    ("Казань", "kazan"),
    ("Владивосток", "vladivostok"),
]

# JavaScript для извлечения карточек товаров (из ozon_prices.py).
EXTRACT_CARDS_JS = """(targetCount) => {
    const cards = document.querySelectorAll('[data-index]');
    const results = [];

    for (const card of cards) {
        if (results.length >= targetCount) break;

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
                if (!price) {
                    price = t;
                } else if (!oldPrice) {
                    oldPrice = t;
                }
            }
        }

        let rating = 0;
        let reviewsCount = 0;

        for (const t of textNodes) {
            if (!rating) {
                const ratingMatch = t.match(/^(\\d\\.\\d)$/);
                if (ratingMatch) {
                    rating = parseFloat(ratingMatch[1]);
                    continue;
                }
            }

            const reviewMatch = t.match(/([\\d\\s]+)\\s*отзыв/i);
            if (reviewMatch) {
                reviewsCount = parseInt(reviewMatch[1].replace(/\\s/g, ''), 10) || 0;
                continue;
            }

            if (rating && !reviewsCount) {
                const numMatch = t.match(/^([\\d\\s]+)$/);
                if (numMatch) {
                    const num = parseInt(numMatch[1].replace(/\\s/g, ''), 10);
                    if (num > 0 && num < 1000000) {
                        reviewsCount = num;
                    }
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


@dataclass
class ProductCard:
    """Карточка товара из поисковой выдачи Ozon."""
    title: str
    price: str
    old_price: str
    rating: float
    reviews_count: int


@dataclass
class GeoCityResult:
    """Результат сбора по одному городу."""
    city: str
    cards: list[ProductCard]
    available: bool


def parse_price(price_str: str) -> int | None:
    """'56 099 ₽' -> 56099, '1 234,50 ₽' -> 1234. Возвращает None если не парсится."""
    digits = re.sub(r"[^\d]", "", price_str.split(",")[0].split(".")[0])
    return int(digits) if digits else None


def _handle_geo_popup(page, city_name: str, timeout: int = 5000):
    """Нажимает 'Сменить' в попапе геолокации, затем закрывает карту."""
    try:
        btn = page.wait_for_selector(
            'button:has-text("Сменить")', timeout=timeout
        )
        if btn and btn.is_visible():
            btn.click()
            print(f"  Попап «Сменить город» — нажали «Сменить» ({city_name})")
            time.sleep(2)

            # После «Сменить» появляется окно с картой — закрываем крестиком
            try:
                close_btn = page.wait_for_selector(
                    '[aria-label="Закрыть"], [aria-label="close"], button:has(svg) >> nth=-1',
                    timeout=3000,
                )
                if close_btn and close_btn.is_visible():
                    close_btn.click()
                    print(f"  Карта закрыта ({city_name})")
                    time.sleep(1)
            except PlaywrightTimeout:
                page.keyboard.press("Escape")
                print(f"  Карта: нажали Escape ({city_name})")
                time.sleep(1)
            return
    except PlaywrightTimeout:
        pass
    print(f"  Попап смены города не появился ({city_name})")


def scrape_cards_for_city(
    api_key: str,
    search_url: str,
    city_name: str,
    city_slug: str,
    target_cards: int = TARGET_CARDS,
    scroll_pause: float = 2.0,
) -> GeoCityResult:
    """Собирает до target_cards карточек из выдачи Ozon для конкретного города."""

    baas = Baas(api_key=api_key)
    empty = GeoCityResult(city=city_name, cards=[], available=False)

    try:
        print(f"\n[{city_name}] Создаём браузер (geo={city_slug})...")
        ws_url = baas.create(geo=city_slug)
        print(f"[{city_name}] Браузер: {baas.browser_id}")

        with sync_playwright() as p:
            browser = p.chromium.connect_over_cdp(ws_url)
            time.sleep(2)

            page = browser.contexts[0].pages[0]

            print(f"[{city_name}] Загрузка: {search_url}")
            try:
                page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
            except PlaywrightTimeout:
                print(f"[{city_name}] Таймаут загрузки")
                page.screenshot(path=f"ozon_geo_{city_slug}_timeout.png")
                browser.close()
                return empty

            time.sleep(3)

            # Обработка попапа "Сменить город на г. ..."
            _handle_geo_popup(page, city_name)

            # Проверка блокировки
            content = page.content()
            text_lower = content.lower()
            blocked_words = ["captcha", "подтвердите", "робот", "заблокирован", "recaptcha"]
            if any(w in text_lower for w in blocked_words):
                print(f"[{city_name}] БЛОКИРОВКА!")
                browser.close()
                return empty

            # Ожидание карточек
            try:
                page.wait_for_selector("[data-index]", timeout=15000)
                print(f"[{city_name}] Карточки найдены")
            except PlaywrightTimeout:
                print(f"[{city_name}] Карточки не найдены — нет в наличии")
                page.screenshot(path=f"ozon_geo_{city_slug}_no_cards.png")
                browser.close()
                return empty

            # Скролл для подгрузки карточек (lazy load)
            print(f"[{city_name}] Подгрузка карточек (цель: {target_cards})...")
            for _ in range(10):
                current_count = page.evaluate(
                    "document.querySelectorAll('[data-index]').length"
                )
                print(f"  [{city_name}] ...{current_count} карточек")
                if current_count >= target_cards:
                    break
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(scroll_pause)

            page.screenshot(path=f"ozon_geo_{city_slug}.png")

            # Извлечение данных
            raw_cards = page.evaluate(EXTRACT_CARDS_JS, target_cards)
            print(f"[{city_name}] Извлечено: {len(raw_cards)} карточек")
            browser.close()

        cards = [
            ProductCard(
                title=r.get("title", ""),
                price=r.get("price", ""),
                old_price=r.get("old_price", ""),
                rating=r.get("rating", 0),
                reviews_count=r.get("reviews_count", 0),
            )
            for r in raw_cards
        ]

        return GeoCityResult(city=city_name, cards=cards, available=len(cards) > 0)

    except Exception as e:
        print(f"[{city_name}] ОШИБКА: {e}")
        return empty

    finally:
        baas.release()
        print(f"[{city_name}] Браузер освобождён")


def city_price_stats(cards: list[ProductCard]) -> dict:
    """Считает мин/макс/авг цену из списка карточек."""
    prices = []
    for c in cards:
        p = parse_price(c.price)
        if p and p > 0:
            prices.append(p)
    if not prices:
        return {"min": 0, "max": 0, "avg": 0, "count": 0}
    return {
        "min": min(prices),
        "max": max(prices),
        "avg": round(sum(prices) / len(prices)),
        "count": len(prices),
    }


def fmt_price(value: int) -> str:
    """56099 -> '56 099 ₽'"""
    if not value:
        return "—"
    s = f"{value:,}".replace(",", " ")
    return f"{s} ₽"


def print_table(results: list[GeoCityResult]):
    """Выводит таблицу сравнения мин/макс/авг цен по городам."""
    w = 78
    print("\n" + "=" * w)
    print("СРАВНЕНИЕ ЦЕН ПО ГОРОДАМ")
    print("=" * w)

    print(
        f"{'Город':<20} {'Карточек':>8} {'Мин':>12} {'Макс':>12} {'Средняя':>12} {'Разброс':>10}"
    )
    print("-" * w)

    for r in results:
        if not r.available:
            print(f"{r.city:<20} {'нет в наличии':>8}")
            continue

        stats = city_price_stats(r.cards)
        spread = stats["max"] - stats["min"] if stats["count"] > 1 else 0
        print(
            f"{r.city:<20} {stats['count']:>8} "
            f"{fmt_price(stats['min']):>12} "
            f"{fmt_price(stats['max']):>12} "
            f"{fmt_price(stats['avg']):>12} "
            f"{fmt_price(spread):>10}"
        )

    print("=" * w)

    # Общая статистика по всем городам
    all_stats = []
    for r in results:
        if r.available:
            s = city_price_stats(r.cards)
            if s["count"]:
                all_stats.append((r.city, s))

    if len(all_stats) > 1:
        avgs = [(city, s["avg"]) for city, s in all_stats]
        cheapest = min(avgs, key=lambda x: x[1])
        priciest = max(avgs, key=lambda x: x[1])
        print(f"\nДешевле всего: {cheapest[0]} (средняя {fmt_price(cheapest[1])})")
        print(f"Дороже всего:  {priciest[0]} (средняя {fmt_price(priciest[1])})")
        diff = priciest[1] - cheapest[1]
        print(f"Разница:       {fmt_price(diff)}")


def main():
    if len(sys.argv) < 2:
        print("Использование:")
        print('  venv/bin/python parsers/ozon/ozon_prices_geo.py "iphone 15"')
        print('  venv/bin/python parsers/ozon/ozon_prices_geo.py "https://www.ozon.ru/search/?text=..."')
        return

    query = sys.argv[1].replace("\\", "")

    if query.startswith("http://") or query.startswith("https://"):
        search_url = query
    else:
        search_url = f"https://www.ozon.ru/search/?text={quote_plus(query)}"

    cities = DEFAULT_CITIES

    print("=== Мульти-гео мониторинг цен Ozon ===")
    print(f"Запрос: {query}")
    print(f"URL: {search_url}")
    print(f"Города: {', '.join(name for name, _ in cities)}")
    print(f"Карточек на город: {TARGET_CARDS}")

    results: list[GeoCityResult] = []
    for city_name, city_slug in cities:
        result = scrape_cards_for_city(
            api_key=API_KEY,
            search_url=search_url,
            city_name=city_name,
            city_slug=city_slug,
        )
        results.append(result)

    print_table(results)

    # Сохранение в JSON
    output = {
        "search_url": search_url,
        "cities_count": len(results),
        "results": [],
    }
    for r in results:
        stats = city_price_stats(r.cards)
        output["results"].append({
            "city": r.city,
            "available": r.available,
            "cards_count": len(r.cards),
            "price_min": stats["min"],
            "price_max": stats["max"],
            "price_avg": stats["avg"],
            "cards": [asdict(c) for c in r.cards],
        })

    output_file = "ozon_prices_geo.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\nСохранено в {output_file}")


if __name__ == "__main__":
    start_t=time.time()
    main()
    print(f"Занято времени:{time.time()-start_t}")

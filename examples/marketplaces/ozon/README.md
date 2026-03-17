# Парсеры Ozon

Набор скриптов для сбора данных с Ozon через облачные браузеры [dawg-baas](../../dawgdocs/) + Playwright.

## Зависимости

- `dawg_baas` — клиент для создания облачных браузеров
- `playwright` — автоматизация браузера
- Python 3.10+

## Скрипты

### ozon_reviews.py

Парсинг отзывов с карточки товара. Открывает страницу отзывов, скроллит для подгрузки, извлекает автора, рейтинг, дату и текст.

```bash
venv/bin/python parsers/ozon/ozon_reviews.py "https://www.ozon.ru/product/..."
```

**Выход:** `ozon_reviews.json` — массив отзывов (автор, рейтинг 1-5, дата, текст).

### ozon_bulk_reviews.py

Массовый сбор отзывов. Берёт поисковую выдачу, собирает ссылки на товары, затем параллельно парсит отзывы для каждого.

```bash
venv/bin/python parsers/ozon/ozon_bulk_reviews.py "iphone 15"
venv/bin/python parsers/ozon/ozon_bulk_reviews.py "iphone 15" --limit 10 --workers 3
venv/bin/python parsers/ozon/ozon_bulk_reviews.py "https://www.ozon.ru/search/?text=iphone+15"
```

| Флаг | По умолчанию | Описание |
|------|-------------|----------|
| `--limit` | 30 | Кол-во товаров для парсинга |
| `--workers` | 5 | Параллельных воркеров |
| `-o` / `--output` | `ozon_bulk_reviews.json` | Файл результатов |

**Выход:** JSON с массивом товаров, у каждого — список отзывов.

### ozon_prices.py

Мониторинг цен из поисковой выдачи. Собирает карточки товаров с названием, ценой, старой ценой, рейтингом и кол-вом отзывов. Использует инкрементальный сбор с дедупликацией (Ozon виртуализирует DOM при скролле).

```bash
venv/bin/python parsers/ozon/ozon_prices.py "iphone 15"
venv/bin/python parsers/ozon/ozon_prices.py "https://www.ozon.ru/search/?text=iphone+15"
```

**Выход:** `ozon_prices.json` — массив карточек (title, price, old_price, rating, reviews_count).

### ozon_prices_geo.py

Мульти-гео мониторинг цен — сравнение цен одного запроса по разным городам. По умолчанию: Москва, Санкт-Петербург, Новосибирск, Казань, Владивосток.

```bash
venv/bin/python parsers/ozon/ozon_prices_geo.py "iphone 15"
```

**Выход:** `ozon_prices_geo.json` + таблица в консоли с мин/макс/средней ценой и разбросом по городам.

## Конфигурация

В каждом скрипте в начале файла задаются:

- `API_KEY` — ключ для dawg-baas
- `PROXY` — прокси (опционально, формат `http://user:pass@host:port`)
- `TARGET_CARDS` / `TARGET_PRODUCTS` — кол-во карточек/товаров для сбора

## Антиблокировка

Все скрипты проверяют наличие капчи/блокировки после загрузки страницы и корректно завершаются при обнаружении. При ошибках сохраняются скриншоты для диагностики.

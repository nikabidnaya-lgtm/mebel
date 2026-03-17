# Furniture Scraper (profoffice.ru + riva.ru)

Playwright-ориентированный асинхронный модуль для подбора бюджетных аналогов мебели на двух сайтах:

- `profoffice.ru` (обход каталога без `/search/`)
- `riva.ru` (поиск + ссылки из sitemap + JS/lazy-load)

## Возможности

- Единый интерфейс `FurnitureScraper`:
  - `search_analogs(query_dict, max_results=5)`
  - `scrape_site(site, query, budget=None)`
- Асинхронный сбор данных (`asyncio`, `playwright`, `httpx`).
- Парсинг карточки:
  - title, price, url, short_description, image_url, material, size, availability.
- Доп. извлечение текста из PDF (PyMuPDF) для `profoffice`.
- Фильтрация по бюджету и ранжирование по цене и текстовому сходству.
- Выгрузка результата в Excel.
- CLI для пакетной обработки входного Excel.

## Установка

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install playwright beautifulsoup4 lxml httpx pandas pymupdf aiofiles tenacity loguru python-dotenv pytest pytest-asyncio
playwright install chromium
```

## .env

```env
HEADLESS=true
PROXY=
```

## Формат входа

Строки Excel должны содержать (минимум):
- `name`
- `description`
- `size`
- `budget_max`
- `keywords` (строка через запятую)

## CLI

```bash
python scraper.py --excel input.xlsx --output output.xlsx --max-results 5
```

По умолчанию скрипт использует **явно заданные локальные пути**:

- вход: `data/input.xlsx`
- выход: `data/output.xlsx`

Можно запускать вообще без аргументов:

```bash
python scraper.py
```

## Использование из Python

```python
import asyncio
from scraper import FurnitureScraper

query = {
    "name": "Офисное кресло Тип 1",
    "description": "Каркас полипропилен, PU пена, ткань, газлифт",
    "size": "685 / 685 / 940-1040",
    "budget_max": 25000,
    "keywords": ["офисное кресло", "газлифт", "ткань", "подлокотники"],
}

async def run():
    scraper = FurnitureScraper()
    try:
        result = await scraper.to_dataframe(query, max_results=5)
        print(result.head())
    finally:
        await scraper.close()

asyncio.run(run())
```

## Тесты

```bash
pytest -q
```

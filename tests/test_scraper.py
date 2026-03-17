import pytest
from bs4 import BeautifulSoup

from scraper import FurnitureScraper


@pytest.mark.asyncio
async def test_similarity_and_sorting_budget_filter():
    scraper = FurnitureScraper()
    scraper.scrape_site = _mock_scrape_site  # type: ignore[method-assign]

    query = {
        "name": "Офисное кресло Тип 1",
        "description": "газлифт ткань подлокотники",
        "size": "685 / 685 / 940-1040",
        "budget_max": 25000,
        "keywords": ["газлифт", "ткань"],
    }

    results = await scraper.search_analogs(query, max_results=5)
    assert len(results) == 2
    assert results[0]["price"] <= results[1]["price"]
    assert all(r["price"] <= 25000 for r in results)


@pytest.mark.asyncio
async def test_extract_price_material_and_size():
    scraper = FurnitureScraper()
    text = "Цена 24 990 ₽, каркас полипропилен, обивка ткань, размер 685/685/940"
    assert scraper._extract_price(text) == 24990
    material = scraper._extract_material(text)
    assert material is not None and "ткан" in material
    size = scraper._extract_size(text)
    assert size is not None and "685" in size


@pytest.mark.asyncio
async def test_extract_description_from_html():
    scraper = FurnitureScraper()
    html = """
    <html><body>
      <h1>Модель X</h1>
      <div class='product-description'>Эргономичное кресло, ткань кат.2, газлифт, подлокотники.</div>
    </body></html>
    """
    soup = BeautifulSoup(html, "lxml")
    desc = scraper._extract_description(soup)
    assert "Эргономичное кресло" in desc


@pytest.mark.asyncio
async def test_row_aliases_and_budget_parsing():
    scraper = FurnitureScraper()
    row = {
        "Наименование": "Кресло Альфа",
        "Описание": "ткань, газлифт",
        "Размер": "600/600/900",
        "Бюджет": "до 25 000 ₽",
        "Ключевые слова": "кресло, ткань",
    }
    # эмуляция поведения pandas.Series
    import pandas as pd

    series = pd.Series(row)
    assert scraper._pick_row_value(series, ["name", "наименование"]) == "Кресло Альфа"
    assert scraper._parse_budget(series) == 25000


async def _mock_scrape_site(site: str, query: str, budget: int | None = None):
    base = [
        {
            "title": f"{site} cheap chair",
            "price": 19990,
            "url": f"https://{site}.ru/item1",
            "short_description": "офисное кресло с газлифтом и тканевой обивкой",
            "image_url": "https://img/1.jpg",
            "site": site,
        },
        {
            "title": f"{site} expensive chair",
            "price": 35990,
            "url": f"https://{site}.ru/item2",
            "short_description": "кресло премиум",
            "image_url": "https://img/2.jpg",
            "site": site,
        },
    ]
    if budget:
        return [x for x in base if x["price"] <= budget]
    return base

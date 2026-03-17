import argparse
import asyncio
import os
import random
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urljoin

import fitz
import httpx
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from loguru import logger
from playwright.async_api import Browser, BrowserContext, Page, async_playwright
from tenacity import retry, stop_after_attempt, wait_exponential


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]


@dataclass
class ScraperConfig:
    headless: bool = True
    proxy: str | None = None
    timeout_ms: int = 45000
    profoffice_delay_s: float = 1.0
    riva_delay_s: float = 3.0


class FurnitureScraper:
    """Асинхронный скрейпер аналогов мебели с единым API для profoffice и riva."""

    def __init__(self, config: ScraperConfig | None = None) -> None:
        load_dotenv()
        self.config = config or ScraperConfig(
            headless=os.getenv("HEADLESS", "true").lower() == "true",
            proxy=os.getenv("PROXY"),
        )
        self._http = httpx.AsyncClient(timeout=30)

    async def close(self) -> None:
        await self._http.aclose()

    async def search_analogs(self, query_dict: dict[str, Any], max_results: int = 5) -> list[dict[str, Any]]:
        """Ищет аналоги на двух сайтах, фильтрует по бюджету и сортирует по цене+релевантности."""
        query_text = self._build_query(query_dict)
        budget = query_dict.get("budget_max")
        keywords = query_dict.get("keywords", [])

        profoffice_task = self.scrape_site("profoffice", query_text, budget)
        riva_task = self.scrape_site("riva", query_text, budget)
        results = [*await profoffice_task, *await riva_task]

        for item in results:
            item["similarity"] = self._similarity(query_text, item.get("short_description", ""), keywords)
            item["original_name"] = query_dict.get("name", "")
            item["savings"] = max((budget or 0) - item.get("price", 0), 0) if budget else None

        filtered = [r for r in results if (not budget or (r.get("price") and r["price"] <= budget))]
        filtered.sort(key=lambda x: (x.get("price") or 10**9, -x.get("similarity", 0)))
        return filtered[:max_results]

    async def scrape_site(
        self,
        site: Literal["profoffice", "riva"],
        query: str,
        budget: int | None = None,
    ) -> list[dict[str, Any]]:
        if site == "profoffice":
            return await self._scrape_profoffice(query, budget)
        return await self._scrape_riva(query, budget)

    async def to_dataframe(self, query_dict: dict[str, Any], max_results: int = 5) -> pd.DataFrame:
        data = await self.search_analogs(query_dict, max_results=max_results)
        columns = [
            "original_name",
            "analog_title",
            "analog_price",
            "analog_url",
            "analog_description",
            "image_url",
            "savings",
            "site",
        ]
        rows = [
            {
                "original_name": d.get("original_name"),
                "analog_title": d.get("title"),
                "analog_price": d.get("price"),
                "analog_url": d.get("url"),
                "analog_description": d.get("short_description"),
                "image_url": d.get("image_url"),
                "savings": d.get("savings"),
                "site": d.get("site"),
            }
            for d in data
        ]
        return pd.DataFrame(rows, columns=columns)

    async def process_excel(self, excel_path: str, output_path: str, max_results_per_row: int = 5) -> pd.DataFrame:
        source = pd.read_excel(excel_path)
        all_rows: list[pd.DataFrame] = []
        for _, row in source.iterrows():
            query = {
                "name": row.get("name") or row.get("original_name") or "",
                "description": row.get("description", ""),
                "size": row.get("size", ""),
                "budget_max": int(row["budget_max"]) if not pd.isna(row.get("budget_max")) else None,
                "keywords": self._parse_keywords(row.get("keywords", "")),
            }
            df = await self.to_dataframe(query, max_results=max_results_per_row)
            all_rows.append(df)

        result = pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()
        export_to_excel(result, output_path)
        return result

    @retry(wait=wait_exponential(multiplier=1, min=1, max=8), stop=stop_after_attempt(3))
    async def _safe_get(self, url: str, headers: dict[str, str] | None = None) -> httpx.Response:
        response = await self._http.get(url, headers=headers)
        response.raise_for_status()
        return response

    async def _scrape_profoffice(self, query: str, budget: int | None) -> list[dict[str, Any]]:
        base_url = "https://profoffice.ru"
        start_paths = ["/catalog/chairs/", "/catalog/chairs/staff/", "/catalog/chairs/leaders/"]
        visited: set[str] = set()
        to_visit = [urljoin(base_url, p) for p in start_paths]
        cards: list[dict[str, Any]] = []

        while to_visit:
            page_url = to_visit.pop(0)
            if page_url in visited:
                continue
            visited.add(page_url)
            await asyncio.sleep(self.config.profoffice_delay_s)

            try:
                resp = await self._safe_get(page_url, headers={"User-Agent": random.choice(USER_AGENTS)})
            except Exception as exc:
                logger.warning("profoffice skip {}: {}", page_url, exc)
                continue

            soup = BeautifulSoup(resp.text, "lxml")
            for link in soup.select("a[href*='/catalog/chairs/']"):
                full = urljoin(base_url, link.get("href", ""))
                depth = full.count("/", len(base_url) + 1) - "catalog/chairs".count("/")
                if full.startswith(f"{base_url}/catalog/chairs/") and full not in visited and depth <= 3:
                    to_visit.append(full)

            for a in soup.select("a[href*='/catalog/']"):
                item_url = urljoin(base_url, a.get("href", ""))
                if item_url in visited:
                    continue
                if re.search(r"/catalog/.+/.+", item_url):
                    item = await self._parse_generic_product(base_url, item_url, "profoffice", query)
                    if item and (not budget or not item.get("price") or item["price"] <= budget):
                        cards.append(item)

        return self._deduplicate(cards)

    async def _scrape_riva(self, query: str, budget: int | None) -> list[dict[str, Any]]:
        base_url = "https://riva.ru"
        catalog_links = await self._riva_catalog_links(base_url)
        search_url = f"{base_url}/search/?q={query.replace(' ', '+')}"
        results: list[dict[str, Any]] = []

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.config.headless)
            context = await self._new_context(browser)
            page = await context.new_page()

            for url in [search_url, *catalog_links[:30]]:
                await asyncio.sleep(self.config.riva_delay_s)
                try:
                    await page.goto(url, wait_until="networkidle", timeout=self.config.timeout_ms)
                    await page.mouse.wheel(0, 3000)
                    html = await page.content()
                    soup = BeautifulSoup(html, "lxml")
                    links = [urljoin(base_url, a.get("href", "")) for a in soup.select("a[href*='/catalog/']")]
                    for link in links:
                        item = await self._parse_riva_card(page, link, query)
                        if item and (not budget or not item.get("price") or item["price"] <= budget):
                            results.append(item)
                except Exception as exc:
                    await self._save_error_screenshot(page, "riva")
                    logger.warning("riva page error {}: {}", url, exc)
            await context.close()
            await browser.close()

        return self._deduplicate(results)

    async def _riva_catalog_links(self, base_url: str) -> list[str]:
        sitemap_url = f"{base_url}/index.php/sitemap.xml"
        try:
            xml_resp = await self._safe_get(sitemap_url, headers={"User-Agent": random.choice(USER_AGENTS)})
            root = ET.fromstring(xml_resp.text)
            ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
            links = [
                loc.text
                for loc in root.findall(".//sm:loc", ns)
                if loc.text and "/catalog/" in loc.text
            ]
            return links
        except Exception as exc:
            logger.warning("sitemap parse error: {}", exc)
            return [f"{base_url}/catalog/"]

    async def _parse_riva_card(self, page: Page, url: str, query: str) -> dict[str, Any] | None:
        if "/catalog/" not in url:
            return None
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=self.config.timeout_ms)
            await page.mouse.wheel(0, 2000)
            html = await page.content()
        except Exception:
            return None

        soup = BeautifulSoup(html, "lxml")
        title = self._text_or_none(soup.select_one("h1"))
        if not title:
            return None
        description = self._extract_description(soup)
        return {
            "title": title,
            "price": self._extract_price(soup.get_text(" ", strip=True)),
            "url": url,
            "short_description": description,
            "image_url": self._extract_image(soup, "https://riva.ru"),
            "material": self._extract_material(description),
            "size": self._extract_size(description),
            "availability": self._extract_availability(soup.get_text(" ", strip=True)),
            "site": "riva",
        }

    async def _parse_generic_product(
        self, base_url: str, item_url: str, site: str, query: str
    ) -> dict[str, Any] | None:
        try:
            resp = await self._safe_get(item_url, headers={"User-Agent": random.choice(USER_AGENTS)})
        except Exception:
            return None

        soup = BeautifulSoup(resp.text, "lxml")
        title = self._text_or_none(soup.select_one("h1"))
        if not title:
            return None

        description = self._extract_description(soup)
        pdf_text = await self._maybe_extract_pdf_text(soup, base_url)
        full_desc = f"{description} {pdf_text}".strip()

        return {
            "title": title,
            "price": self._extract_price(resp.text),
            "url": item_url,
            "short_description": full_desc[:200],
            "image_url": self._extract_image(soup, base_url),
            "material": self._extract_material(full_desc),
            "size": self._extract_size(full_desc),
            "availability": self._extract_availability(resp.text),
            "site": site,
        }

    async def _maybe_extract_pdf_text(self, soup: BeautifulSoup, base_url: str) -> str:
        pdf_link = soup.select_one("a[href$='.pdf']")
        if not pdf_link:
            return ""
        pdf_url = urljoin(base_url, pdf_link.get("href", ""))
        try:
            pdf_resp = await self._safe_get(pdf_url)
            with fitz.open(stream=BytesIO(pdf_resp.content), filetype="pdf") as doc:
                text = " ".join(page.get_text() for page in doc)
            return text[:3000]
        except Exception as exc:
            logger.warning("pdf parse error {}: {}", pdf_url, exc)
            return ""

    async def _new_context(self, browser: Browser) -> BrowserContext:
        kwargs = {
            "user_agent": random.choice(USER_AGENTS),
            "viewport": {"width": 1440, "height": 900},
        }
        if self.config.proxy:
            kwargs["proxy"] = {"server": self.config.proxy}
        return await browser.new_context(**kwargs)

    async def _save_error_screenshot(self, page: Page, prefix: str) -> None:
        path = Path("artifacts")
        path.mkdir(parents=True, exist_ok=True)
        file = path / f"{prefix}_error_{int(asyncio.get_running_loop().time()*1000)}.png"
        try:
            await page.screenshot(path=str(file), full_page=True)
        except Exception:
            pass

    def _build_query(self, payload: dict[str, Any]) -> str:
        return " ".join(
            str(payload.get(k, "")) for k in ["name", "description", "size", "keywords"] if payload.get(k)
        )

    def _similarity(self, query: str, text: str, keywords: list[str]) -> float:
        query_set = set(re.findall(r"\w+", query.lower()))
        text_set = set(re.findall(r"\w+", text.lower()))
        overlap = (len(query_set & text_set) / len(query_set)) if query_set else 0
        kw_score = sum(1 for k in keywords if k.lower() in text.lower()) / max(len(keywords), 1)
        return round((overlap * 0.7 + kw_score * 0.3) * 100, 2)

    def _extract_price(self, text: str) -> int | None:
        m = re.search(r"(\d[\d\s]{2,})\s*[₽рР]", text)
        if not m:
            return None
        return int(re.sub(r"\D", "", m.group(1)))

    def _extract_description(self, soup: BeautifulSoup) -> str:
        candidates = [
            self._text_or_none(soup.select_one(".product-description")),
            self._text_or_none(soup.select_one(".description")),
            self._text_or_none(soup.select_one(".tabs-content")),
            soup.get_text(" ", strip=True),
        ]
        text = next((c for c in candidates if c), "")
        return re.sub(r"\s+", " ", text)[:200]

    def _extract_image(self, soup: BeautifulSoup, base: str) -> str | None:
        img = soup.select_one("img")
        if not img:
            return None
        src = img.get("src") or img.get("data-src")
        return urljoin(base, src) if src else None

    def _extract_size(self, text: str) -> str | None:
        m = re.search(r"(\d{2,4}\s*[x/×]\s*\d{2,4}(?:\s*[x/×-]\s*\d{2,4})?)", text)
        return m.group(1) if m else None

    def _extract_material(self, text: str) -> str | None:
        found = [m for m in ["ткан", "металл", "дерево", "полипропилен", "эко-кожа", "пу пена"] if m in text.lower()]
        return ", ".join(found) if found else None

    def _extract_availability(self, text: str) -> str:
        t = text.lower()
        if "в наличии" in t:
            return "в наличии"
        if "под заказ" in t:
            return "под заказ"
        return "не указано"

    def _text_or_none(self, node: Any) -> str | None:
        return node.get_text(" ", strip=True) if node else None

    def _parse_keywords(self, raw: Any) -> list[str]:
        if isinstance(raw, list):
            return [str(x).strip() for x in raw if str(x).strip()]
        if not raw or pd.isna(raw):
            return []
        return [x.strip() for x in str(raw).split(",") if x.strip()]

    def _deduplicate(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        seen: set[str] = set()
        out = []
        for item in items:
            key = item.get("url") or item.get("title")
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(item)
        return out


def export_to_excel(df: pd.DataFrame, filename: str) -> None:
    Path(filename).parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(filename, index=False)


async def _run_cli(args: argparse.Namespace) -> None:
    scraper = FurnitureScraper()
    try:
        await scraper.process_excel(args.excel, args.output, args.max_results)
    finally:
        await scraper.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Скрейпер аналогов мебели")
    parser.add_argument("--excel", required=True, help="Путь к входному Excel")
    parser.add_argument("--output", required=True, help="Путь к выходному Excel")
    parser.add_argument("--max-results", type=int, default=5, help="Максимум аналогов на одну позицию")
    args = parser.parse_args()
    asyncio.run(_run_cli(args))


if __name__ == "__main__":
    main()

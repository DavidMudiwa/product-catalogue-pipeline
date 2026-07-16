"""
Scrapes product listings from Takealot's search API and writes them to CSV.

Usage:
    python src/prdct_scarp.py --department tv-audio-video --category tvs-25953 \
        --output takealot_all_tvs.csv
"""
# are the selected fields enough for analytics and reporting? or do we need to add more fields from the API response?
# how do i exract the image url for each product

from __future__ import annotations

import argparse
import csv
import logging
import time
from dataclasses import dataclass, fields
from typing import Any, Iterator

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = (
    "https://api.takealot.com/rest/v-1-17-0/searches/"
    "products,filters,facets,sort_options,breadcrumbs,slots_audience,context,seo,layout"
)
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("takealot_scraper")


@dataclass
class ProductRow:
    product_id: int | None
    title: str | None
    brand: str | None
    price_min: float | None
    price_max: float | None
    pretty_price: str | None
    rating: float | None
    reviews: int | None
    in_stock: bool | None
    product_url: str | None

    @classmethod
    def field_names(cls) -> list[str]:
        return [f.name for f in fields(cls)]


def build_session(retries: int = 5, backoff_factor: float = 1.0) -> requests.Session:
    """Session with connection pooling and automatic retry/backoff on transient errors."""
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(HEADERS)
    return session


def parse_product(item: dict[str, Any]) -> ProductRow | None:
    """Convert one raw API result item into a ProductRow. Returns None for non-product entries."""
    if item.get("type") != "product_views":
        return None  # skip ads/banners/other placement types

    pv = item.get("product_views", {})
    core = pv.get("core", {})
    buybox = pv.get("buybox_summary", {})
    review = pv.get("review_summary", {})
    stock = pv.get("stock_availability_summary", {})

    plid = core.get("id")
    slug = core.get("slug")
    prices = buybox.get("prices") or []

    return ProductRow(
        product_id=plid,
        title=core.get("title"),
        brand=core.get("brand"),
        price_min=min(prices) if prices else None,
        price_max=max(prices) if prices else None,
        pretty_price=buybox.get("pretty_price"),
        rating=review.get("star_rating"),
        reviews=review.get("review_count"),
        in_stock=stock.get("is_in_stock"),
        product_url=f"https://www.takealot.com/{slug}/PLID{plid}" if slug and plid else None,
    )


def fetch_pages(
    session: requests.Session,
    params: dict[str, Any],
    max_pages: int = 200,
    delay_seconds: float = 1.0,
) -> Iterator[list[dict[str, Any]]]:
    """Yields the raw 'results' list for each page, following the cursor until exhausted."""
    cursor = None
    seen_cursors: set[str] = set()

    for page in range(1, max_pages + 1):
        page_params = dict(params)
        if cursor:
            page_params["after"] = cursor

        try:
            response = session.get(BASE_URL, params=page_params, timeout=30)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as exc:
            log.error("Page %d request failed after retries: %s", page, exc)
            break
        except ValueError as exc:
            log.error("Page %d returned invalid JSON: %s", page, exc)
            break

        try:
            products_section = data["sections"]["products"]
            results = products_section["results"]
            paging = products_section["paging"]
        except KeyError as exc:
            log.error("Page %d had unexpected response shape (missing %s)", page, exc)
            break

        log.info("Page %d: %d raw results", page, len(results))
        yield results

        next_cursor = paging.get("next_is_after")
        if not next_cursor or next_cursor in seen_cursors:
            if next_cursor in seen_cursors:
                log.warning("Cursor repeated — stopping to avoid an infinite loop")
            break

        seen_cursors.add(next_cursor)
        cursor = next_cursor
        time.sleep(delay_seconds)  # be polite to the API
    else:
        log.warning("Hit max_pages=%d — there may be more results left uncollected", max_pages)


def scrape(
    department_slug: str,
    category_slug: str,
    customer_id: str,
    client_id: str,
    output_path: str,
    max_pages: int = 200,
    delay_seconds: float = 1.0,
) -> int:
    params = {
        "sort": "Relevance",
        "department_slug": department_slug,
        "category_slug": category_slug,
        "customer_id": customer_id,
        "client_id": client_id,
    }

    session = build_session()
    seen_ids: set[int] = set()
    total_written = 0
    expected_total: int | None = None

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ProductRow.field_names())
        writer.writeheader()

        for page_num, results in enumerate(
            fetch_pages(session, params, max_pages=max_pages, delay_seconds=delay_seconds), start=1
        ):
            for item in results:
                row = parse_product(item)
                if row is None:
                    continue
                if row.product_id is None or row.product_id in seen_ids:
                    continue  # skip malformed or duplicate entries
                seen_ids.add(row.product_id)
                writer.writerow(row.__dict__)
                total_written += 1
            f.flush()  # persist progress after each page in case of a later crash

    log.info("Collected %d unique products -> %s", total_written, output_path)
    if expected_total is not None and total_written < expected_total:
        log.warning("Expected %d results but only collected %d", expected_total, total_written)
    return total_written


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape Takealot product listings to CSV.")
    parser.add_argument("--department", default="tv-audio-video", help="department_slug")
    parser.add_argument("--category", default="tvs-25953", help="category_slug")
    parser.add_argument("--customer-id", default="-1442556678")
    parser.add_argument("--client-id", default="31fe46c8-82ae-4488-868a-8d56a5efe7bd")
    parser.add_argument("--output", default="takealot_all_tvs.csv")
    parser.add_argument("--max-pages", type=int, default=200)
    parser.add_argument("--delay", type=float, default=1.0, help="seconds between requests")
    args = parser.parse_args()

    scrape(
        department_slug=args.department,
        category_slug=args.category,
        customer_id=args.customer_id,
        client_id=args.client_id,
        output_path=args.output,
        max_pages=args.max_pages,
        delay_seconds=args.delay,
    )


if __name__ == "__main__":
    main()
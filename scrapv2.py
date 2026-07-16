"""
Scrapes product listings from Takealot's search API and writes them to CSV.

Usage:
    
python scrapv2.py --department cellular-gps --category smart-watches-27389 --max-pages 1 --delay 5.0
"""

from __future__ import annotations

import argparse
import csv
import itertools
import logging
import os
import re
import time
from dataclasses import dataclass, fields
from datetime import datetime, timezone
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
    # --- identity (-> dim_products) ---
    product_id: int | None          # core.id / PLID
    tsin: int | None                # Takealot SKU id — distinct from product_id
    offer_id: int | None            # buybox_summary.product_id — the winning offer, not the PLID
    title: str | None
    brand: str | None
    slug: str | None
    product_url: str | None
    department_slug: str | None
    category_slug: str | None
    department_name: str | None     # from breadcrumbs
    category_name: str | None       # from breadcrumbs
    
    # --- price (-> fct_price_snapshots) ---
    price_min: float | None
    price_max: float | None
    listing_price: float | None
    pretty_price: str | None
    discount_pct: float | None
    is_multi_offer: bool | None

    # --- stock (-> fct_stock_snapshots) ---
    in_stock: bool | None
    stock_status: str | None
    is_preorder: bool | None

    # --- reviews (-> fct_review_snapshots) ---
    rating: float | None
    reviews: int | None
    rating_1_star: int | None
    rating_2_star: int | None
    rating_3_star: int | None
    rating_4_star: int | None
    rating_5_star: int | None

    # --- crawl metadata (needed on every fact row; not present in the API at all) ---
    scraped_at: str | None

    @classmethod
    def field_names(cls) -> list[str]:
        return [f.name for f in fields(cls)]


def build_output_filename(category_name: str | None, category_slug: str, run_time: datetime) -> str:
    """e.g. category_name='TV's' -> 'tvs_20260715_143000.csv'"""
    raw = category_name or category_slug
    slug = re.sub(r"[^a-z0-9]+", "_", raw.lower().replace("'", "")).strip("_")
    timestamp = run_time.strftime("%Y%m%d_%H%M%S")
    return f"{slug}_{timestamp}.csv"


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


def parse_product(
    item: dict[str, Any],
    department_name: str | None,
    category_name: str | None,
    scraped_at: str,
) -> ProductRow | None:
    """Convert one raw API result item into a ProductRow. Returns None for non-product entries."""
    if item.get("type") != "product_views":
        return None  # skip ads/banners/other placement types

    pv = item.get("product_views", {})
    core = pv.get("core", {})
    buybox = pv.get("buybox_summary", {})
    review = pv.get("review_summary", {})
    stock = pv.get("stock_availability_summary", {})
    gallery = pv.get("gallery", {})
    dist = review.get("distribution", {})

    plid = core.get("id")
    slug = core.get("slug")
    prices = buybox.get("prices") or []
    price_min = min(prices) if prices else None
    listing_price = buybox.get("listing_price")

    discount_pct = None
    if listing_price and price_min and listing_price > 0:
        discount_pct = round((1 - price_min / listing_price) * 100, 1)

    

    return ProductRow(
        product_id=plid,
        tsin=buybox.get("tsin"),
        offer_id=buybox.get("product_id"),
        title=core.get("title"),
        brand=core.get("brand"),
        slug=slug,
        product_url=f"https://www.takealot.com/{slug}/PLID{plid}" if slug and plid else None,
        department_slug=None,  # filled in by caller from the run's search params
        category_slug=None,
        department_name=department_name,
        category_name=category_name,
        price_min=price_min,
        price_max=max(prices) if prices else None,
        listing_price=listing_price,
        pretty_price=buybox.get("pretty_price"),
        discount_pct=discount_pct,
        is_multi_offer=len(prices) > 1,
        in_stock=stock.get("is_in_stock"),
        stock_status=stock.get("status"),
        is_preorder=buybox.get("is_preorder"),
        rating=review.get("star_rating"),
        reviews=review.get("review_count"),
        rating_1_star=dist.get("num_1_star_ratings"),
        rating_2_star=dist.get("num_2_star_ratings"),
        rating_3_star=dist.get("num_3_star_ratings"),
        rating_4_star=dist.get("num_4_star_ratings"),
        rating_5_star=dist.get("num_5_star_ratings"),
        scraped_at=scraped_at,
    )


def extract_breadcrumb_names(data: dict[str, Any]) -> tuple[str | None, str | None]:
    """Pulls display names for department and category out of the breadcrumbs section."""
    department_name = category_name = None
    try:
        crumbs = data["sections"]["breadcrumbs"]["results"]
    except KeyError:
        return department_name, category_name

    for crumb in crumbs:
        b = crumb.get("breadcrumb", {})
        if b.get("filter_name") == "Type":
            department_name = b.get("display_value")
        elif b.get("filter_name") == "Category":
            category_name = b.get("display_value")
    return department_name, category_name


def fetch_pages(
    session: requests.Session,
    params: dict[str, Any],
    max_pages: int = 200,
    delay_seconds: float = 1.0,
) -> Iterator[tuple[list[dict[str, Any]], dict[str, Any]]]:
    """Yields (results, full_page_data) for each page, following the cursor until exhausted."""
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
        yield results, data

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
    output_dir: str = ".",
    output_path: str | None = None,
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

    run_time = datetime.now(timezone.utc)
    scraped_at = run_time.isoformat()

    session = build_session()
    page_iter = fetch_pages(session, params, max_pages=max_pages, delay_seconds=delay_seconds)

    # Peek at page 1 so we know the category's display name before opening the output file.
    try:
        first_page = next(page_iter)
    except StopIteration:
        log.error("No pages returned — nothing to write")
        return 0

    department_name, category_name = extract_breadcrumb_names(first_page[1])

    if output_path is None:
        filename = build_output_filename(category_name, category_slug, run_time)
        os.makedirs(output_dir, exist_ok=True)
        output_path = f"{output_dir.rstrip('/')}/{filename}"

    seen_ids: set[int] = set()
    total_written = 0
    expected_total: int | None = None

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ProductRow.field_names())
        writer.writeheader()

        for results, _ in itertools.chain([first_page], page_iter):
            for item in results:
                row = parse_product(item, department_name, category_name, scraped_at)
                if row is None:
                    continue
                if row.product_id is None or row.product_id in seen_ids:
                    continue  # skip malformed or duplicate entries
                seen_ids.add(row.product_id)
                row.department_slug = department_slug
                row.category_slug = category_slug
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
    parser.add_argument("--output", default=None, help="explicit output filename (overrides auto-naming)")
    parser.add_argument("--output-dir", default=".", help="directory for the auto-named output file")
    parser.add_argument("--max-pages", type=int, default=200)
    parser.add_argument("--delay", type=float, default=1.0, help="seconds between requests")
    args = parser.parse_args()

    scrape(
        department_slug=args.department,
        category_slug=args.category,
        customer_id=args.customer_id,
        client_id=args.client_id,
        output_dir=args.output_dir,
        output_path=args.output,
        max_pages=args.max_pages,
        delay_seconds=args.delay,
    )


if __name__ == "__main__":
    main()
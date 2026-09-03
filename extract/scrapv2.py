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
import random
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
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

# Rotate through a handful of realistic, current desktop browser UAs instead of
# a single static "Mozilla/5.0" — a bare UA string is an easy signal for a WAF
# to flag as scripted traffic.
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.5 Safari/605.1.15",
]


def build_headers() -> dict[str, str]:
    """Fresh header set per session — random UA instead of one static value."""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json",
        "Accept-Language": "en-ZA,en;q=0.9",
    }


def generate_customer_id() -> str:
    """
    Generates a plausible customer_id in the same shape as observed real values
    (a negative 32-bit-range integer, e.g. "-1442556678") so each session looks
    like a distinct anonymous visitor instead of reusing one fixed identity.
    """
    return str(random.randint(-2_147_483_648, -1))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s")
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
    session.headers.update(build_headers())
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
    customer_id: str | None = None,
    client_id: str | None = None,
    output_dir: str = ".",
    output_path: str | None = None,
    max_pages: int = 200,
    delay_seconds: float = 1.0,
    start_jitter_seconds: float = 0.0,
) -> int:
    # Generate fresh customer_id/client_id per call unless explicitly passed in.
    # Reusing one static identity across concurrent threads/categories makes
    # simultaneous requests look like one session bursting traffic, which is
    # a common trigger for rate-based WAF blocks.
    if customer_id is None:
        customer_id = generate_customer_id()
    if client_id is None:
        client_id = str(uuid.uuid4())

    if start_jitter_seconds > 0:
        time.sleep(random.uniform(0, start_jitter_seconds))

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


def scrape_many(
    categories: list[tuple[str, str]],
    customer_id: str | None = None,
    client_id: str | None = None,
    output_dir: str = ".",
    max_pages: int = 200,
    delay_seconds: float = 1.0,
    max_workers: int = 4,
    max_start_jitter_seconds: float = 3.0,
) -> dict[tuple[str, str], int]:
    """
    Runs scrape() for each (department_slug, category_slug) pair concurrently.

    Pages *within* a category can't be parallelized (the API's cursor-based
    paging means page N+1 depends on the cursor returned by page N), but the
    categories themselves are independent — different params, different
    cursor chains, different output files — so they're a good fit for a
    thread pool. This is an I/O-bound workload (waiting on network requests
    and the polite `delay_seconds` sleep), so threads work well here despite
    the GIL.

    Each category gets its own customer_id and client_id (freshly generated,
    unless explicit fixed values are passed in for all of them) and a small
    random delay before its first request, so concurrent threads don't all
    announce the same identity and fire in the same instant.

    Returns a dict mapping each (department_slug, category_slug) pair to the
    number of rows written (or -1 if that category's scrape raised an error).
    """
    results: dict[tuple[str, str], int] = {}

    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="scraper") as pool:
        future_to_cat = {
            pool.submit(
                scrape,
                department_slug=department_slug,
                category_slug=category_slug,
                customer_id=customer_id,  # None -> scrape() generates a unique one per category
                client_id=client_id,      # None -> scrape() generates a unique one per category
                output_dir=output_dir,
                output_path=None,
                max_pages=max_pages,
                delay_seconds=delay_seconds,
                start_jitter_seconds=max_start_jitter_seconds,
            ): (department_slug, category_slug)
            for department_slug, category_slug in categories
        }

        for future in as_completed(future_to_cat):
            cat = future_to_cat[future]
            try:
                results[cat] = future.result()
            except Exception:
                log.exception("Category %s/%s failed", *cat)
                results[cat] = -1

    succeeded = sum(1 for n in results.values() if n >= 0)
    total_rows = sum(n for n in results.values() if n >= 0)
    log.info(
        "Done: %d/%d categories succeeded, %d total rows written",
        succeeded, len(categories), total_rows,
    )
    for (dept, cat), count in results.items():
        status = "OK" if count >= 0 else "FAILED"
        log.info("  %-8s %s/%s (%d rows)", status, dept, cat, max(count, 0))

    return results


def _parse_categories_file(path: str) -> list[tuple[str, str]]:
    """Reads department_slug,category_slug pairs from a CSV/text file, one per line."""
    pairs: list[tuple[str, str]] = []
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.reader(f):
            row = [c.strip() for c in row if c.strip()]
            if not row or row[0].startswith("#"):
                continue
            if len(row) != 2:
                raise ValueError(f"Bad line in categories file (expected 'department,category'): {row}")
            pairs.append((row[0], row[1]))
    return pairs


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape Takealot product listings to CSV.")
    parser.add_argument(
        "--department", default=None, nargs="+",
        help="one or more department_slug values, paired positionally with --category",
    )
    parser.add_argument(
        "--category", default=None, nargs="+",
        help="one or more category_slug values, paired positionally with --department",
    )
    parser.add_argument(
        "--categories-file", default=None,
        help="CSV/text file with one 'department_slug,category_slug' pair per line; "
             "an alternative to passing several --department/--category values",
    )
    parser.add_argument(
        "--customer-id", default=None,
        help="fixed customer_id to use for every request. If omitted (recommended), "
             "a fresh random customer_id is generated per category so concurrent runs "
             "don't share one identity.",
    )
    parser.add_argument(
        "--client-id", default=None,
        help="fixed client_id to use for every request. If omitted (recommended), "
             "a fresh UUID is generated per category so concurrent runs don't share one identity.",
    )
    parser.add_argument(
        "--start-jitter", type=float, default=3.0,
        help="max random seconds to wait before each category's first request when running "
             "multiple categories concurrently (only used by scrape_many)",
    )
    parser.add_argument(
        "--output", default=None,
        help="explicit output filename (overrides auto-naming; only valid for a single category)",
    )
    parser.add_argument("--output-dir", default=".", help="directory for the auto-named output file(s)")
    parser.add_argument("--max-pages", type=int, default=200)
    parser.add_argument("--delay", type=float, default=1.0, help="seconds between requests within a category")
    parser.add_argument(
        "--workers", type=int, default=4,
        help="number of categories to scrape concurrently (only matters with multiple categories)",
    )
    args = parser.parse_args()

    # Build the list of (department_slug, category_slug) pairs to scrape.
    if args.categories_file:
        categories = _parse_categories_file(args.categories_file)
    elif args.department or args.category:
        department = args.department or []
        category = args.category or []
        if len(department) != len(category):
            parser.error("--department and --category must have the same number of values")
        categories = list(zip(department, category))
    else:
        categories = [("tv-audio-video", "tvs-25953")]  # default, matches old script behavior

    if len(categories) == 1:
        department_slug, category_slug = categories[0]
        scrape(
            department_slug=department_slug,
            category_slug=category_slug,
            customer_id=args.customer_id,
            client_id=args.client_id,
            output_dir=args.output_dir,
            output_path=args.output,
            max_pages=args.max_pages,
            delay_seconds=args.delay,
        )
    else:
        if args.output:
            parser.error("--output can't be used with multiple categories; use --output-dir instead")
        scrape_many(
            categories=categories,
            customer_id=args.customer_id,
            client_id=args.client_id,
            output_dir=args.output_dir,
            max_pages=args.max_pages,
            delay_seconds=args.delay,
            max_workers=args.workers,
            max_start_jitter_seconds=args.start_jitter,
        )


if __name__ == "__main__":
    main()
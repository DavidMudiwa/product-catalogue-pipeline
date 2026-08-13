"""
Runs the Takealot scraper across every category defined in categories.yml.

Adding a category later means editing categories.yml only -- nothing here
needs to change. One category failing (network error, bad category slug,
etc.) doesn't stop the rest from running; failures are collected and
reported in the summary at the end.

Usage:
    uv run extract/run_scraper.py   --config extract/categories.yml   --output-dir scraped_data   --max-pages 1   --delay 2.0   --delay-between-categories 5.0
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from dataclasses import dataclass
import yaml
# NOTE: match this import to whatever your actual scraper file is named
# locally (this project has used both scrapv2.py and scrap_v2.py at
# different points -- pick whichever one is actually on disk).
import scrapv2 as scraper

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("run_scrapers")


@dataclass
class CategoryResult:
    name: str
    department_slug: str
    category_slug: str
    rows: int = 0
    success: bool = False
    error: str | None = None


def load_categories(config_path: str) -> list[dict]:
    with open(config_path) as f:
        config = yaml.safe_load(f)
    categories = config.get("categories") or []
    if not categories:
        raise ValueError(f"No categories found in {config_path}")
    return categories


def run_all(
    config_path: str,
    output_dir: str,
    customer_id: str,
    client_id: str,
    max_pages: int,
    delay_seconds: float,
    delay_between_categories: float,
) -> list[CategoryResult]:
    categories = load_categories(config_path)
    log.info("Loaded %d categories from %s", len(categories), config_path)

    results: list[CategoryResult] = []

    for i, cat in enumerate(categories, start=1):
        name = cat.get("name", cat["category_slug"])
        result = CategoryResult(
            name=name,
            department_slug=cat["department_slug"],
            category_slug=cat["category_slug"],
        )

        log.info("[%d/%d] Scraping %s (%s / %s)", i, len(categories), name,
                  cat["department_slug"], cat["category_slug"])

        try:
            rows = scraper.scrape(
                department_slug=cat["department_slug"],
                category_slug=cat["category_slug"],
                customer_id=customer_id,
                client_id=client_id,
                output_dir=output_dir,
                max_pages=max_pages,
                delay_seconds=delay_seconds,
            )
            result.rows = rows
            result.success = True
            log.info("[%d/%d] Done: %s -> %d rows", i, len(categories), name, rows)
        except Exception as exc:
            # One bad category (typo'd slug, transient network failure that
            # exhausted retries, etc.) must not take down the other 12.
            result.error = str(exc)
            log.error("[%d/%d] FAILED: %s -> %s", i, len(categories), name, exc)

        results.append(result)

        if i < len(categories):
            time.sleep(delay_between_categories)

    return results


def print_summary(results: list[CategoryResult]) -> int:
    """Returns a process exit code: 0 if everything succeeded, 1 if anything failed."""
    succeeded = [r for r in results if r.success]
    failed = [r for r in results if not r.success]

    print()
    print("=" * 60)
    print(f"Summary: {len(succeeded)}/{len(results)} categories succeeded")
    print("=" * 60)
    for r in results:
        status = f"OK ({r.rows} rows)" if r.success else f"FAILED: {r.error}"
        print(f"  {r.name:30s} {status}")

    if failed:
        print()
        print(f"{len(failed)} categor{'y' if len(failed)==1 else 'ies'} failed -- rerun is safe, "
              "already-loaded categories won't be duplicated downstream.")
        return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the scraper across all configured categories.")
    parser.add_argument("--config", default="categories.yml")
    parser.add_argument("--output-dir", default="scraped_data")
    parser.add_argument("--customer-id", default="-1442556678")
    parser.add_argument("--client-id", default="31fe46c8-82ae-4488-868a-8d56a5efe7bd")
    parser.add_argument("--max-pages", type=int, default=200)
    parser.add_argument("--delay", type=float, default=1.0, help="seconds between pages within a category")
    parser.add_argument("--delay-between-categories", type=float, default=5.0,
                         help="seconds between categories -- gentler on the API than back-to-back page delay alone")
    args = parser.parse_args()

    results = run_all(
        config_path=args.config,
        output_dir=args.output_dir,
        customer_id=args.customer_id,
        client_id=args.client_id,
        max_pages=args.max_pages,
        delay_seconds=args.delay,
        delay_between_categories=args.delay_between_categories,
    )
    return print_summary(results)


if __name__ == "__main__":
    sys.exit(main())
import requests
import pandas as pd
import time


# docker compose run --rm --entrypoint bash scrapper

#TV_URL = "https://api.takealot.com/rest/v-1-16-0/searches/products,filters,facets,sort_options,breadcrumbs,slots_audience,context,seo,layout"
URL = " https://api.takealot.com/rest/v-1-17-0/searches/products,filters,facets,sort_options,breadcrumbs,slots_audience,context,seo,layout,related_searches,suggested_filters?sort=Relevance&department_slug=home-kitchen&category_slug=homeware-26000&customer_id=-1495800015&client_id=968079c1-043b-4fec-a936-66f3f36877a3"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

BASE_PARAMS = {
    "sort": "Relevance",
    "department_slug": "tv-audio-video",
    "category_slug": "tvs-25953",
    "customer_id": "-1442556678",
    "client_id": "31fe46c8-82ae-4488-868a-8d56a5efe7bd"
}

rows = []
cursor = None
page = 1

while True:

    params = BASE_PARAMS.copy()

    if cursor:
        params["after"] = cursor

    response = requests.get(
        URL,
        params=params,
        headers=HEADERS,
        timeout=60
    )

    response.raise_for_status()

    data = response.json()

    products = data["sections"]["products"]["results"]

    print(f"Page {page}: {len(products)} products")

    for item in products:

        pv = item["product_views"]

        core = pv.get("core", {})
        buybox = pv.get("buybox_summary", {})
        review = pv.get("review_summary", {})

        plid = core.get("id")

        rows.append({
            "product_id": plid,
            "title": core.get("title"),
            "brand": core.get("brand"),
            "price": buybox.get("pretty_price"),
            "rating": review.get("star_rating"),
            "reviews": review.get("review_count"),
            "product_url": f"https://www.takealot.com/{core.get('slug')}/PLID{plid}"
            if core.get("slug") else None
        })

    paging = data["sections"]["products"]["paging"]

    cursor = paging.get("next_is_after")

    if not cursor:
        break

    page += 1

    # Be polite to the API
    time.sleep(1)

df = (
    pd.DataFrame(rows)
      .drop_duplicates(subset="product_id")
)

df.to_csv("takealot_all_tvs.csv", index=False)

print()
print(f"Collected {len(df)} TVs")
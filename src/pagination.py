import requests
import json

url = "https://api.takealot.com/rest/v-1-16-0/searches/products,filters,facets,sort_options,breadcrumbs,slots_audience,context,seo,layout"

params = {
    # Removed the filter so we get the full TV category
    "sort": "Relevance",
    "department_slug": "tv-audio-video",
    "category_slug": "tvs-25953",
    "customer_id": "-1442556678",
    "client_id": "31fe46c8-82ae-4488-868a-8d56a5efe7bd"
}

headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

response = requests.get(url, params=params, headers=headers, timeout=60)
response.raise_for_status()

data = response.json()

# Save the full response for inspection
with open("full_response2.json", "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2, ensure_ascii=False)

print("=" * 80)
print("TOP LEVEL KEYS")
print("=" * 80)
print(list(data.keys()))

print("\n")

print("=" * 80)
print("SEARCH REQUEST")
print("=" * 80)
print(json.dumps(data.get("search_request", {}), indent=2))

print("\n")

sections = data.get("sections", {})

print("=" * 80)
print("SECTION NAMES")
print("=" * 80)
print(list(sections.keys()))

print("\n")

products = sections.get("products", {})

print("=" * 80)
print("PRODUCTS KEYS")
print("=" * 80)
print(list(products.keys()))

print("\n")

print("=" * 80)
print("PRODUCTS OBJECT")
print("=" * 80)

for key, value in products.items():

    print(f"\n{key}")

    if isinstance(value, list):
        print(f"LIST ({len(value)} items)")

    elif isinstance(value, dict):
        print("DICT")
        print("Keys:", list(value.keys()))

    else:
        print(type(value), value)

print("\n")

results = products.get("results", [])

print("=" * 80)
print("RESULTS")
print("=" * 80)
print("Products returned:", len(results))

if results:

    print("\nFirst result keys:")
    print(list(results[0].keys()))

    if "product_views" in results[0]:

        print("\nproduct_views keys:")
        print(list(results[0]["product_views"].keys()))

print("\n")

# Look for anything that resembles pagination
print("=" * 80)
print("SEARCHING FOR PAGINATION FIELDS")
print("=" * 80)

pagination_keywords = [
    "page",
    "pages",
    "page_size",
    "pageSize",
    "limit",
    "offset",
    "cursor",
    "next",
    "previous",
    "total",
    "total_pages",
    "total_results",
    "count",
    "has_next"
]

def find_keys(obj, path="root"):

    if isinstance(obj, dict):

        for k, v in obj.items():

            for keyword in pagination_keywords:

                if keyword.lower() in k.lower():

                    print(f"{path}.{k} = {v}")

            find_keys(v, f"{path}.{k}")

    elif isinstance(obj, list):

        if obj and isinstance(obj[0], dict):
            find_keys(obj[0], path + "[0]")


find_keys(data)

print("\n")

print("=" * 80)
print("Response saved as full_response.json")
print("=" * 80)
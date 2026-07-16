import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

URL = os.getenv("TAKEALOT_URL")

PARAMS = {
    "filter": os.getenv("FILTER"),
    "sort": os.getenv("SORT"),
    "department_slug": os.getenv("DEPARTMENT"),
    "category_slug": os.getenv("CATEGORY"),
    "customer_id": os.getenv("CUSTOMER_ID"),
    "client_id": os.getenv("CLIENT_ID"),
}

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}


def explore_api():
    response = requests.get(URL, params=PARAMS, headers=HEADERS, timeout=60)
    response.raise_for_status()

    data = response.json()

    print("\n=== TOP LEVEL KEYS ===")
    print(list(data.keys()))

    sections = data.get("sections", {})
    print("\n=== SECTIONS ===")
    print(list(sections.keys()))

    products_section = sections.get("products", {})
    print("\n=== PRODUCTS SECTION KEYS ===")
    print(list(products_section.keys()))

    results = products_section.get("results", [])
    print("\n=== NUMBER OF PRODUCTS ===")
    print(len(results))

    if results:
        sample = results[0]

        print("\n=== SAMPLE PRODUCT KEYS ===")
        print(list(sample.keys()))

        pv = sample.get("product_views", {})
        print("\n=== product_views KEYS ===")
        print(list(pv.keys()))

        # Drill deeper
        for key, value in pv.items():
            if isinstance(value, dict):
                print(f"\n--- {key} KEYS ---")
                print(list(value.keys()))

    # Save full JSON for manual inspection
    with open("full_response3.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print("\nFull JSON saved as full_response.json")


if __name__ == "__main__":
    explore_api()
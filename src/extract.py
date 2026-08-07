import os
import logging
import requests
import pandas
import json 
from datetime import datetime, UTC
from dotenv import load_dotenv

load_dotenv()  # loads .env file

URL = os.getenv("TAKEALOT_URL")

PARAMS = {
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def extract_takealot_data():
    logger.info("Extracting data from source API...") 
    response = requests.get(URL, params=PARAMS, headers=HEADERS,timeout=60)
    response.raise_for_status()  # Raise an error for bad status codes
    logger.info("Data extraction successful. Status code: %s", response.status_code)
    return response.json()

def transform_products(data):
    products = data["sections"]["products"]["results"]
    date_scanned = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    rows = []
    logger.info("Transforming product data...", len(products))

    for product in products:
        pv = product.get("product_views", {})

        badges = pv.get("badges", {})
        core = pv.get("core", {})
        gallery = pv.get("gallery", {})
        review = pv.get("review_summary", {})
        buybox = pv.get("buybox_summary", {})
        stock = pv.get("stock_availability_summary", {})
        badges = pv.get("badges", {})
        ecommerce = pv.get("enhanced_ecommerce_impression", {}).get("ecommerce", {})
        
        estimated_delivery = stock.get("estimated_delivery", {})
        distribution_centres = stock.get("distribution_centres", [])
        badge_entries = badges.get("entries", [])
        images = gallery.get("images", [])
        prices = buybox.get("prices", [])

        first_badge = badge_entries[0] if badge_entries else {}
        first_eec_product = ecommerce_products[0] if ecommerce_products else {}
        review_distribution = review.get("distribution", {})
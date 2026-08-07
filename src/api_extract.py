from playwright.sync_api import sync_playwright

URL = "https://www.takealot.com/home-kitchen/homeware-26000?sort=Relevance"
seen = set()

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    page = browser.new_page()

    def handle_response(response):
        try:
            url = response.url
            ctype = response.headers.get("content-type", "").lower()

            if "application/json" in ctype and "takealot.com" in url and url not in seen:
                seen.add(url)
                print("\nURL:", url)
                print("Status:", response.status)
        except Exception:
            pass

    page.on("response", handle_response)

    page.goto(URL, wait_until="domcontentloaded", timeout=120000)
    page.wait_for_timeout(15000)

    for _ in range(12):
        page.mouse.wheel(0, 3500)
        page.wait_for_timeout(2000)

    browser.close()
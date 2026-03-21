import json
import time
import pathlib

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

SEARCH_URL = "https://www.fragrantica.com/search/?spol=unisex~male"
LINKS_FILE = pathlib.Path("links.json")
BUTTON_XPATH = '/html/body/div[1]/main/div/div[1]/div[1]/div/div/main/div[2]/div/div[2]/button'


def save(links: set):
    LINKS_FILE.write_text(json.dumps(sorted(links), indent=2))


def collect(page) -> set:
    anchors = page.query_selector_all('a[href*="/perfume/"]')
    hrefs = set()
    for a in anchors:
        href = a.get_attribute('href')
        if href and '/perfume/' in href and not href.startswith('/perfumes/'):
            hrefs.add(href)
    return hrefs


def main():
    links: set = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()
        Stealth().apply_stealth_sync(page)

        page.goto(SEARCH_URL, wait_until='domcontentloaded')
        page.wait_for_timeout(3000)

        round_num = 0
        try:
            while True:
                # Extract and save
                found = collect(page)
                new = found - links
                if new:
                    links |= new
                    save(links)
                    print(f"Round {round_num}: +{len(new)} new → {len(links)} total")

                # Scroll to bottom
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(1)

                # Click load more
                btn = page.locator(f'xpath={BUTTON_XPATH}')
                if btn.count() == 0 or not btn.is_visible():
                    print("Load more button gone — done.")
                    break
                btn.click()
                page.wait_for_timeout(2000)
                round_num += 1

        except KeyboardInterrupt:
            print(f"\nInterrupted. Saved {len(links)} links.")
        finally:
            browser.close()

    print(f"Done. {len(links)} links saved to {LINKS_FILE}")


if __name__ == '__main__':
    main()

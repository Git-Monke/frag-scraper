import json
import re
import sqlite3
import string
import pathlib

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

BASE = "https://www.fragrantica.com"
SEARCH_URL = "https://www.fragrantica.com/search/"
LINKS_FILE = pathlib.Path("links.json")
DB_FILE = pathlib.Path("fragrances.db")


def normalize(href: str) -> str | None:
    if not href:
        return None
    url = href if href.startswith("http") else BASE + href
    return url if re.search(r'-\d+\.html$', url) else None


def collect(page) -> set[str]:
    anchors = page.query_selector_all('a[href*="/perfume/"]')
    hrefs = set()
    for a in anchors:
        href = a.get_attribute('href')
        if href and '/perfume/' in href and not href.startswith('/perfumes/'):
            hrefs.add(href)
    return hrefs


def save(links: set[str]):
    LINKS_FILE.write_text(json.dumps(sorted(links), indent=2))


def load_seen() -> set[str]:
    seen: set[str] = set()

    if LINKS_FILE.exists():
        seen.update(json.loads(LINKS_FILE.read_text()))
        print(f"Loaded {len(seen)} links from {LINKS_FILE}")

    if DB_FILE.exists():
        before = len(seen)
        conn = sqlite3.connect(DB_FILE)
        for (url,) in conn.execute("SELECT url FROM fragrances"):
            seen.add(url)
        conn.close()
        print(f"Loaded {len(seen) - before} additional URLs from DB ({len(seen)} total seen)")

    return seen


def main():
    seen = load_seen()
    all_links: set[str] = set(json.loads(LINKS_FILE.read_text())) if LINKS_FILE.exists() else set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()
        Stealth().apply_stealth_sync(page)

        page.goto(SEARCH_URL, wait_until='domcontentloaded')
        page.wait_for_timeout(2000)

        try:
            for letter in string.ascii_lowercase[string.ascii_lowercase.index('n'):string.ascii_lowercase.index('q')+1]:
                print(f"\n--- Letter: {letter} ---")
                page.goto(SEARCH_URL, wait_until='domcontentloaded')
                page.wait_for_timeout(1500)

                search_input = page.locator('input[placeholder*="typing"]')
                search_input.wait_for(state='visible', timeout=10000)
                search_input.click()
                search_input.type(letter)
                page.wait_for_timeout(2000)

                letter_links: set[str] = set()
                round_num = 0

                while True:
                    letter_links |= collect(page)

                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    page.wait_for_timeout(300)

                    btn = page.locator('xpath=/html/body/div[1]/main/div/div[1]/div[1]/div/div/main/div[2]/div/div[2]/button')
                    if btn.count() == 0 or not btn.is_visible() or not btn.is_enabled():
                        print(f"  Round {round_num}: {len(letter_links)} total for '{letter}' — done")
                        break

                    prev_size = len(letter_links)
                    print(f"  Round {round_num}: {len(letter_links)} so far, clicking more...")
                    btn.scroll_into_view_if_needed()
                    btn.click()
                    page.wait_for_timeout(1500)

                    letter_links |= collect(page)
                    if len(letter_links) == prev_size:
                        print(f"  Round {round_num}: no new links after click, stopping '{letter}'")
                        break

                    round_num += 1

                new = {normalize(h) for h in letter_links}
                new.discard(None)
                new -= seen

                if new:
                    seen |= new
                    all_links |= new
                    save(all_links)
                    print(f"  '{letter}': +{len(new)} new links → {len(all_links)} total in links.json")
                else:
                    print(f"  '{letter}': no new links")

        except KeyboardInterrupt:
            print(f"\nInterrupted. Saving {len(all_links)} links.")
            save(all_links)
        finally:
            browser.close()

    print(f"\nDone. {len(all_links)} links in {LINKS_FILE}")


if __name__ == '__main__':
    main()

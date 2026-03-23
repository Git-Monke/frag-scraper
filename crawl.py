import json, time, random, pathlib, re
from fragrantica import FragranticaScraper, FragranticaDB

BASE = "https://www.fragrantica.com"
LINKS_FILE = pathlib.Path("links.json")

def load_links() -> list[str]:
    if LINKS_FILE.exists():
        return json.loads(LINKS_FILE.read_text())
    return []

def save_links(links: list[str]):
    LINKS_FILE.write_text(json.dumps(links, indent=2))

def normalize(href: str) -> str | None:
    """Convert relative href to absolute URL; return None if not a fragrance page."""
    if not href:
        return None
    url = href if href.startswith("http") else BASE + href
    if not re.search(r'-\d+\.html$', url):
        return None
    return url

def main():
    links = load_links()
    seen = set(links)
    idx = 0

    print(f"Starting with {len(links)} links in queue")

    _WINDOW = 100 
    timestamps: list[float] = []

    with FragranticaDB() as db:
        with FragranticaScraper(db=db) as scraper:
            try:
                while idx < len(links):
                    url = links[idx]
                    idx += 1
                    total = len(links)

                    if db.is_scraped(url):
                        print(f"[{idx}/{total}] skip  {url}")
                        similar = []
                    else:
                        data = scraper.scrape_and_save(url)
                        if data:
                            name = data.get('name', '?')
                            rating = data.get('rating')
                            votes = data.get('votes')
                            timestamps.append(time.time())
                            if len(timestamps) > _WINDOW:
                                timestamps.pop(0)
                            if len(timestamps) >= 2:
                                span = timestamps[-1] - timestamps[0]
                                rate_per_min = (len(timestamps) - 1) / span * 60
                                rate_suffix = f"  {rate_per_min:.1f}/min  {rate_per_min * 60:.0f}/hr"
                            else:
                                rate_suffix = ""
                            print(f"[{idx}/{total}] {name} — {rating} ({votes} votes){rate_suffix}")
                            similar = data.get('similar_fragrances_json') or []
                        else:
                            print(f"[{idx}/{total}] FAILED  {url}")
                            similar = []

                    added = 0
                    for item in similar:
                        href = item.get('url') if isinstance(item, dict) else item
                        new_url = normalize(href)
                        if new_url and new_url not in seen and not db.is_scraped(new_url):
                            seen.add(new_url)
                            links.append(new_url)
                            added += 1

                    if added:
                        save_links(links)
                        print(f"  → +{added} new links (queue now {len(links)})")

            except KeyboardInterrupt:
                print(f"\nInterrupted at index {idx}. Queue has {len(links)} links.")
                save_links(links)

    print(f"Done. Processed up to index {idx} of {len(links)} total links.")

if __name__ == '__main__':
    main()


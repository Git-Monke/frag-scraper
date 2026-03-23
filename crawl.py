import json, queue, time, random, pathlib, re, threading
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

_MAX_WORKERS = 3
_WINDOW = 100


def main():
    links = load_links()
    seen = set(links)

    print(f"Starting with {len(links)} links in queue")

    url_queue: queue.Queue = queue.Queue()
    for url in links:
        url_queue.put(url)

    timestamps: list[float] = []
    ts_lock      = threading.Lock()
    links_lock   = threading.Lock()
    print_lock   = threading.Lock()
    counter_lock = threading.Lock()
    completed    = [0]   # mutable int for display index
    stop_event   = threading.Event()

    with FragranticaDB() as db:
        with FragranticaScraper(db=db) as scraper:

            def worker(worker_index: int):
                time.sleep(worker_index * random.uniform(1, 3))  # stagger initial startup
                from playwright.sync_api import sync_playwright
                pw = sync_playwright().start()
                browser, context = scraper._make_browser(pw)
                local_count = 0
                try:
                    while not stop_event.is_set():
                        try:
                            url = url_queue.get(timeout=2)
                        except queue.Empty:
                            break

                        if url is None:   # sentinel
                            url_queue.task_done()
                            break

                        if db.is_scraped(url):
                            with counter_lock:
                                completed[0] += 1
                                idx = completed[0]
                            with print_lock:
                                print(f"[{idx}/{len(links)}] skip  {url}")
                            url_queue.task_done()
                            continue

                        data = scraper._scrape_page(url, context)

                        if data and db:
                            all_notes = []
                            for layer in ('top_notes_json', 'middle_notes_json', 'base_notes_json'):
                                all_notes.extend(data.get(layer) or [])
                            for layer in ('top_notes_json', 'middle_notes_json', 'base_notes_json'):
                                for note in (data.get(layer) or []):
                                    note.pop('image_url', None)
                            db.upsert_fragrance(data)
                            db.upsert_notes(all_notes)

                        with counter_lock:
                            completed[0] += 1
                            idx = completed[0]

                        if data:
                            local_count += 1

                            if scraper.restart_every and local_count % scraper.restart_every == 0:
                                with print_lock:
                                    print("Restarting browser to free memory...")
                                try:
                                    context.close()
                                    browser.close()
                                except Exception:
                                    pass
                                browser, context = scraper._make_browser(pw)

                            with ts_lock:
                                timestamps.append(time.time())
                                if len(timestamps) > _WINDOW:
                                    timestamps.pop(0)
                                ts = list(timestamps)

                            rate_suffix = ""
                            if len(ts) >= 2:
                                span = ts[-1] - ts[0]
                                rpm = (len(ts) - 1) / span * 60
                                rate_suffix = f"  {rpm:.1f}/min  {rpm * 60:.0f}/hr"

                            name   = data.get('name', '?')
                            rating = data.get('rating')
                            votes  = data.get('votes')
                            with print_lock:
                                print(f"[{idx}/{len(links)}] {name} — {rating} ({votes} votes){rate_suffix}")

                            similar = data.get('similar_fragrances_json') or []
                            added = 0
                            with links_lock:
                                for item in similar:
                                    href = item.get('url') if isinstance(item, dict) else item
                                    new_url = normalize(href)
                                    if new_url and new_url not in seen and not db.is_scraped(new_url):
                                        seen.add(new_url)
                                        links.append(new_url)
                                        url_queue.put(new_url)  # before task_done so q.join() waits
                                        added += 1
                            if added:
                                save_links(links)
                                with print_lock:
                                    print(f"  → +{added} new links (queue now {len(links)})")
                        else:
                            with print_lock:
                                print(f"[{idx}/{len(links)}] FAILED  {url}")

                        url_queue.task_done()
                finally:
                    try:
                        context.close()
                        browser.close()
                        pw.stop()
                    except Exception:
                        pass

            threads = [threading.Thread(target=worker, args=(i,), daemon=True)
                       for i in range(_MAX_WORKERS)]
            for t in threads:
                t.start()

            try:
                url_queue.join()
            except KeyboardInterrupt:
                print(f"\nInterrupted. Queue has {len(links)} links.")
                stop_event.set()
                # drain so q.join() can unblock
                while not url_queue.empty():
                    try:
                        url_queue.get_nowait()
                        url_queue.task_done()
                    except queue.Empty:
                        break
                save_links(links)
            finally:
                stop_event.set()
                for t in threads:
                    t.join(timeout=30)

    print(f"Done. Processed {completed[0]} of {len(links)} total links.")

if __name__ == '__main__':
    main()


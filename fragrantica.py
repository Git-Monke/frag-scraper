"""
fragrantica.py — Fragrantica scraper module.

Classes:
    FragranticaDB      — SQLite persistence layer
    FragranticaParser  — Static HTML parsers (try/except; always saves partial data)
    FragranticaScraper — HTTP layer; wraps parser and DB
"""

import json
import re
import sqlite3
import subprocess
import time
import random
from datetime import datetime, timezone

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth
from bs4 import BeautifulSoup


# ── Helpers ────────────────────────────────────────────────────────────────────

def _parse_abbrev_count(s: str) -> int | None:
    """Parse abbreviated vote counts: '155' → 155, '2.9k' → 2900, '11.2k' → 11200."""
    if not s:
        return None
    s = s.strip().replace(',', '')
    try:
        if 'k' in s.lower():
            return int(float(s.lower().replace('k', '')) * 1000)
        return int(s)
    except (ValueError, AttributeError):
        return None


# ── FragranticaDB ──────────────────────────────────────────────────────────────

class FragranticaDB:
    def __init__(self, db_path: str = "fragrances.db"):
        self._conn = sqlite3.connect(db_path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._create_tables()

    def _create_tables(self):
        self._conn.executescript("""
        CREATE TABLE IF NOT EXISTS fragrances (
            id                      INTEGER PRIMARY KEY,
            url                     TEXT UNIQUE NOT NULL,
            name                    TEXT NOT NULL,
            brand                   TEXT,
            year                    INTEGER,
            rating                  REAL,
            votes                   INTEGER,
            scraped_at              TEXT NOT NULL,

            longevity_very_weak     INTEGER,
            longevity_weak          INTEGER,
            longevity_moderate      INTEGER,
            longevity_long_lasting  INTEGER,
            longevity_eternal       INTEGER,

            sillage_intimate        INTEGER,
            sillage_moderate        INTEGER,
            sillage_strong          INTEGER,
            sillage_enormous        INTEGER,

            rating_love             INTEGER,
            rating_like             INTEGER,
            rating_ok               INTEGER,
            rating_dislike          INTEGER,
            rating_hate             INTEGER,

            season_spring           INTEGER,
            season_summer           INTEGER,
            season_fall             INTEGER,
            season_winter           INTEGER,

            time_day                INTEGER,
            time_night              INTEGER,

            gender_female           INTEGER,
            gender_more_female      INTEGER,
            gender_unisex           INTEGER,
            gender_more_male        INTEGER,
            gender_male             INTEGER,

            price_way_overpriced    INTEGER,
            price_overpriced        INTEGER,
            price_ok                INTEGER,
            price_good_value        INTEGER,
            price_great_value       INTEGER,

            top_notes_json          TEXT,
            middle_notes_json       TEXT,
            base_notes_json         TEXT,
            accords_json            TEXT,
            image_url               TEXT
        );

        CREATE TABLE IF NOT EXISTS notes (
            name       TEXT PRIMARY KEY,
            image_url  TEXT
        );
        """)
        self._conn.commit()

    def upsert_fragrance(self, data: dict) -> int:
        frag_id = data['id']

        cols = [
            'id', 'url', 'name', 'brand', 'year', 'rating', 'votes', 'scraped_at',
            'longevity_very_weak', 'longevity_weak', 'longevity_moderate',
            'longevity_long_lasting', 'longevity_eternal',
            'sillage_intimate', 'sillage_moderate', 'sillage_strong', 'sillage_enormous',
            'rating_love', 'rating_like', 'rating_ok', 'rating_dislike', 'rating_hate',
            'season_spring', 'season_summer', 'season_fall', 'season_winter',
            'time_day', 'time_night',
            'gender_female', 'gender_more_female', 'gender_unisex',
            'gender_more_male', 'gender_male',
            'price_way_overpriced', 'price_overpriced', 'price_ok',
            'price_good_value', 'price_great_value',
            'top_notes_json', 'middle_notes_json', 'base_notes_json',
            'accords_json', 'image_url',
        ]

        row = {c: data.get(c) for c in cols}
        for json_col in ('top_notes_json', 'middle_notes_json', 'base_notes_json',
                         'accords_json'):
            v = row.get(json_col)
            if isinstance(v, (list, dict)):
                row[json_col] = json.dumps(v)

        col_list = ', '.join(cols)
        placeholders = ', '.join(f':{c}' for c in cols)
        self._conn.execute(
            f"INSERT OR REPLACE INTO fragrances ({col_list}) VALUES ({placeholders})",
            row,
        )

        self._conn.commit()
        return frag_id

    def upsert_notes(self, notes: list[dict]):
        """Insert or ignore note name→image_url pairs."""
        for note in notes:
            name = note.get('name')
            image_url = note.get('image_url')
            if name:
                self._conn.execute(
                    "INSERT OR IGNORE INTO notes (name, image_url) VALUES (?, ?)",
                    (name, image_url),
                )
        self._conn.commit()

    def is_scraped(self, url: str) -> bool:
        m = re.search(r'-(\d+)\.html$', url)
        if m:
            row = self._conn.execute(
                "SELECT 1 FROM fragrances WHERE id = ?", (int(m.group(1)),)
            ).fetchone()
        else:
            row = self._conn.execute(
                "SELECT 1 FROM fragrances WHERE url = ?", (url,)
            ).fetchone()
        return row is not None

    def get_all(self) -> list[dict]:
        rows = self._conn.execute("SELECT * FROM fragrances").fetchall()
        return [dict(r) for r in rows]

    def get_by_url(self, url: str) -> dict | None:
        row = self._conn.execute(
            "SELECT * FROM fragrances WHERE url = ?", (url,)
        ).fetchone()
        return dict(row) if row else None

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


# ── FragranticaParser ──────────────────────────────────────────────────────────

class FragranticaParser:

    @staticmethod
    def parse(soup, url: str) -> dict:
        data = {}
        data.update(FragranticaParser._parse_basic(soup, url))
        data.update(FragranticaParser._parse_rating(soup))
        data['accords_json'] = FragranticaParser._parse_accords(soup)
        data['image_url'] = FragranticaParser._parse_fragrance_image(soup)
        data.update(FragranticaParser._parse_notes(soup))
        data.update(FragranticaParser._parse_vote_widget(soup, 'longevity'))
        data.update(FragranticaParser._parse_vote_widget(soup, 'sillage'))
        data.update(FragranticaParser._parse_vote_widget(soup, 'price'))
        data.update(FragranticaParser._parse_vote_widget(soup, 'gender'))
        data.update(FragranticaParser._parse_distribution_pcts(soup, 'rating'))
        data.update(FragranticaParser._parse_distribution_pcts(soup, 'season'))
        data.update(FragranticaParser._parse_distribution_pcts(soup, 'time'))
        data['similar_fragrances_json'] = FragranticaParser._parse_similar(soup)
        data['scraped_at'] = datetime.now(timezone.utc).isoformat()
        return data

    @staticmethod
    def _parse_basic(soup, url: str) -> dict:
        result = {'url': url, 'id': None, 'name': None, 'brand': None, 'year': None}
        try:
            m = re.search(r'-(\d+)\.html$', url)
            if m:
                result['id'] = int(m.group(1))
        except Exception:
            pass

        try:
            h1 = soup.find('h1', attrs={'itemprop': 'name'})
            if h1:
                full = h1.get_text(separator=' ', strip=True)
                for suffix in (' for women and men', ' for women', ' for men'):
                    if full.lower().endswith(suffix):
                        full = full[:-len(suffix)].strip()
                        break
                result['name'] = full
        except Exception:
            pass

        try:
            brand_tag = soup.select_one('[itemprop="brand"] [itemprop="name"]')
            if brand_tag:
                result['brand'] = brand_tag.get_text(strip=True)
        except Exception:
            pass

        try:
            # Year appears in a span near the title section, e.g. "(2020)" or just "2020"
            # Search within the first ~2000 chars of the page to avoid noisy matches
            early_text = soup.get_text()[:3000]
            m = re.search(r'\b(19[5-9]\d|20[0-3]\d)\b', early_text)
            if m:
                result['year'] = int(m.group(1))
        except Exception:
            pass

        return result

    @staticmethod
    def _parse_fragrance_image(soup) -> str | None:
        try:
            img = soup.find('img', attrs={'itemprop': 'image'})
            if img:
                return img.get('src')
        except Exception:
            pass
        return None

    @staticmethod
    def _parse_rating(soup) -> dict:
        result = {'rating': None, 'votes': None}
        try:
            rv = soup.find(itemprop='ratingValue')
            if rv:
                result['rating'] = float(rv.get_text(strip=True))
        except Exception:
            pass
        try:
            rc = soup.find(itemprop='ratingCount')
            if rc:
                content = rc.get('content') or rc.get_text(strip=True).replace(',', '')
                result['votes'] = int(content)
        except Exception:
            pass
        return result

    @staticmethod
    def _parse_accords(soup) -> list:
        """
        Primary: parse the /accords-search/ query string (exact percentages from Fragrantica).
        Fallback: parse width-% style bars with span.truncate labels.
        """
        try:
            accord_link = soup.find('a', href=re.compile(r'/accords-search/'))
            if accord_link:
                href = accord_link.get('href', '')
                qs = href.split('?', 1)[-1] if '?' in href else ''
                accords = []
                for part in qs.split('&'):
                    if '=' in part:
                        name, val = part.split('=', 1)
                        try:
                            accords.append({
                                'name': name.replace('-', ' ').strip(),
                                'strength_pct': float(val),
                            })
                        except ValueError:
                            pass
                if accords:
                    return accords
        except Exception:
            pass

        accords = []
        try:
            for div in soup.find_all('div', style=re.compile(r'width:\s*[\d.]+%')):
                span = div.find('span', class_='truncate')
                if not span:
                    continue
                name = span.get_text(strip=True)
                if not name or ',' in name or '…' in name or '...' in name or len(name) > 40:
                    continue
                m = re.search(r'width:\s*([\d.]+)%', div.get('style', ''))
                if m:
                    accords.append({'name': name, 'strength_pct': float(m.group(1))})
        except Exception:
            pass

        return accords

    @staticmethod
    def _parse_notes(soup) -> dict:
        """Parse official note pyramid. Handles full hierarchy, partial hierarchy, and flat lists."""
        result = {
            'top_notes_json': [],
            'middle_notes_json': [],
            'base_notes_json': [],
        }
        try:
            all_pyramids = soup.find_all(id='pyramid')
            if not all_pyramids:
                return result
            pyramid = all_pyramids[-1]

            h4s = pyramid.find_all('h4')

            if h4s:
                # Original depth-agnostic descendants walk — works regardless of nesting structure
                current_layer = None
                for el in pyramid.descendants:
                    if not hasattr(el, 'name') or el.name is None:
                        continue
                    if el.name == 'h4':
                        text = el.get_text(strip=True).lower()
                        if 'top' in text:
                            current_layer = 'top_notes_json'
                        elif 'middle' in text or 'heart' in text:
                            current_layer = 'middle_notes_json'
                        elif 'base' in text:
                            current_layer = 'base_notes_json'
                        else:
                            current_layer = None
                        continue
                    if (el.name == 'a'
                            and current_layer is not None
                            and 'pyramid-note-link' in (el.get('class') or [])):
                        img = el.find('img')
                        if not img:
                            continue
                        name = img.get('alt', '').strip() or el.get_text(strip=True)
                        if not name:
                            continue
                        m = re.search(r'width:\s*([\d.]+)rem', img.get('style', ''))
                        strength_pct = round((float(m.group(1)) / 5.0) * 100, 1) if m else None
                        img_src = img.get('src', '').strip() or None
                        result[current_layer].append({'name': name, 'strength_pct': strength_pct, 'image_url': img_src})
            else:
                # Flat list with no hierarchy — store everything as top notes
                for a in pyramid.select('a.pyramid-note-link'):
                    img = a.find('img')
                    if not img:
                        continue
                    name = img.get('alt', '').strip() or a.get_text(strip=True)
                    if not name:
                        continue
                    m = re.search(r'width:\s*([\d.]+)rem', img.get('style', ''))
                    strength_pct = round((float(m.group(1)) / 5.0) * 100, 1) if m else None
                    img_src = img.get('src', '').strip() or None
                    result['top_notes_json'].append({'name': name, 'strength_pct': strength_pct, 'image_url': img_src})
        except Exception:
            pass

        return result

    @staticmethod
    def _parse_perfumers(soup) -> list:
        perfumers = []
        try:
            seen = set()
            for a in soup.select('a[href*="/noses/"]'):
                href = a.get('href', '').rstrip('/')
                if not href or href in seen:
                    continue
                name = a.get_text(strip=True)
                if name:
                    seen.add(href)
                    perfumers.append(name)
        except Exception:
            pass
        return perfumers

    @staticmethod
    def _parse_vote_widget(soup, title: str) -> dict:
        """
        Parse raw vote counts for longevity, sillage, price, or gender sections.

        HTML pattern: <div class="tw-perf-card ...">
            <span ...>LONGEVITY</span>   ← header
            <div class="mt-3 space-y-2">
              <div class="flex items-center ...">  ← row
                <div ...><span ...>very weak</span></div>  ← label div
                <div ...><span ...>155</span></div>        ← count div
                <div ...>...</div>                         ← bar (ignored)
              </div>
              ...
            </div>
        </div>
        """
        label_maps = {
            'longevity': {
                'very weak': 'longevity_very_weak',
                'weak': 'longevity_weak',
                'moderate': 'longevity_moderate',
                'long lasting': 'longevity_long_lasting',
                'eternal': 'longevity_eternal',
            },
            'sillage': {
                'intimate': 'sillage_intimate',
                'moderate': 'sillage_moderate',
                'strong': 'sillage_strong',
                'enormous': 'sillage_enormous',
            },
            'price': {
                'way overpriced': 'price_way_overpriced',
                'overpriced': 'price_overpriced',
                'ok': 'price_ok',
                'good value': 'price_good_value',
                'great value': 'price_great_value',
            },
            'gender': {
                'female': 'gender_female',
                'more female': 'gender_more_female',
                'unisex': 'gender_unisex',
                'more male': 'gender_more_male',
                'male': 'gender_male',
            },
        }

        result = {}
        label_map = label_maps.get(title, {})
        if not label_map:
            return result

        header_match = 'price value' if title == 'price' else title

        try:
            for card in soup.select('.tw-perf-card'):
                header = next(
                    (s for s in card.find_all('span')
                     if ' '.join(s.get_text(strip=True).lower().split()) == header_match),
                    None
                )
                if not header:
                    continue
                for row in card.select('.space-y-2 > div'):
                    spans = row.find_all('span')
                    if len(spans) < 2:
                        continue
                    label = ' '.join(spans[0].get_text(strip=True).lower().split())
                    col = label_map.get(label)
                    if col:
                        result[col] = _parse_abbrev_count(spans[1].get_text(strip=True))
                if result:
                    return result
        except Exception:
            pass

        return result

    @staticmethod
    def _parse_distribution_pcts(soup, title: str) -> dict:
        """
        Parse opinion polls: rating (love/like/ok/dislike/hate), season, time of day.

        HTML pattern: <div class="tw-rating-card">
            <div class="tw-rating-card-header">
              <span class="tw-rating-card-label">Rating</span>
            </div>
            <div class="p-2">
              <div ...>
                <div class="flex flex-col items-center ...">  ← item
                  <div>...</div>  ← icon
                  <span ...>love</span>                       ← LABEL (direct child)
                  <div ...>
                    <div>...</div>                            ← bar
                    <span class="... tabular-nums">16.4k</span>  ← COUNT
                  </div>
                </div>
                ...
              </div>
            </div>
        </div>
        Season and time share the "When To Wear" card.
        """
        label_maps = {
            'rating': {
                'love': 'rating_love',
                'like': 'rating_like',
                'ok': 'rating_ok',
                'dislike': 'rating_dislike',
                'hate': 'rating_hate',
            },
            'season': {
                'spring': 'season_spring',
                'summer': 'season_summer',
                'fall': 'season_fall',
                'autumn': 'season_fall',
                'winter': 'season_winter',
            },
            'time': {
                'day': 'time_day',
                'night': 'time_night',
            },
        }

        result = {}
        label_map = label_maps.get(title, {})
        if not label_map:
            return result

        card_label = {'rating': 'rating', 'season': 'when to wear', 'time': 'when to wear'}
        target = card_label.get(title, '')

        try:
            for card in soup.select('.tw-rating-card'):
                label_span = card.select_one('.tw-rating-card-label')
                if not label_span or label_span.get_text(strip=True).lower() != target:
                    continue
                for item in card.select('div.flex.flex-col.items-center'):
                    count_span = item.find('span', class_=re.compile(r'tabular-nums'))
                    if not count_span:
                        continue
                    label_text = next(
                        (c.get_text(strip=True).lower() for c in item.children
                         if hasattr(c, 'name') and c.name == 'span'),
                        None
                    )
                    col = label_map.get(label_text)
                    if col:
                        result[col] = _parse_abbrev_count(count_span.get_text(strip=True))
                if result:
                    return result
        except Exception:
            pass

        return result

    @staticmethod
    def _parse_similar(soup) -> list:
        """Collect similar fragrances linked from the page (href contains /perfume/)."""
        similar = []
        try:
            seen = set()
            for a in soup.select('a[href*="/perfume/"]'):
                href = a.get('href', '')
                # Must end in -NNN.html (actual fragrance pages)
                if not re.search(r'-\d+\.html$', href):
                    continue
                if href in seen:
                    continue
                name = a.get_text(strip=True)
                if name:
                    seen.add(href)
                    similar.append({'name': name, 'url': href})
        except Exception:
            pass
        return similar



# ── FragranticaScraper ─────────────────────────────────────────────────────────

class FragranticaScraper:

    def __init__(self, db=None, delay=(5.0, 9.0), retry_wait=60, max_retries=3, restart_every=50):
        self.db = db
        self.delay = delay
        self.retry_wait = retry_wait
        self.max_retries = max_retries
        self.restart_every = restart_every
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(
            headless=False,
            args=['--disable-features=Translate'],
        )

    def _cycle_vpn(self):
        """Reconnect Mullvad to get a fresh random US server."""
        print("Cycling VPN (reconnecting for fresh US server)...")
        try:
            subprocess.run(['mullvad', 'relay', 'set', 'location', 'us'],
                           check=True, capture_output=True)
            subprocess.run(['mullvad', 'reconnect'], check=True, capture_output=True)
            time.sleep(8)   # wait for new IP to establish
            print("VPN cycled.")
        except Exception as e:
            print(f"VPN cycle failed ({e}), falling back to 60s wait...")
            time.sleep(60)

    def scrape(self, url: str) -> dict | None:
        for attempt in range(self.max_retries):
            try:
                page = self._browser.new_page()
                Stealth().apply_stealth_sync(page)
                try:
                    response = page.goto(url, wait_until='domcontentloaded', timeout=30000)
                    if response and response.status == 429:
                        print(f"Rate limited on: {url}")
                        page.close()
                        self._cycle_vpn()
                        continue
                    if response and response.status != 200:
                        print(f"HTTP {response.status} for {url}")
                        page.close()
                        self._cycle_vpn()
                        continue
                    # Scroll 500px at 10Hz until the demographics section (the deepest lazy
                    # section we need) enters the viewport — this triggers its IntersectionObserver.
                    # Stop after 8000px to avoid getting stuck on abnormally long pages.
                    for _ in range(20):
                        page.evaluate("window.scrollBy(0, 250)")
                        time.sleep(0.1)
                        reached = page.evaluate("""
                            () => {
                                const el = document.getElementById('demographics');
                                if (!el) return false;
                                const rect = el.getBoundingClientRect();
                                return rect.top < window.innerHeight + 500;
                            }
                        """)
                        if reached:
                            break

                    # Now poll for Vue API calls to finish loading the data
                    _SENTINELS = [
                        'top_notes_json',
                        'longevity_very_weak', 'sillage_intimate',
                        'price_ok', 'gender_male',
                        'rating_love', 'season_spring', 'time_day',
                    ]
                    data = None
                    for poll in range(15):
                        time.sleep(1)
                        html = page.content()
                        soup = BeautifulSoup(html, 'html.parser')
                        data = FragranticaParser.parse(soup, url)
                        notes_data = FragranticaParser._parse_notes(soup)
                        data.update(notes_data)
                        if all(data.get(f) is not None for f in _SENTINELS):
                            break
                    if data is None:
                        html = page.content()
                        soup = BeautifulSoup(html, 'html.parser')
                        data = FragranticaParser.parse(soup, url)
                        notes_data = FragranticaParser._parse_notes(soup)
                        data.update(notes_data)
                finally:
                    page.close()
                return data
            except Exception as e:
                print(f"Error scraping {url} (attempt {attempt + 1}/{self.max_retries}): {e}")
                if attempt < self.max_retries - 1:
                    self._cycle_vpn()
        return None

    def _restart_browser(self):
        print("Restarting browser to free memory...")
        try:
            self._browser.close()
        except Exception:
            pass
        self._browser = self._pw.chromium.launch(
            headless=False,
            args=['--disable-features=Translate'],
        )

    def close(self):
        self._browser.close()
        self._pw.stop()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def scrape_and_save(self, url: str) -> dict | None:
        data = self.scrape(url)
        if data and self.db:
            # Collect notes with image_url BEFORE stripping from the JSON columns
            all_notes = []
            for layer in ('top_notes_json', 'middle_notes_json', 'base_notes_json'):
                all_notes.extend(data.get(layer) or [])

            # Strip image_url from note dicts so it isn't serialized into JSON columns
            for layer in ('top_notes_json', 'middle_notes_json', 'base_notes_json'):
                for note in (data.get(layer) or []):
                    note.pop('image_url', None)

            self.db.upsert_fragrance(data)
            self.db.upsert_notes(all_notes)
        return data

    def scrape_many(self, urls: list, skip_existing: bool = True,
                    progress: bool = True) -> list[dict]:
        results = []
        total = len(urls)
        scraped_count = 0
        # Ring buffer of (timestamp, count) snapshots for moving-average rate.
        # We keep one snapshot per successful scrape; the window covers the last
        # 100 scrapes (≈ a few minutes at normal pace) for the per-minute figure,
        # and we extrapolate to per-hour from that same rate.
        _WINDOW = 100
        timestamps: list[float] = []

        for i, url in enumerate(urls, 1):
            if skip_existing and self.db and self.db.is_scraped(url):
                if progress:
                    print(f"[{i}/{total}] skip  {url}")
                continue

            data = self.scrape_and_save(url)
            if data:
                results.append(data)
                scraped_count += 1
                timestamps.append(time.monotonic())
                if len(timestamps) > _WINDOW:
                    timestamps.pop(0)

                if progress:
                    name = data.get('name', '?')
                    rating = data.get('rating')
                    votes = data.get('votes')
                    if len(timestamps) >= 2:
                        span = timestamps[-1] - timestamps[0]
                        rate_per_min = (len(timestamps) - 1) / span * 60
                        rate_suffix = f"  {rate_per_min:.1f}/min  {rate_per_min * 60:.0f}/hr"
                    else:
                        rate_suffix = ""
                    print(f"[{i}/{total}] {name} — {rating} ({votes} votes){rate_suffix}")

                if self.restart_every and scraped_count % self.restart_every == 0:
                    self._restart_browser()
            else:
                if progress:
                    print(f"[{i}/{total}] FAILED  {url}")

            if i < total:
                time.sleep(random.uniform(*self.delay))

        return results

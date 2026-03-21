# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Python tool for scraping Fragrantica fragrance reviews (currently targeting Lattafa Perfumes) and ranking them using a Bayesian algorithm to surface truly top-rated fragrances with statistical confidence.

## Commands

```bash
# Activate virtual environment (Windows)
source venv/Scripts/activate

# Run the scraper (reads links.json, writes to fragrances.db)
python scrape.py

# Run analysis (reads fragrance_data.json, prints ranked results)
python analysis.py

# Quick import test (no DB needed)
python -c "from fragrantica import FragranticaScraper; s = FragranticaScraper(); print('OK')"

# Inspect the database
sqlite3 fragrances.db ".tables"
sqlite3 fragrances.db "SELECT name, rating, votes FROM fragrances LIMIT 10;"
```

No test runner, linter, or build system is configured.

## Architecture

**Data flow:** `links.json` → `scrape.py` → `fragrances.db` → `analysis.py` → console output

```
fragrantica.py    — module: FragranticaDB, FragranticaParser, FragranticaScraper
scrape.py         — thin runner (~10 lines); imports fragrantica
analysis.py       — unchanged; reads fragrance_data.json (legacy)
fragrances.db     — SQLite database (auto-created by FragranticaDB)
fragrance_data.json — legacy JSON file (superseded by fragrances.db)
```

### fragrantica.py

**`FragranticaDB`** — SQLite persistence. Schema has three tables:
- `fragrances` — one row per fragrance; flat columns for all vote distributions
- `accords` — normalized (fragrance_id, name, strength_pct)
- `perfumers` — normalized (fragrance_id, name)

**`FragranticaParser`** — all static methods; each sub-parser is try/except so partial data always saves:
- `_parse_basic` — id (from URL regex), name, brand, year, gender
- `_parse_rating` — itemprop ratingValue / ratingCount
- `_parse_accords` — accords-search query string (primary) or width-% style bars (fallback)
- `_parse_notes` — official note pyramid (data-v-062802d2); top/middle/base layers stored as JSON
- `_parse_perfumers` — `a[href*="/noses/"]`
- `_parse_vote_widget` — longevity / sillage / price vote counts (abbreviated: "2.9k" → 2900)
- `_parse_distribution_pcts` — rating/season/time/gender polls (index="N" containers)
- `_parse_similar` — `a[href*="/perfume/"]` links

**`FragranticaScraper`** — HTTP layer via `cloudscraper`; handles 429 retry; `scrape_many` skips already-scraped URLs.

### scrape.py
- Reads URLs from `links.json`
- Opens `FragranticaDB`, creates `FragranticaScraper`, calls `scrape_many`
- Resumable: `skip_existing=True` skips URLs already in the DB
- Rate-limit handling: 60s wait on 429; random 5–9s delay between requests

### analysis.py
- Currently reads `fragrance_data.json` (legacy JSON); unchanged
- Bayesian ranking formula: `(C * m + R * v) / (m + v)`

### Data files
- `links.json` — input list of Fragrantica URLs
- `fragrances.db` — primary output (SQLite)
- `fragrance_data.json` — legacy flat JSON (superseded)

## Dependencies

Managed in `venv/` (Python 3.14.2). Key packages: `cloudscraper`, `beautifulsoup4`, `requests`. No `requirements.txt` exists — install manually if recreating the environment.

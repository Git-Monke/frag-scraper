import json
from fragrantica import FragranticaScraper, FragranticaDB

with open('links.json') as f:
    links = json.load(f)

with FragranticaDB("fragrances_v2.db") as db:
    scraper = FragranticaScraper(db=db)
    scraper.scrape_many(links, skip_existing=True, progress=True)

print("Done!")

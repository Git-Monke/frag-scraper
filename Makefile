.PHONY: crawl crawl-headless app

crawl:
	python crawl.py

crawl-headless:
	xvfb-run --server-args="-screen 0 1920x1080x24" python crawl.py

app:
	python app.py

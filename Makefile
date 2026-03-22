PYTHON = venv/Scripts/python

.PHONY: crawl crawl-headless app

crawl:
	$(PYTHON) crawl.py

crawl-headless:
	xvfb-run --server-args="-screen 0 1920x1080x24" $(PYTHON) crawl.py

app:
	$(PYTHON) app.py

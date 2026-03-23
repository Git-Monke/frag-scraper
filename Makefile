.PHONY: crawl crawl-headless app

crawl:
	python crawl.py

crawl-headless:
	while true; do \
		timeout 1800 xvfb-run --server-args="-screen 0 1920x1080x24" python crawl.py; \
		echo "Restarting..."; \
		pkill -9 Xvfb || true; \
		rm -f /tmp/.X99-lock /tmp/.X*-lock; \
		sleep 3; \
	done
app:
	python app.py

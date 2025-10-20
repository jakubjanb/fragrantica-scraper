Fragrantica Perfume Scraper

This project crawls perfume pages on Fragrantica and stores brand, perfume name, rating, and votes in a local CSV file.

Important: Respect robots.txt and the website's terms. Use responsibly for personal/educational purposes.

Project layout:
- fragrantica_scraper/
  - __init__.py
  - crawler.py  # CLI module with the scraper logic
- main.py        # Optional convenience entrypoint to run the CLI
- requirements.txt
- README.md

Install dependencies:
- python -m pip install -r requirements.txt

Run examples:
- python -m fragrantica_scraper.crawler --start-url https://www.fragrantica.com/perfume/EIGHT-BOB/EIGHT-BOB-16295.html --max-pages 20 --out-csv perfumes.csv
- python -m fragrantica_scraper.crawler --start-url https://www.fragrantica.com/perfume/Chanel/Chance-21.html --max-pages 10 --out-csv perfumes.csv --delay-seconds 2.5

Notes:
- The crawler reads robots.txt and will skip disallowed URLs.
- Output CSV columns: brand, name, rating, votes, url, last_crawled.
- Re-running the scraper will append new perfumes and skip URLs already present in the CSV.

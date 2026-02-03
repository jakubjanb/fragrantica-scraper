git remote -vFragrantica Perfume Scraper

A command‑line scraper that crawls perfume pages on Fragrantica and saves: brand, perfume name, rating, votes, URL, and crawl timestamp into a CSV file.

Important: Always respect robots.txt and the website's terms. Use responsibly for personal/educational purposes only.

Project layout:
- fragrantica_scraper/
  - __init__.py
  - crawler.py  # CLI with the scraper logic
- main.py        # Optional convenience entry point to run the CLI
- requirements.txt
- README.md

Requirements
- Python 3.9+ (tested on recent 3.x)
- Install dependencies:
  - python -m pip install -r requirements.txt

Quick start
- Crawl starting from a specific perfume page:
  - python -m fragrantica_scraper.crawler --start-url https://www.fragrantica.com/perfume/EIGHT-BOB/EIGHT-BOB-16295.html --max-pages 20 --out-csv perfumes.csv
- Crawl a different seed with a bit more politeness delay:
  - python -m fragrantica_scraper.crawler --start-url https://www.fragrantica.com/perfume/Chanel/Chance-21.html --max-pages 10 --out-csv perfumes.csv --delay-seconds 2.5
- Brand mode: scrape only a single brand. If you omit --start-url and provide --brand, the crawler will seed itself from the brand's page and name the output CSV after the brand.
  - python -m fragrantica_scraper.crawler --brand "Giorgio Armani" --max-pages 50
  - Output file example: Saved Data/Giorgio_Armani.csv (or Giorgio_Armani.csv if Saved Data is not used)

You can also run through main.py:
- python main.py --start-url https://www.fragrantica.com/perfume/EIGHT-BOB/EIGHT-BOB-16295.html --max-pages 10

CLI options (from fragrantica_scraper/crawler.py)
- --start-url URL            Seed URL; can be specified multiple times. Example perfume URL: https://www.fragrantica.com/perfume/EIGHT-BOB/EIGHT-BOB-16295.html
- --brand NAME               Brand/company name to scrape. Only fragrances from this brand will be saved. If provided and --out-csv not overridden, the output defaults to <brand>.csv.
- --out-csv PATH             Path to output CSV file. Default: perfumes.csv.
- --max-pages N              Max perfume pages to save. Use 0 or a negative number for no limit. Default: 100.
- --delay-seconds S          Base politeness delay between requests (jitter is added). Default: 5.0s.
- --timeout S                HTTP timeout in seconds. Default: 20.0s.
- --user-agent UA            User-Agent string used for requests. Default: a generic PerfumeBot UA.
- --session-size N           Number of perfume pages to save before taking a longer cooldown break. Default: 30.
- --session-break-seconds S  Cooldown duration after each session. Default: 900s (15 minutes).
- --proxy URL                Proxy URL to use (http/https/socks5), e.g. http://user:pass@host:port
- --proxies-file PATH        File with one proxy per line (# comments allowed).
- --rotate-every N           Rotate proxy/User-Agent/Accept-Language after N processed perfume pages (0 disables). Default: 0.

How it works
- Robots: The crawler loads and obeys robots.txt and skips disallowed URLs.
- Link filtering: Only valid perfume URLs are followed; irrelevant sections (board, designers, search, news, etc.) are ignored.
- CSV schema: brand, name, rating, votes, url, last_crawled.
- Idempotency: When re-running, URLs already present in the CSV are skipped; new perfumes are appended.

Data location
- By default, the output CSV is written next to where you run the command (perfumes.csv) unless you specify --out-csv or use --brand which derives a sensible default name.
- Example CSVs collected for various brands are available under Saved Data/ in this repository.

Notes
- Network politeness and optional session cooldowns help reduce load and chance of being rate-limited.
- If you plan heavy crawling, consider using proxies and rotation and increase delays.
- For troubleshooting or to customize behavior further, inspect fragrantica_scraper/crawler.py.

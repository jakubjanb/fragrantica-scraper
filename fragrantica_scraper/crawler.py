#!/usr/bin/env python3
"""
Fragrantica perfume scraper

Crawls perfume pages on https://www.fragrantica.com and stores
brand, perfume name, rating and votes into a CSV file.

Usage examples:
    python -m fragrantica_scraper.crawler \
        --start-url https://www.fragrantica.com/perfume/EIGHT-BOB/EIGHT-BOB-16295.html \
        --max-pages 25 --out-csv perfumes.csv

Note: Respect robots.txt and the website's terms of service. This tool is for
personal/educational purposes only.
"""
import argparse
import collections
import csv
import datetime as dt
import os
import random
import re
import sys
import time
from typing import Optional, Tuple
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup
import urllib.robotparser as robotparser

DOMAIN = "www.fragrantica.com"
PERFUME_URL_RE = re.compile(
    r"^https?://(?:www\.)?fragrantica\.com/perfume/[^/]+/[^/]+-\d+\.html$",
    re.IGNORECASE,
)
# Path-only matcher for tighter link filtering
PERFUME_PATH_RE = re.compile(r"^/perfume/[^/]+/[^/]+-\d+\.html$", re.IGNORECASE)
AVOID_PREFIXES = (
    "/board/", "/designers/", "/search/", "/news/", "/articles/", "/perfumery/",
)
RATING_VOTES_RE = re.compile(
    r"Perfume\s+rating\s+([0-9]+(?:\.[0-9]+)?)\s+out\s+of\s+5\s+with\s+([\d,]+)\s+votes",
    re.IGNORECASE,
)
DESIGNER_LABEL_RE = re.compile(r"^\s*Designer\s*", re.IGNORECASE)

# ------------------------------------------------------------
# CSV helpers
# ------------------------------------------------------------
CSV_FIELDS = ["brand", "name", "rating", "votes", "url", "last_crawled"]

def ensure_csv_with_header(path: str):
    # Create the file with header if it doesn't exist
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writeheader()


def load_existing_urls(path: str):
    urls = set()
    if not os.path.exists(path):
        return urls
    try:
        with open(path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = (row.get("url") or "").strip()
                if url:
                    urls.add(url)
    except Exception:
        # If CSV is malformed, ignore for now (we'll append valid rows)
        pass
    return urls


def append_row(path: str, row: dict):
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writerow(row)

# ------------------------------------------------------------
# Parsing helpers
# ------------------------------------------------------------
def clean_space(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def parse_brand_from_page(soup: BeautifulSoup) -> Optional[str]:
    # Look for "Designer <Brand>"
    for node in soup.find_all(text=DESIGNER_LABEL_RE):
        # Often "Designer EIGHT & BOB"
        try:
            # Sometimes the brand is in the next sibling or parent context
            sibling_text = node.parent.get_text(" ", strip=True)
            m = re.search(r"Designer\s+(.*)", sibling_text, re.IGNORECASE)
            if m:
                return clean_space(m.group(1))
        except Exception:
            pass
    # Try a meta tag fallback
    og_title = soup.find("meta", attrs={"property": "og:title"})
    if og_title and og_title.get("content"):
        # Often "EIGHT & BOB EIGHT & BOB for men"
        txt = og_title["content"]
        # Brand is usually the first token(s) before the fragrance name; this is fuzzy.
        # We'll return None here and let URL fallback handle it.
    return None

def parse_name_from_page(soup: BeautifulSoup) -> Optional[str]:
    # Try the H1 first
    h1 = soup.find(["h1", "h2"])
    if h1:
        txt = clean_space(h1.get_text(" ", strip=True))
        # Commonly ends with "for men/women/unisex" — strip that if present
        txt = re.sub(r"\s+for\s+(men|women|unisex)\s*$", "", txt, flags=re.IGNORECASE)
        # Remove duplicated brand prefix if present — we keep it as fragrance name anyway
        return txt if txt else None

    # Try og:title
    og_title = soup.find("meta", attrs={"property": "og:title"})
    if og_title and og_title.get("content"):
        txt = clean_space(og_title["content"])
        txt = re.sub(r"\s+for\s+(men|women|unisex)\s*$", "", txt, flags=re.IGNORECASE)
        return txt if txt else None

    return None

def parse_rating_votes_from_text(text: str) -> Tuple[Optional[float], Optional[int]]:
    m = RATING_VOTES_RE.search(text)
    if not m:
        return None, None
    rating = float(m.group(1))
    votes = int(m.group(2).replace(",", ""))
    return rating, votes

def parse_brand_name_from_url(url: str) -> Tuple[Optional[str], Optional[str]]:
    # /perfume/<brand>/<name>-<id>.html
    try:
        path = urlparse(url).path
        parts = [p for p in path.split("/") if p]
        if len(parts) >= 3 and parts[0].lower() == "perfume":
            brand = parts[1]
            name_and_id = parts[2]
            # remove the -<id>.html suffix
            name = re.sub(r"-\d+\.html$", "", name_and_id, flags=re.IGNORECASE)
            # de-slug
            brand = clean_space(brand.replace("-", " ").replace("%26", "&").replace("%20", " "))
            name = clean_space(name.replace("-", " ").replace("%26", "&").replace("%20", " "))
            return brand, name
    except Exception:
        pass
    return None, None

# ------------------------------------------------------------
# Crawl + fetch
# ------------------------------------------------------------
DEFAULT_UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
]

def build_session(user_agent: str, timeout: float):
    # If the placeholder UA is used, pick a realistic one for the session
    if user_agent == "Mozilla/5.0 (compatible; PerfumeBot/1.0; +https://example.com/botinfo)":
        user_agent = random.choice(DEFAULT_UAS)
    s = requests.Session()
    s.headers.update({
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    })
    s.timeout = timeout
    return s

def can_fetch(rp: robotparser.RobotFileParser, ua: str, url: str) -> bool:
    try:
        return rp.can_fetch(ua, url)
    except Exception:
        return False

def normalize_url(url: str) -> Optional[str]:
    try:
        u = urlparse(url)
        if not u.scheme:
            return None
        if u.netloc.lower().startswith("fragrantica.com"):
            # force www
            u = u._replace(netloc=DOMAIN)
        # Remove fragments
        u = u._replace(fragment="")
        return urlunparse(u)
    except Exception:
        return None

def extract_links(base_url: str, soup: BeautifulSoup):
    links = set()
    for a in soup.find_all("a", href=True):
        full = urljoin(base_url, a["href"])  # absolute
        full = normalize_url(full)
        if not full:
            continue
        u = urlparse(full)
        if u.netloc != DOMAIN:
            continue
        path = u.path or "/"
        # Allow only perfume detail pages; skip sensitive/rate-limited sections
        if not PERFUME_PATH_RE.match(path):
            if any(path.startswith(p) for p in AVOID_PREFIXES):
                continue
            # Skip non-perfume paths altogether
            continue
        # Exclude obvious non-HTML assets
        if any(full.lower().endswith(ext) for ext in (".jpg", ".png", ".gif", ".svg", ".css", ".js", ".json", ".xml")):
            continue
        links.add(full)
    return links

def polite_sleep(delay_min: float, delay_max: float):
    time.sleep(random.uniform(delay_min, delay_max))


def backoff_sleep(resp, base_delay: float, attempt: int):
    """Honor Retry-After and apply exponential backoff with jitter."""
    retry_after = None
    if resp is not None:
        try:
            retry_after = resp.headers.get("Retry-After")
        except Exception:
            retry_after = None
    if retry_after:
        try:
            seconds = float(retry_after)
            time.sleep(max(seconds, base_delay))
            return
        except Exception:
            # If not numeric, fall through to exponential backoff
            pass
    sleep_min = base_delay * (2 ** (attempt - 1))
    sleep_max = sleep_min + 1.5
    polite_sleep(sleep_min, sleep_max)

# ------------------------------------------------------------
# Main scrape logic
# ------------------------------------------------------------
def scrape_perfume_page(url: str, soup: BeautifulSoup):
    page_text = soup.get_text(" ", strip=True)
    rating, votes = parse_rating_votes_from_text(page_text)

    brand = parse_brand_from_page(soup)
    name = parse_name_from_page(soup)

    # Fallbacks from URL when needed
    u_brand, u_name = parse_brand_name_from_url(url)
    if brand is None:
        brand = u_brand
    if name is None:
        name = u_name

    # Final cleanup
    brand = clean_space(brand or "")
    name = clean_space(name or "")

    return {
        "brand": brand or None,
        "name": name or None,
        "rating": rating,
        "votes": votes,
    }

def crawl(args):
    out_csv = args.out_csv
    ensure_csv_with_header(out_csv)
    existing_urls = load_existing_urls(out_csv)

    session = build_session(args.user_agent, args.timeout)
    rp = robotparser.RobotFileParser()
    rp.set_url(f"https://{DOMAIN}/robots.txt")
    try:
        rp.read()
    except Exception:
        print("[warn] Could not read robots.txt; exiting for safety.", file=sys.stderr)
        sys.exit(1)

    ua = args.user_agent
    queue = collections.deque()
    seen = set()

    # Seed URLs
    for su in args.start_url:
        su = normalize_url(su)
        if not su:
            continue
        if urlparse(su).netloc != DOMAIN:
            print(f"[skip] Out-of-domain seed: {su}")
            continue
        queue.append(su)
        seen.add(su)

    pages_processed = 0

    while queue and pages_processed < args.max_pages:
        url = queue.popleft()

        if not can_fetch(rp, ua, url):
            print(f"[robots] Disallowed: {url}")
            continue

        # Skip URLs already saved in the CSV if they look like perfume pages
        if PERFUME_URL_RE.match(url) and url in existing_urls:
            # Already saved from a previous run
            continue

        # Retry loop for transient errors and rate limiting
        MAX_RETRIES = 3
        RETRY_STATUSES = {429, 500, 502, 503, 504}
        attempt = 1
        success = False
        soup = None
        while True:
            try:
                resp = session.get(url, timeout=args.timeout)
            except requests.RequestException as e:
                if attempt >= MAX_RETRIES:
                    print(f"[error] Request failed (give up): {url} ({e})")
                    break
                print(f"[warn] Request exception, retrying {attempt}/{MAX_RETRIES}: {url} ({e})")
                backoff_sleep(resp=None, base_delay=args.delay_seconds, attempt=attempt)
                attempt += 1
                continue

            content_type = resp.headers.get("Content-Type", "")
            if resp.status_code in RETRY_STATUSES:
                if attempt < MAX_RETRIES:
                    print(f"[wait] Status {resp.status_code}, retrying {attempt}/{MAX_RETRIES}: {url}")
                    backoff_sleep(resp, base_delay=args.delay_seconds, attempt=attempt)
                    attempt += 1
                    continue
                print(f"[skip] Non-HTML or status {resp.status_code}: {url}")
                polite_sleep(args.delay_seconds, args.delay_seconds + 1.0)
                break
            if resp.status_code != 200 or "text/html" not in content_type:
                print(f"[skip] Non-HTML or status {resp.status_code}: {url}")
                polite_sleep(args.delay_seconds, args.delay_seconds + 1.0)
                break

            # Success
            soup = BeautifulSoup(resp.text, "lxml")
            success = True
            break

        if not success:
            # Could not fetch this URL successfully; move on
            continue

        # If this is a perfume page, parse & store
        if PERFUME_URL_RE.match(url):
            data = scrape_perfume_page(url, soup)
            if data["brand"] and data["name"] and (data["rating"] is not None) and (data["votes"] is not None):
                if url in existing_urls:
                    print(f"[skip] Already saved: {url}")
                else:
                    row = {
                        "brand": data["brand"],
                        "name": data["name"],
                        "rating": data["rating"],
                        "votes": data["votes"],
                        "url": url,
                        "last_crawled": dt.datetime.utcnow().isoformat(),
                    }
                    append_row(out_csv, row)
                    existing_urls.add(url)
                    print(f"[saved] {data['brand']} — {data['name']} | {data['rating']} (votes: {data['votes']})")
            else:
                print(f"[note] Missing fields (brand/name/rating/votes) for {url}")

            pages_processed += 1

        # Enqueue more links to continue crawling (only if we still have budget)
        if pages_processed < args.max_pages:
            for link in extract_links(url, soup):
                if link in seen:
                    continue
                # Stay within domain; broadly traverse to find more perfume pages.
                seen.add(link)
                queue.append(link)

        polite_sleep(args.delay_seconds, args.delay_seconds + 1.5)

    print(f"\nDone. Pages processed (perfume pages saved/attempted): {pages_processed}")
    print(f"CSV path: {out_csv}")

# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Crawl Fragrantica perfume pages and store rating/votes in a CSV file."
    )
    parser.add_argument(
        "--start-url",
        action="append",
        required=True,
        help="Seed URL (can be specified multiple times). Example: https://www.fragrantica.com/perfume/EIGHT-BOB/EIGHT-BOB-16295.html",
    )
    parser.add_argument("--out-csv", default="perfumes.csv", help="Path to output CSV file.")
    parser.add_argument("--max-pages", type=int, default=100, help="Max perfume pages to save.")
    parser.add_argument("--delay-seconds", type=float, default=5.0, help="Base politeness delay between requests.")
    parser.add_argument("--timeout", type=float, default=20.0, help="HTTP timeout in seconds.")
    parser.add_argument(
        "--user-agent",
        default="Mozilla/5.0 (compatible; PerfumeBot/1.0; +https://example.com/botinfo)",
        help="User-Agent string used for requests.",
    )
    args = parser.parse_args()
    crawl(args)

if __name__ == "__main__":
    main()

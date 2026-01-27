#!/usr/bin/env python3
"""
Fragrantica scraper crawler orchestration.

Holds the core crawling loop, while parsing, networking, storage, and
configuration are delegated to dedicated modules to improve maintainability.
"""
from __future__ import annotations

import collections
import datetime as dt
import os
import random
import re
import sys
from typing import Optional
from argparse import Namespace
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
import urllib.robotparser as robotparser

from .config import (
    DOMAIN,
    PERFUME_URL_RE,
    DEFAULT_UAS,
    DEFAULT_ACCEPT_LANGS,
)
from .network import (
    build_session,
    can_fetch,
    normalize_url,
    extract_links,
    polite_sleep,
    session_sleep,
    backoff_sleep,
)
from .parsing import parse_brand_name_from_url, scrape_perfume_page
from .storage import ensure_csv_with_header, load_existing_urls, append_row

def _normalize_brand_compare(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    return re.sub(r"\s+", " ", s or "").strip().casefold()


def _brand_to_designers_slug(brand: str) -> str:
    # Convert brand to something like 'Eight-and-Bob' or 'Chanel'
    s = brand.strip()
    s = re.sub(r"[’'`]+", "", s)
    s = s.replace("&", "and")
    # Replace non-alnum with hyphens
    s = re.sub(r"[^A-Za-z0-9]+", "-", s).strip("-")
    return s


def _brand_to_perfume_slug(brand: str) -> str:
    # Convert to slug used in /perfume/<Brand>/<Name>-<id>.html
    words = re.findall(r"[A-Za-z0-9]+", brand)
    return "-".join(words)


def crawl(args: Namespace) -> int:
    # Determine brand (company) if provided
    brand_input = (args.brand or "").strip()
    if not brand_input and sys.stdin and sys.stdin.isatty():
        try:
            brand_input = input("Enter company/brand name to scrape (leave empty to crawl freely): ").strip()
        except EOFError:
            brand_input = ""

    brand_filter_cmp = _normalize_brand_compare(brand_input) if brand_input else None

    # Derive output CSV path: if brand given and default out_csv was not overridden
    out_csv = args.out_csv
    default_out = (out_csv == "perfumes.csv")
    if brand_filter_cmp and default_out:
        # Build a nicer file name, prefer Saved Data/ folder if it exists
        safe_name = re.sub(r"[^A-Za-z0-9]+", "_", brand_input).strip("_") or "brand"
        saved_dir = os.path.join(os.getcwd(), "Saved Data")
        if os.path.isdir(saved_dir):
            out_csv = os.path.join(saved_dir, f"{safe_name}.csv")
        else:
            out_csv = f"{safe_name}.csv"

    ensure_csv_with_header(out_csv)
    # Also mirror CSV to the Desktop data folder as requested
    mirror_dir = "/Users/jakubjan/Desktop/PROJEKTY/PERFUMY APLIKACJA/Data"
    mirror_csv = os.path.join(mirror_dir, os.path.basename(out_csv))
    ensure_csv_with_header(mirror_csv)

    existing_urls = load_existing_urls(out_csv)

    # Build proxy list
    proxies = []
    if getattr(args, "proxy", None):
        proxies.append(args.proxy.strip())
    if getattr(args, "proxies_file", None):
        try:
            with open(args.proxies_file, "r", encoding="utf-8") as pf:
                for line in pf:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    proxies.append(line)
        except FileNotFoundError:
            print(f"[warn] Proxies file not found: {args.proxies_file}")
        except Exception as e:
            print(f"[warn] Could not read proxies file {args.proxies_file}: {e}")
    proxy_index = -1

    # Current identity state
    current_accept_language = None
    ua = args.user_agent

    session = None

    def rotate_identity(per_session: bool = False):
        nonlocal proxy_index, session, ua, current_accept_language
        # Advance proxy if any
        next_proxy = None
        if proxies:
            proxy_index = (proxy_index + 1) % len(proxies)
            next_proxy = proxies[proxy_index]
        # Choose UA: if user used placeholder default, rotate; else keep provided UA
        if args.user_agent == "Mozilla/5.0 (compatible; PerfumeBot/1.0; +https://example.com/botinfo)":
            ua = random.choice(DEFAULT_UAS)
        else:
            ua = args.user_agent
        # Rotate Accept-Language from a small pool
        current_accept_language = random.choice(DEFAULT_ACCEPT_LANGS)
        session = build_session(ua, args.timeout, proxy=next_proxy, accept_language=current_accept_language)
        if per_session:
            print("[identity] New session identity:",
                  f"proxy={'<none>' if not next_proxy else next_proxy}",
                  f"UA={ua}",
                  f"Accept-Language={current_accept_language}")

    rp = robotparser.RobotFileParser()
    rp.set_url(f"https://{DOMAIN}/robots.txt")
    try:
        rp.read()
    except Exception:
        print("[warn] Could not read robots.txt; exiting for safety.", file=sys.stderr)
        sys.exit(1)

    queue = collections.deque()
    seen = set()

    # Seed URLs
    seeds = list(args.start_url or [])
    if not seeds and brand_filter_cmp:
        designers_slug = _brand_to_designers_slug(brand_input)
        seeds.append(f"https://{DOMAIN}/designers/{designers_slug}.html")

    for su in seeds:
        su = normalize_url(su)
        if not su:
            continue
        if urlparse(su).netloc != DOMAIN:
            print(f"[skip] Out-of-domain seed: {su}")
            continue
        queue.append(su)
        seen.add(su)

    if not queue:
        print("[error] No seed URL provided and brand not specified; nothing to crawl.", file=sys.stderr)
        sys.exit(2)

    # Initialize identity (proxy, UA, Accept-Language)
    rotate_identity(per_session=True)

    pages_processed = 0
    pages_since_rotate = 0

    # Precompute expected brand slug for URL filtering
    expected_brand_slug = _brand_to_perfume_slug(brand_input).casefold() if brand_filter_cmp else None

    while queue and (args.max_pages <= 0 or pages_processed < args.max_pages):
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

        processed_incremented = False
        # If this is a perfume page, parse & store
        if PERFUME_URL_RE.match(url):
            data = scrape_perfume_page(url, soup)
            # Apply brand filter if provided
            if brand_filter_cmp:
                page_brand_cmp = _normalize_brand_compare(data.get("brand"))
                if not page_brand_cmp and url:
                    u_brand, _ = parse_brand_name_from_url(url)
                    page_brand_cmp = _normalize_brand_compare(u_brand)
                if page_brand_cmp != brand_filter_cmp:
                    # Not the requested brand; skip saving and counting
                    pass
                else:
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
                            append_row(mirror_csv, row)
                            existing_urls.add(url)
                            print(f"[saved] {data['brand']} — {data['name']} | {data['rating']} (votes: {data['votes']})")
                    else:
                        print(f"[note] Missing fields (brand/name/rating/votes) for {url}")
                    pages_processed += 1
                    processed_incremented = True
            else:
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
                        append_row(mirror_csv, row)
                        existing_urls.add(url)
                        print(f"[saved] {data['brand']} — {data['name']} | {data['rating']} (votes: {data['votes']})")
                else:
                    print(f"[note] Missing fields (brand/name/rating/votes) for {url}")
                pages_processed += 1
                processed_incremented = True

        # Rotate identity after N pages if requested
        if processed_incremented:
            pages_since_rotate += 1
            if getattr(args, "rotate_every", 0) > 0 and pages_since_rotate >= args.rotate_every:
                print(f"[rotate] Switching proxy/UA after {pages_since_rotate} processed pages")
                pages_since_rotate = 0
                rotate_identity()

        # Take a longer break after each session_size perfume pages
        if processed_incremented and args.session_size > 0 and pages_processed % args.session_size == 0:
            # If we still have work to do (queue not empty and budget not exhausted), pause
            if queue and (args.max_pages <= 0 or pages_processed < args.max_pages):
                mins = int(args.session_break_seconds // 60)
                secs = int(args.session_break_seconds % 60)
                print(f"[pause] Session limit reached ({pages_processed} pages). Cooling down for ~{mins}m{secs}s…")
                session_sleep(args.session_break_seconds, jitter_ratio=0.15)
                # Switch identity for next session
                rotate_identity(per_session=True)

        # Enqueue more links to continue crawling (only if we still have budget)
        if (args.max_pages <= 0 or pages_processed < args.max_pages):
            for link in extract_links(url, soup):
                if link in seen:
                    continue
                if brand_filter_cmp and expected_brand_slug:
                    # Only traverse perfume pages for this brand
                    u_brand, _ = parse_brand_name_from_url(link)
                    if (u_brand or ""):
                        if _brand_to_perfume_slug(u_brand).casefold() != expected_brand_slug:
                            continue
                seen.add(link)
                queue.append(link)

        polite_sleep(args.delay_seconds, args.delay_seconds + 1.5)

    print(f"\nDone. Pages processed (perfume pages saved/attempted): {pages_processed}")
    print(f"CSV path: {out_csv}")
    return pages_processed

__all__ = ["crawl"]

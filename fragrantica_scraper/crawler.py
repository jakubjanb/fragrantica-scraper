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
import time
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


def _remove_accents(text: str) -> str:
    """Remove accents/diacritics from Unicode text.

    Examples:
        'Hermès' -> 'Hermes'
        'Frédéric' -> 'Frederic'
        'Chloé' -> 'Chloe'
    """
    import unicodedata
    # Normalize to NFD (decomposed form) then filter out combining marks
    nfd = unicodedata.normalize('NFD', text)
    return ''.join(char for char in nfd if unicodedata.category(char) != 'Mn')


def _brand_to_designers_slug(brand: str) -> str:
    # Convert brand to something like 'Eight-and-Bob' or 'Chanel'
    s = brand.strip()
    # Remove accents first (Hermès -> Hermes)
    s = _remove_accents(s)
    s = re.sub(r"[''`]+", "", s)
    s = s.replace("&", "and")
    # Replace non-alnum with hyphens
    s = re.sub(r"[^A-Za-z0-9]+", "-", s).strip("-")
    return s


def _brand_to_perfume_slug(brand: str) -> str:
    # Convert to slug used in /perfume/<Brand>/<Name>-<id>.html
    # Remove accents first (Hermès -> Hermes)
    brand = _remove_accents(brand)
    words = re.findall(r"[A-Za-z0-9]+", brand)
    return "-".join(words)


def _load_proxies(args: Namespace) -> list[str]:
    """Load proxies from file and/or command-line argument."""
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
            if proxies:
                print(f"[proxy] Loaded {len(proxies)} proxy(ies) from {args.proxies_file}")
        except FileNotFoundError:
            if args.proxies_file != "proxies.txt":  # Only warn if user explicitly specified a file
                print(f"[warn] Proxies file not found: {args.proxies_file}")
        except Exception as e:
            print(f"[warn] Could not read proxies file {args.proxies_file}: {e}")
    return proxies


def _scrape_brand_simple(
    args: Namespace,
    brand_input: str,
    out_csv: str,
    mirror_csv: str,
    existing_urls: set
) -> int:
    """
    Simplified brand scraping: fetch brand page once, extract all perfume URLs,
    then process them one by one with proper delays.
    """
    print(f"[brand] Starting simplified scrape for: {brand_input}")

    # Load proxies
    proxies = _load_proxies(args)
    proxy_index = -1
    proxy_failures = {}  # Track failures per proxy

    def get_next_proxy() -> Optional[str]:
        nonlocal proxy_index
        if not proxies:
            return None
        # Skip proxies that have failed too many times recently
        max_attempts = len(proxies) * 2  # Avoid infinite loop
        attempts = 0
        while attempts < max_attempts:
            proxy_index = (proxy_index + 1) % len(proxies)
            candidate = proxies[proxy_index]
            # Allow proxy if it has fewer than 3 recent failures
            if proxy_failures.get(candidate, 0) < 3:
                return candidate
            attempts += 1
        # If all proxies are bad, reset failures and try again
        print("[warn] All proxies have failures, resetting failure counts")
        proxy_failures.clear()
        proxy_index = (proxy_index + 1) % len(proxies)
        return proxies[proxy_index]

    # Build brand designer page URL
    designers_slug = _brand_to_designers_slug(brand_input)
    brand_url = f"https://{DOMAIN}/designers/{designers_slug}.html"

    # Create session with realistic headers
    ua = random.choice(DEFAULT_UAS) if args.user_agent == "Mozilla/5.0 (compatible; PerfumeBot/1.0; +https://example.com/botinfo)" else args.user_agent
    accept_lang = random.choice(DEFAULT_ACCEPT_LANGS)
    current_proxy = get_next_proxy()

    session = build_session(ua, args.timeout, proxy=current_proxy, accept_language=accept_lang)
    print(f"[identity] UA={ua[:50]}... Accept-Language={accept_lang} Proxy={'<none>' if not current_proxy else current_proxy}")

    # Fetch brand page
    print(f"[fetch] Brand page: {brand_url}")
    try:
        resp = session.get(brand_url, timeout=args.timeout)
        if resp.status_code != 200:
            print(f"[error] Failed to fetch brand page (status {resp.status_code})")
            return 0

        soup = BeautifulSoup(resp.text, "lxml")
        print(f"[ok] Brand page loaded successfully")
    except Exception as e:
        print(f"[error] Exception fetching brand page: {e}")
        return 0

    # Extract all perfume URLs from brand page
    # The brand slug should match the URL path for brand filtering
    expected_brand_slug = _brand_to_perfume_slug(brand_input).casefold()

    perfume_urls = []
    for a in soup.find_all("a", href=True):
        href = a["href"]

        # Convert relative to absolute
        if href.startswith("/perfume/"):
            href = f"https://{DOMAIN}{href}"

        # Check if this is a perfume URL
        if not PERFUME_URL_RE.match(href):
            continue

        # Filter by brand: the URL should contain /perfume/<BrandSlug>/
        # Extract brand from URL path
        url_parts = href.split("/perfume/")
        if len(url_parts) < 2:
            continue

        # Get the brand part from URL (between /perfume/ and next /)
        path_after_perfume = url_parts[1]
        brand_from_url = path_after_perfume.split("/")[0] if "/" in path_after_perfume else ""

        # Compare brand slugs (case-insensitive)
        if brand_from_url.casefold() != expected_brand_slug:
            continue

        full_url = normalize_url(href)
        if full_url and full_url not in existing_urls:
            perfume_urls.append(full_url)

    # Remove duplicates while preserving order
    seen_in_batch = set()
    unique_perfume_urls = []
    for url in perfume_urls:
        if url not in seen_in_batch:
            seen_in_batch.add(url)
            unique_perfume_urls.append(url)

    perfume_urls = unique_perfume_urls

    print(f"[found] {len(perfume_urls)} perfume URLs to scrape (excluding already saved)")

    if not perfume_urls:
        print("[done] No new perfumes to scrape")
        return 0

    # Apply max_pages limit
    if args.max_pages > 0:
        perfume_urls = perfume_urls[:args.max_pages]
        print(f"[limit] Processing first {len(perfume_urls)} perfumes (max_pages={args.max_pages})")

    # Process each perfume one by one
    saved_count = 0
    saved_since_break = int(getattr(args, "saved_since_break", 0) or 0)
    requests_since_rotate = 0
    failed_urls = []  # Track failed URLs for retry
    proxy_failures = {}  # Track failures per proxy to blacklist bad ones

    for idx, url in enumerate(perfume_urls, 1):
        print(f"[{idx}/{len(perfume_urls)}] Fetching: {url}")

        # Check if we need to rotate proxy (independent of session breaks)
        if args.rotate_every > 0 and requests_since_rotate >= args.rotate_every:
            print(f"[rotate] Switching proxy after {requests_since_rotate} requests")
            current_proxy = get_next_proxy()
            ua = random.choice(DEFAULT_UAS) if args.user_agent == "Mozilla/5.0 (compatible; PerfumeBot/1.0; +https://example.com/botinfo)" else args.user_agent
            accept_lang = random.choice(DEFAULT_ACCEPT_LANGS)
            session = build_session(ua, args.timeout, proxy=current_proxy, accept_language=accept_lang)
            print(f"[identity] New: UA={ua[:50]}... Accept-Language={accept_lang} Proxy={'<none>' if not current_proxy else current_proxy}")
            requests_since_rotate = 0

        # Delay before each request (except first)
        if idx > 1:
            delay = random.uniform(args.delay_seconds, args.delay_seconds + 2.0)
            print(f"[wait] {delay:.1f}s")
            time.sleep(delay)

        # Fetch perfume page with retry logic
        max_retries = 3
        success = False
        soup = None

        for attempt in range(1, max_retries + 1):
            try:
                resp = session.get(url, timeout=args.timeout)

                # Handle 429 or 403: force immediate proxy rotation
                if resp.status_code in (429, 403):
                    print(f"[{resp.status_code}] Rate limited/blocked, forcing proxy rotation (attempt {attempt}/{max_retries})")
                    if attempt < max_retries and proxies:
                        # Mark current proxy as problematic
                        if current_proxy:
                            proxy_failures[current_proxy] = proxy_failures.get(current_proxy, 0) + 1
                        # Force proxy rotation
                        current_proxy = get_next_proxy()
                        ua = random.choice(DEFAULT_UAS) if args.user_agent == "Mozilla/5.0 (compatible; PerfumeBot/1.0; +https://example.com/botinfo)" else args.user_agent
                        accept_lang = random.choice(DEFAULT_ACCEPT_LANGS)
                        session = build_session(ua, args.timeout, proxy=current_proxy, accept_language=accept_lang)
                        print(f"[identity] Rotated: UA={ua[:50]}... Proxy={'<none>' if not current_proxy else current_proxy}")
                        # Reset rotation counter
                        requests_since_rotate = 0
                        # Shorter backoff for rate limiting (5s, 10s, 20s)
                        wait_time = 5 * (2 ** (attempt - 1))
                        print(f"[backoff] Waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                        continue
                    elif attempt < max_retries:
                        # No proxies available, just backoff
                        wait_time = 30 * (2 ** (attempt - 1))
                        print(f"[backoff] Waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                        continue
                    else:
                        print(f"[skip] Giving up after {max_retries} retries")
                        break

                if resp.status_code != 200:
                    print(f"[skip] Status {resp.status_code}")
                    break

                # Success - clear proxy failure counter if using proxy
                if current_proxy and current_proxy in proxy_failures:
                    proxy_failures[current_proxy] = max(0, proxy_failures[current_proxy] - 1)

                soup = BeautifulSoup(resp.text, "lxml")
                success = True
                break

            except requests.exceptions.ProxyError as e:
                # Proxy connection failed (502, tunnel errors, etc.)
                print(f"[error] Proxy error (attempt {attempt}/{max_retries}): {e}")
                if current_proxy:
                    proxy_failures[current_proxy] = proxy_failures.get(current_proxy, 0) + 1
                    print(f"[proxy] Marked {current_proxy} as problematic ({proxy_failures[current_proxy]} failures)")

                # Immediately rotate proxy on proxy errors, don't retry with same proxy
                if attempt < max_retries and proxies:
                    current_proxy = get_next_proxy()
                    ua = random.choice(DEFAULT_UAS) if args.user_agent == "Mozilla/5.0 (compatible; PerfumeBot/1.0; +https://example.com/botinfo)" else args.user_agent
                    accept_lang = random.choice(DEFAULT_ACCEPT_LANGS)
                    session = build_session(ua, args.timeout, proxy=current_proxy, accept_language=accept_lang)
                    print(f"[identity] Rotated to: Proxy={'<none>' if not current_proxy else current_proxy}")
                    requests_since_rotate = 0
                    # Short delay before retry (2s, 4s, 8s)
                    time.sleep(2 * attempt)
                    continue
                break

            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                # Connection timeout or failure - likely proxy issue
                print(f"[error] Connection error (attempt {attempt}/{max_retries}): {e}")
                if current_proxy:
                    proxy_failures[current_proxy] = proxy_failures.get(current_proxy, 0) + 1

                # Rotate proxy on connection errors
                if attempt < max_retries and proxies:
                    current_proxy = get_next_proxy()
                    session = build_session(ua, args.timeout, proxy=current_proxy, accept_language=accept_lang)
                    print(f"[identity] Rotated to: Proxy={'<none>' if not current_proxy else current_proxy}")
                    requests_since_rotate = 0
                    time.sleep(2 * attempt)
                    continue
                break

            except Exception as e:
                # Other errors - less aggressive rotation
                print(f"[error] Request failed (attempt {attempt}/{max_retries}): {e}")
                if attempt < max_retries:
                    time.sleep(5 * attempt)
                    continue
                break

        if not success or not soup:
            failed_urls.append(url)
            continue

        # Increment request counter after each successful or failed request
        requests_since_rotate += 1

        # Parse and save
        data = scrape_perfume_page(url, soup)

        # Check if we got redirected to a different perfume (ID mismatch)
        if url != resp.url:
            print(f"[redirect] {url} -> {resp.url}")
            # Update URL to the final one after redirect
            url = resp.url

        if data["brand"] and data["name"]:
            # Check if rating/votes are missing
            if data["rating"] is None or data["votes"] is None:
                print(f"[skip] {data['brand']} — {data['name']} | No ratings yet (new perfume)")
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
                saved_count += 1
                saved_since_break += 1
                print(f"[saved] {data['brand']} — {data['name']} | {data['rating']} (votes: {data['votes']})")
        else:
            print(f"[skip] Missing brand or name fields")

        # Session break after N saves
        if args.session_size > 0 and saved_since_break >= args.session_size:
            remaining = len(perfume_urls) - idx
            if remaining > 0:
                mins = int(args.session_break_seconds // 60)
                secs = int(args.session_break_seconds % 60)
                print(f"\n[pause] Session save limit reached ({args.session_size} fragrances).")
                print(f"[pause] {remaining} perfumes remaining. Cooling down for ~{mins}m{secs}s...\n")
                time.sleep(args.session_break_seconds)
                saved_since_break = 0

                # Rotate identity (including proxy)
                ua = random.choice(DEFAULT_UAS)
                accept_lang = random.choice(DEFAULT_ACCEPT_LANGS)
                current_proxy = get_next_proxy()
                session = build_session(ua, args.timeout, proxy=current_proxy, accept_language=accept_lang)
                print(f"[identity] New session: UA={ua[:50]}... Accept-Language={accept_lang} Proxy={'<none>' if not current_proxy else current_proxy}")

    # Retry failed URLs once
    if failed_urls:
        print(f"\n[retry] {len(failed_urls)} URLs failed. Retrying once...")
        for retry_idx, url in enumerate(failed_urls, 1):
            print(f"[retry {retry_idx}/{len(failed_urls)}] {url}")

            # Brief delay before retry
            time.sleep(random.uniform(args.delay_seconds, args.delay_seconds + 2.0))

            # Try once more with fresh session/proxy
            current_proxy = get_next_proxy()
            ua = random.choice(DEFAULT_UAS) if args.user_agent == "Mozilla/5.0 (compatible; PerfumeBot/1.0; +https://example.com/botinfo)" else args.user_agent
            accept_lang = random.choice(DEFAULT_ACCEPT_LANGS)
            session = build_session(ua, args.timeout, proxy=current_proxy, accept_language=accept_lang)

            try:
                resp = session.get(url, timeout=args.timeout)
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.text, "lxml")
                    data = scrape_perfume_page(url, soup)

                    if data["brand"] and data["name"] and data["rating"] is not None and data["votes"] is not None:
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
                        saved_count += 1
                        print(f"[saved] {data['brand']} — {data['name']} | {data['rating']} (votes: {data['votes']})")
                    else:
                        print(f"[skip] Missing data or no ratings")
                else:
                    print(f"[skip] Status {resp.status_code}")
            except Exception as e:
                print(f"[error] Retry failed: {e}")

    # Store counter for multi-brand runs
    args.saved_since_break_end = saved_since_break

    print(f"\n[done] Saved {saved_count} new perfumes for {brand_input}")
    if failed_urls:
        still_failed = len(failed_urls) - (saved_count - (len(perfume_urls) - len(failed_urls)))
        if still_failed > 0:
            print(f"[warn] {still_failed} URLs could not be saved after retry")
    print(f"[csv] {out_csv}")
    return saved_count


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
        # Remove accents first to ensure clean filenames (Hermès -> Hermes)
        safe_brand = _remove_accents(brand_input)
        safe_name = re.sub(r"[^A-Za-z0-9]+", "_", safe_brand).strip("_") or "brand"
        saved_dir = os.path.join(os.getcwd(), "Saved Data")
        if os.path.isdir(saved_dir):
            out_csv = os.path.join(saved_dir, f"{safe_name}.csv")
        else:
            out_csv = f"{safe_name}.csv"

    ensure_csv_with_header(out_csv)
    # Also mirror CSV to the Desktop data folder as requested
    mirror_dir = "/Users/jakubjborkala/Desktop/PROJEKTY/PERFUMY APLIKACJA/Data"
    mirror_csv = os.path.join(mirror_dir, os.path.basename(out_csv))
    ensure_csv_with_header(mirror_csv)

    existing_urls = load_existing_urls(out_csv)

    # NEW SIMPLIFIED APPROACH: Direct brand scraping
    if brand_input:
        return _scrape_brand_simple(args, brand_input, out_csv, mirror_csv, existing_urls)

    # Build proxy list
    proxies = _load_proxies(args)
    proxy_index = -1
    proxy_failures = {}  # Track failures per proxy

    def get_next_proxy() -> Optional[str]:
        nonlocal proxy_index
        if not proxies:
            return None
        # Skip proxies that have failed too many times recently
        max_attempts = len(proxies) * 2  # Avoid infinite loop
        attempts = 0
        while attempts < max_attempts:
            proxy_index = (proxy_index + 1) % len(proxies)
            candidate = proxies[proxy_index]
            # Allow proxy if it has fewer than 3 recent failures
            if proxy_failures.get(candidate, 0) < 3:
                return candidate
            attempts += 1
        # If all proxies are bad, reset failures and try again
        print("[warn] All proxies have failures, resetting failure counts")
        proxy_failures.clear()
        proxy_index = (proxy_index + 1) % len(proxies)
        return proxies[proxy_index]

    # Current identity state
    current_accept_language = None
    ua = args.user_agent
    current_proxy = None

    session = None

    def rotate_identity(per_session: bool = False):
        nonlocal session, ua, current_accept_language, current_proxy
        # Advance proxy if any
        current_proxy = get_next_proxy()
        # Choose UA: if user used placeholder default, rotate; else keep provided UA
        if args.user_agent == "Mozilla/5.0 (compatible; PerfumeBot/1.0; +https://example.com/botinfo)":
            ua = random.choice(DEFAULT_UAS)
        else:
            ua = args.user_agent
        # Rotate Accept-Language from a small pool
        current_accept_language = random.choice(DEFAULT_ACCEPT_LANGS)
        session = build_session(ua, args.timeout, proxy=current_proxy, accept_language=current_accept_language)
        if per_session:
            print("[identity] New session identity:",
                  f"proxy={'<none>' if not current_proxy else current_proxy}",
                  f"UA={ua[:50]}...",
                  f"Accept-Language={current_accept_language}")

    rp = robotparser.RobotFileParser()
    rp.set_url(f"https://{DOMAIN}/robots.txt")
    
    # Manually fetch robots.txt with timeout to avoid hanging
    # The standard rp.read() has no timeout and can block indefinitely
    robots_url = f"https://{DOMAIN}/robots.txt"
    try:
        # Create a temporary session just for robots.txt fetch
        temp_session = requests.Session()
        temp_session.headers.update({"User-Agent": args.user_agent})
        robots_resp = temp_session.get(robots_url, timeout=10.0)
        if robots_resp.status_code == 200:
            # Parse the robots.txt content
            rp.parse(robots_resp.text.splitlines())
        else:
            print(f"[warn] Could not fetch robots.txt (status {robots_resp.status_code}); proceeding with caution.", file=sys.stderr)
    except Exception as e:
        print(f"[warn] Could not read robots.txt ({e}); proceeding with caution.", file=sys.stderr)

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

    # Small initial delay to simulate natural browsing
    polite_sleep(1.0, 3.0)

    pages_processed = 0
    requests_since_rotate = 0  # Track requests for proxy rotation (independent of session breaks)

    # Carry "saved since last long break" across crawl() invocations (e.g., multi-brand mode).
    # This lets the cooldown happen exactly when the global save counter reaches session_size,
    # even if it happens mid-way through the next brand.
    saved_since_break = int(getattr(args, "saved_since_break", 0) or 0)

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

        # Check if we need to rotate proxy (independent of session breaks)
        if args.rotate_every > 0 and requests_since_rotate >= args.rotate_every:
            print(f"[rotate] Switching proxy after {requests_since_rotate} requests")
            rotate_identity()
            requests_since_rotate = 0

        # Retry loop for transient errors and rate limiting
        MAX_RETRIES = 3
        RETRY_STATUSES = {500, 502, 503, 504}
        attempt = 1
        success = False
        soup = None
        while True:
            try:
                # Add referer header for more realistic requests
                headers = {}
                if url.startswith(f"https://{DOMAIN}/perfume/"):
                    headers["Referer"] = f"https://{DOMAIN}/"
                elif url.startswith(f"https://{DOMAIN}/designers/"):
                    # Designer pages should look like direct navigation
                    headers["Sec-Fetch-Site"] = "none"
                    headers["Sec-Fetch-Mode"] = "navigate"
                else:
                    headers["Referer"] = f"https://{DOMAIN}/"
                    headers["Sec-Fetch-Site"] = "same-origin"

                resp = session.get(url, timeout=args.timeout, headers=headers, allow_redirects=True)
            except requests.exceptions.ProxyError as e:
                # Proxy connection failed (502, tunnel errors, etc.)
                print(f"[error] Proxy error (attempt {attempt}/{MAX_RETRIES}): {e}")
                if current_proxy:
                    proxy_failures[current_proxy] = proxy_failures.get(current_proxy, 0) + 1
                    print(f"[proxy] Marked {current_proxy} as problematic ({proxy_failures[current_proxy]} failures)")

                # Immediately rotate proxy on proxy errors
                if attempt < MAX_RETRIES and proxies:
                    rotate_identity()
                    requests_since_rotate = 0
                    # Short delay before retry (2s, 4s, 8s)
                    time.sleep(2 * attempt)
                    attempt += 1
                    continue
                print(f"[error] Request failed (give up): {url} ({e})")
                break

            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                # Connection timeout or failure - likely proxy issue
                print(f"[error] Connection error (attempt {attempt}/{MAX_RETRIES}): {e}")
                if current_proxy:
                    proxy_failures[current_proxy] = proxy_failures.get(current_proxy, 0) + 1

                # Rotate proxy on connection errors
                if attempt < MAX_RETRIES and proxies:
                    rotate_identity()
                    requests_since_rotate = 0
                    time.sleep(2 * attempt)
                    attempt += 1
                    continue
                print(f"[error] Request failed (give up): {url} ({e})")
                break

            except requests.RequestException as e:
                # Other request exceptions - less aggressive rotation
                if attempt >= MAX_RETRIES:
                    print(f"[error] Request failed (give up): {url} ({e})")
                    break
                print(f"[warn] Request exception, retrying {attempt}/{MAX_RETRIES}: {url} ({e})")
                backoff_sleep(resp=None, base_delay=args.delay_seconds, attempt=attempt)
                attempt += 1
                continue

            content_type = resp.headers.get("Content-Type", "")

            # Handle 429/403 with immediate proxy rotation
            if resp.status_code in (429, 403):
                if attempt < MAX_RETRIES:
                    print(f"[{resp.status_code}] Rate limited/blocked, forcing proxy rotation (attempt {attempt}/{MAX_RETRIES}): {url}")
                    if current_proxy:
                        proxy_failures[current_proxy] = proxy_failures.get(current_proxy, 0) + 1
                    if proxies:
                        # Force proxy rotation and reset counter
                        rotate_identity()
                        requests_since_rotate = 0
                        print(f"[identity] Rotated to next proxy")
                    # Shorter backoff (5s, 10s, 20s)
                    time.sleep(5 * (2 ** (attempt - 1)))
                    attempt += 1
                    continue
                print(f"[skip] Status {resp.status_code} after {MAX_RETRIES} retries: {url}")
                polite_sleep(args.delay_seconds, args.delay_seconds + 1.0)
                break

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

            # Success - clear proxy failure counter if using proxy
            if current_proxy and current_proxy in proxy_failures:
                proxy_failures[current_proxy] = max(0, proxy_failures[current_proxy] - 1)

            soup = BeautifulSoup(resp.text, "lxml")
            success = True
            break

        if not success:
            # Could not fetch this URL successfully; move on
            continue

        # Increment request counter after each request (successful or not)
        requests_since_rotate += 1

        processed_incremented = False
        saved_incremented = False
        # If this is a perfume page, parse & store
        if PERFUME_URL_RE.match(url):
            # Check for redirects
            final_url = resp.url
            if url != final_url:
                print(f"[redirect] {url} -> {final_url}")
                url = final_url

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
                    if data["brand"] and data["name"]:
                        if data["rating"] is None or data["votes"] is None:
                            print(f"[skip] {data['brand']} — {data['name']} | No ratings yet")
                        elif url in existing_urls:
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
                            saved_incremented = True
                            print(f"[saved] {data['brand']} — {data['name']} | {data['rating']} (votes: {data['votes']})")
                    else:
                        print(f"[note] Missing brand or name for {url}")
                    pages_processed += 1
                    processed_incremented = True
            else:
                if data["brand"] and data["name"]:
                    if data["rating"] is None or data["votes"] is None:
                        print(f"[skip] {data['brand']} — {data['name']} | No ratings yet")
                    elif url in existing_urls:
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
                        saved_incremented = True
                        print(f"[saved] {data['brand']} — {data['name']} | {data['rating']} (votes: {data['votes']})")
                else:
                    print(f"[note] Missing brand or name for {url}")
                pages_processed += 1
                processed_incremented = True

        # Track how many *new* fragrances were saved since the last long break.
        if saved_incremented:
            saved_since_break += 1

        # Take a longer break after each session_size *saved* perfume pages.
        # This uses the carried counter so multi-brand runs cool down globally, not per brand.
        if saved_incremented and args.session_size > 0 and saved_since_break >= args.session_size:
            # If we still have work to do (queue not empty and budget not exhausted), pause
            if queue and (args.max_pages <= 0 or pages_processed < args.max_pages):
                mins = int(args.session_break_seconds // 60)
                secs = int(args.session_break_seconds % 60)
                print(
                    f"[pause] Session save limit reached ({args.session_size} new fragrances). "
                    f"Cooling down for ~{mins}m{secs}s…"
                )
                session_sleep(args.session_break_seconds, jitter_ratio=0.15)
                saved_since_break = 0
                # Switch identity for next session
                rotate_identity(per_session=True)

        # Enqueue more links to continue crawling (only if we still have budget)
        if (args.max_pages <= 0 or pages_processed < args.max_pages):
            # Limit perfume links from designer pages to avoid overwhelming the queue
            # This prevents 100+ URLs being added at once, which causes rapid rate limiting
            is_designer_page = url.startswith(f"https://{DOMAIN}/designers/")
            link_limit = 20 if is_designer_page else 0  # Limit to 20 perfume links per designer page
            
            for link in extract_links(url, soup, limit_perfume_links=link_limit):
                if link in seen:
                    continue
                if brand_filter_cmp and expected_brand_slug:
                    # Allow designer pages for navigation, but filter perfume pages by brand
                    if PERFUME_URL_RE.match(link):
                        # Only traverse perfume pages for this brand
                        u_brand, _ = parse_brand_name_from_url(link)
                        if (u_brand or ""):
                            if _brand_to_perfume_slug(u_brand).casefold() != expected_brand_slug:
                                continue
                    # Designer pages are always allowed for navigation
                seen.add(link)
                queue.append(link)

        # Use longer delay after designer pages (they contain many links)
        if url.startswith(f"https://{DOMAIN}/designers/"):
            polite_sleep(args.delay_seconds * 1.5, args.delay_seconds * 2.0)
        else:
            polite_sleep(args.delay_seconds, args.delay_seconds + 1.5)

    # Expose session counter for callers (e.g., main multi-brand loop).
    args.saved_since_break_end = saved_since_break

    print(f"\nDone. Pages processed (perfume pages saved/attempted): {pages_processed}")
    print(f"CSV path: {out_csv}")
    return pages_processed

__all__ = ["crawl"]

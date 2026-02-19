#!/usr/bin/env python3
"""
Enrichment crawler: adds fragrance_category and sex columns to an existing CSV.

Reads each row from the CSV, visits the fragrance URL, parses the opening
description (e.g. "is a Woody Chypre fragrance for women and men."), and
writes the extracted data back.  Uses the same proxy rotation, session breaks,
and retry logic as the main crawler.
"""
from __future__ import annotations

import argparse
import csv
import os
import random
import shutil
import sys
import tempfile
import time
from typing import Optional

import requests
from bs4 import BeautifulSoup

from fragrantica_scraper.config import DEFAULT_UAS, DEFAULT_ACCEPT_LANGS
from fragrantica_scraper.network import build_session, polite_sleep, session_sleep
from fragrantica_scraper.parsing import parse_category_and_sex

# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

EXPECTED_FIELDS = ["brand", "name", "rating", "votes", "url", "last_crawled", "sex", "fragrance_category"]


def _read_csv(path: str) -> list[dict]:
    """Read CSV into a list of dicts, adding missing columns."""
    rows: list[dict] = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Ensure new columns exist
            row.setdefault("fragrance_category", "")
            row.setdefault("sex", "")
            rows.append(row)
    return rows


def _write_csv(path: str, rows: list[dict]) -> None:
    """Atomically write rows back to CSV (write to temp then rename)."""
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    fd, tmp_path = tempfile.mkstemp(suffix=".csv", dir=os.path.dirname(path) or ".")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        shutil.move(tmp_path, path)
    except Exception:
        # Clean up temp file on failure
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


# ---------------------------------------------------------------------------
# Proxy helpers  (mirrors crawler.py logic)
# ---------------------------------------------------------------------------

def _load_proxies(args: argparse.Namespace) -> list[str]:
    proxies: list[str] = []
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
            if args.proxies_file != "proxies.txt":
                print(f"[warn] Proxies file not found: {args.proxies_file}")
        except Exception as e:
            print(f"[warn] Could not read proxies file {args.proxies_file}: {e}")
    return proxies


# ---------------------------------------------------------------------------
# Main enrichment loop
# ---------------------------------------------------------------------------

def enrich(args: argparse.Namespace) -> int:
    csv_path = args.csv
    if not os.path.isfile(csv_path):
        print(f"[error] CSV not found: {csv_path}", file=sys.stderr)
        return 1

    rows = _read_csv(csv_path)
    print(f"[csv] Loaded {len(rows)} rows from {csv_path}")

    # Find rows that need enrichment
    to_enrich: list[int] = []
    for i, row in enumerate(rows):
        needs_category = not row.get("fragrance_category", "").strip()
        needs_sex = not row.get("sex", "").strip()
        if needs_category or needs_sex:
            to_enrich.append(i)

    print(f"[enrich] {len(to_enrich)} rows need enrichment (out of {len(rows)} total)")

    if not to_enrich:
        print("[done] Nothing to enrich.")
        return 0

    # Skip rows (resume from a specific position in the work queue)
    if args.skip_rows > 0:
        skipped = min(args.skip_rows, len(to_enrich))
        to_enrich = to_enrich[skipped:]
        print(f"[skip] Skipping first {skipped} pending rows (--skip-rows={args.skip_rows}), {len(to_enrich)} remaining")

    # Apply limit
    if args.max_pages > 0:
        to_enrich = to_enrich[: args.max_pages]
        print(f"[limit] Processing first {len(to_enrich)} rows (--max-pages={args.max_pages})")

    # Proxy setup
    proxies = _load_proxies(args)
    proxy_index = -1
    proxy_failures: dict[str, int] = {}

    def get_next_proxy() -> Optional[str]:
        nonlocal proxy_index
        if not proxies:
            return None
        max_attempts = len(proxies) * 2
        attempts = 0
        while attempts < max_attempts:
            proxy_index = (proxy_index + 1) % len(proxies)
            candidate = proxies[proxy_index]
            if proxy_failures.get(candidate, 0) < 3:
                return candidate
            attempts += 1
        print("[warn] All proxies have failures, resetting failure counts")
        proxy_failures.clear()
        proxy_index = (proxy_index + 1) % len(proxies)
        return proxies[proxy_index]

    # Build initial session
    is_default_ua = args.user_agent == "Mozilla/5.0 (compatible; PerfumeBot/1.0; +https://example.com/botinfo)"
    ua = random.choice(DEFAULT_UAS) if is_default_ua else args.user_agent
    accept_lang = random.choice(DEFAULT_ACCEPT_LANGS)
    current_proxy = get_next_proxy()
    session = build_session(ua, args.timeout, proxy=current_proxy, accept_language=accept_lang)
    print(f"[identity] UA={ua[:50]}... Proxy={'<none>' if not current_proxy else current_proxy}")

    enriched_count = 0
    enriched_since_break = 0
    requests_since_rotate = 0
    save_every = 10  # flush CSV every N successful enrichments

    for progress_idx, row_idx in enumerate(to_enrich, 1):
        row = rows[row_idx]
        url = row.get("url", "").strip()
        if not url:
            print(f"[{progress_idx}/{len(to_enrich)}] [skip] No URL in row")
            continue

        print(f"[{progress_idx}/{len(to_enrich)}] Fetching: {url}")

        # Proxy rotation check
        if args.rotate_every > 0 and requests_since_rotate >= args.rotate_every:
            print(f"[rotate] Switching proxy after {requests_since_rotate} requests")
            current_proxy = get_next_proxy()
            ua = random.choice(DEFAULT_UAS) if is_default_ua else args.user_agent
            accept_lang = random.choice(DEFAULT_ACCEPT_LANGS)
            session = build_session(ua, args.timeout, proxy=current_proxy, accept_language=accept_lang)
            print(f"[identity] New: UA={ua[:50]}... Proxy={'<none>' if not current_proxy else current_proxy}")
            requests_since_rotate = 0

        # Delay before each request (except first)
        if progress_idx > 1:
            delay = random.uniform(args.delay_seconds, args.delay_seconds + 2.0)
            print(f"[wait] {delay:.1f}s")
            time.sleep(delay)

        # Fetch with retries
        max_retries = 3
        success = False
        soup = None

        for attempt in range(1, max_retries + 1):
            try:
                resp = session.get(url, timeout=args.timeout)

                if resp.status_code in (429, 403):
                    print(f"[{resp.status_code}] Rate limited/blocked (attempt {attempt}/{max_retries})")
                    if current_proxy:
                        proxy_failures[current_proxy] = proxy_failures.get(current_proxy, 0) + 1
                    if attempt < max_retries and proxies:
                        current_proxy = get_next_proxy()
                        ua = random.choice(DEFAULT_UAS) if is_default_ua else args.user_agent
                        accept_lang = random.choice(DEFAULT_ACCEPT_LANGS)
                        session = build_session(ua, args.timeout, proxy=current_proxy, accept_language=accept_lang)
                        print(f"[identity] Rotated: Proxy={'<none>' if not current_proxy else current_proxy}")
                        requests_since_rotate = 0
                        wait_time = 5 * (2 ** (attempt - 1))
                        print(f"[backoff] Waiting {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    elif attempt < max_retries:
                        wait_time = 30 * (2 ** (attempt - 1))
                        print(f"[backoff] Waiting {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    else:
                        print(f"[skip] Giving up after {max_retries} retries")
                        break

                if resp.status_code != 200:
                    print(f"[skip] Status {resp.status_code}")
                    break

                if current_proxy and current_proxy in proxy_failures:
                    proxy_failures[current_proxy] = max(0, proxy_failures[current_proxy] - 1)

                soup = BeautifulSoup(resp.text, "lxml")
                success = True
                break

            except requests.exceptions.ProxyError as e:
                print(f"[error] Proxy error (attempt {attempt}/{max_retries}): {e}")
                if current_proxy:
                    proxy_failures[current_proxy] = proxy_failures.get(current_proxy, 0) + 1
                if attempt < max_retries and proxies:
                    current_proxy = get_next_proxy()
                    ua = random.choice(DEFAULT_UAS) if is_default_ua else args.user_agent
                    accept_lang = random.choice(DEFAULT_ACCEPT_LANGS)
                    session = build_session(ua, args.timeout, proxy=current_proxy, accept_language=accept_lang)
                    print(f"[identity] Rotated to: Proxy={'<none>' if not current_proxy else current_proxy}")
                    requests_since_rotate = 0
                    time.sleep(2 * attempt)
                    continue
                break

            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                print(f"[error] Connection error (attempt {attempt}/{max_retries}): {e}")
                if current_proxy:
                    proxy_failures[current_proxy] = proxy_failures.get(current_proxy, 0) + 1
                if attempt < max_retries and proxies:
                    current_proxy = get_next_proxy()
                    session = build_session(ua, args.timeout, proxy=current_proxy, accept_language=accept_lang)
                    print(f"[identity] Rotated to: Proxy={'<none>' if not current_proxy else current_proxy}")
                    requests_since_rotate = 0
                    time.sleep(2 * attempt)
                    continue
                break

            except Exception as e:
                print(f"[error] Request failed (attempt {attempt}/{max_retries}): {e}")
                if attempt < max_retries:
                    time.sleep(5 * attempt)
                    continue
                break

        if not success or not soup:
            continue

        requests_since_rotate += 1

        # Parse category and sex
        category, sex = parse_category_and_sex(soup)

        # Update row
        updated = False
        if category and not row.get("fragrance_category", "").strip():
            row["fragrance_category"] = category
            updated = True
        if sex and not row.get("sex", "").strip():
            row["sex"] = sex
            updated = True

        if updated:
            enriched_count += 1
            enriched_since_break += 1
            print(f"[enriched] {row.get('brand', '?')} — {row.get('name', '?')} | category={category or '?'} sex={sex or '?'}")
        else:
            # Check what's still missing vs what the parser returned
            still_needs = []
            if not row.get("fragrance_category", "").strip():
                still_needs.append("category")
            if not row.get("sex", "").strip():
                still_needs.append("sex")
            if still_needs:
                print(f"[warn] Could not extract: {', '.join(still_needs)} | parser got category={category or '?'} sex={sex or '?'}")
            else:
                print(f"[ok] Already complete: category={row.get('fragrance_category', '')}, sex={row.get('sex', '')}")

        # Periodic save
        if enriched_count > 0 and enriched_count % save_every == 0:
            print(f"[save] Writing CSV ({enriched_count} enriched so far)...")
            _write_csv(csv_path, rows)

        # Session break
        if args.session_size > 0 and enriched_since_break >= args.session_size:
            remaining = len(to_enrich) - progress_idx
            if remaining > 0:
                mins = int(args.session_break_seconds // 60)
                secs = int(args.session_break_seconds % 60)
                print(f"\n[pause] Session limit reached ({args.session_size}). "
                      f"{remaining} remaining. Cooling down for ~{mins}m{secs}s...\n")
                session_sleep(args.session_break_seconds, jitter_ratio=0.15)
                enriched_since_break = 0
                # Rotate identity
                ua = random.choice(DEFAULT_UAS) if is_default_ua else args.user_agent
                accept_lang = random.choice(DEFAULT_ACCEPT_LANGS)
                current_proxy = get_next_proxy()
                session = build_session(ua, args.timeout, proxy=current_proxy, accept_language=accept_lang)
                print(f"[identity] New session: UA={ua[:50]}... Proxy={'<none>' if not current_proxy else current_proxy}")

    # Final save
    if enriched_count > 0:
        print(f"[save] Final CSV write ({enriched_count} enriched total)...")
        _write_csv(csv_path, rows)

    print(f"\n[done] Enriched {enriched_count} rows out of {len(to_enrich)} attempted")
    print(f"[csv] {csv_path}")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Enrich existing fragrance CSV with category and sex data."
    )
    parser.add_argument(
        "--csv",
        default=os.path.join("Saved Data", "all_brands_clean.csv"),
        help="Path to the CSV file to enrich.",
    )
    parser.add_argument(
        "--max-pages", type=int, default=0,
        help="Max rows to enrich. 0 or negative = unlimited.",
    )
    parser.add_argument(
        "--skip-rows", type=int, default=0,
        help="Skip the first N pending rows before starting (for resuming). "
             "Matches the progress counter shown in logs, e.g. --skip-rows 6683 starts at [6684/...].",
    )
    parser.add_argument(
        "--delay-seconds", type=float, default=5.0,
        help="Base politeness delay between requests.",
    )
    parser.add_argument("--timeout", type=float, default=20.0, help="HTTP timeout in seconds.")
    parser.add_argument(
        "--user-agent",
        default="Mozilla/5.0 (compatible; PerfumeBot/1.0; +https://example.com/botinfo)",
        help="User-Agent string.",
    )
    parser.add_argument(
        "--session-size", type=int, default=30,
        help="Rows enriched before cooldown break.",
    )
    parser.add_argument(
        "--session-break-seconds", type=float, default=900,
        help="Cooldown duration after each session (default 900s / 15 min).",
    )
    parser.add_argument("--proxy", help="Single proxy URL.")
    parser.add_argument(
        "--proxies-file", default="proxies.txt",
        help="File with one proxy per line.",
    )
    parser.add_argument(
        "--rotate-every", type=int, default=30,
        help="Rotate proxy after N requests.",
    )

    args = parser.parse_args()
    sys.exit(enrich(args))


if __name__ == "__main__":
    main()

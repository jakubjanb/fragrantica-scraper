"""CLI entry point for the Fragrantica scraper.

Keeps only argparse and the program entry point, delegating the actual crawl
logic to the fragrantica_scraper.crawler module.
"""
from __future__ import annotations

import argparse
import copy
import sys
from pathlib import Path

from fragrantica_scraper.crawler import crawl


def _read_brands_file(path: str) -> list[str]:
    brands: list[str] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        brands.append(line)
    return brands


def _dedupe_casefold_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        key = item.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Crawl Fragrantica perfume pages and store rating/votes in a CSV file."
    )
    parser.add_argument(
        "--start-url",
        action="append",
        required=False,
        help=(
            "Seed URL (can be specified multiple times). If omitted and --brand is provided, the scraper "
            "will start from the brand's designers page. Example seed: "
            "https://www.fragrantica.com/perfume/EIGHT-BOB/EIGHT-BOB-16295.html"
        ),
    )
    parser.add_argument(
        "--brand",
        help=(
            "Company/brand name to scrape (interactive prompt will ask if not provided). Only fragrances "
            "from this brand will be saved, and the output CSV will default to <brand>.csv."
        ),
    )
    parser.add_argument(
        "--brands",
        action="append",
        help="Brand name to scrape (repeatable). Runs brand-by-brand and writes one CSV per brand.",
    )
    parser.add_argument(
        "--brands-file",
        help=(
            "Path to a file with one brand per line. Blank lines and lines starting with # are ignored. "
            "Runs brand-by-brand and writes one CSV per brand."
        ),
    )
    parser.add_argument("--out-csv", default="perfumes.csv", help="Path to output CSV file.")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=100,
        help="Max perfume pages to save. Use 0 or a negative number for no limit.",
    )
    parser.add_argument(
        "--delay-seconds", type=float, default=5.0, help="Base politeness delay between requests."
    )
    parser.add_argument("--timeout", type=float, default=20.0, help="HTTP timeout in seconds.")
    parser.add_argument(
        "--user-agent",
        default="Mozilla/5.0 (compatible; PerfumeBot/1.0; +https://example.com/botinfo)",
        help="User-Agent string used for requests.",
    )
    parser.add_argument(
        "--session-size",
        type=int,
        default=30,
        help="Number of perfume pages to save before taking a longer cooldown break.",
    )
    parser.add_argument(
        "--session-break-seconds",
        type=float,
        default=900,
        help="Cooldown duration (in seconds) after each session. Default 900s (15 minutes).",
    )
    parser.add_argument(
        "--proxy",
        help="Proxy URL to use for requests (e.g., http://user:pass@host:port or socks5://user:pass@host:port).",
    )
    parser.add_argument(
        "--proxies-file",
        default="proxies.txt",
        help="Path to a file with one proxy per line (format: http://user:pass@host:port). Lines starting with # are ignored.",
    )
    parser.add_argument(
        "--rotate-every",
        type=int,
        default=30,
        help="Rotate proxy after N requests (independent of session breaks). Default: 30.",
    )

    # Provide a friendlier message if no arguments were supplied, instead of argparse error
    if len(sys.argv) == 1:
        msg = (
            "Missing required arguments: provide at least one of --start-url / --brand / --brands / --brands-file\n\n"
            "Usage examples:\n"
            "  python main.py --start-url https://www.fragrantica.com/perfume/EIGHT-BOB/EIGHT-BOB-16295.html --max-pages 10\n"
            "  python -m fragrantica_scraper --start-url https://www.fragrantica.com/perfume/Chanel/Chance-21.html --max-pages 10\n"
            "  python main.py --brands-file brands.txt --max-pages 10\n"
        )
        print(msg, file=sys.stderr)
        sys.exit(2)

    args = parser.parse_args()

    # Validate that we have at least one way to seed the crawl
    if not (args.start_url or args.brand or args.brands or args.brands_file):
        print(
            "[error] Provide at least one of --start-url / --brand / --brands / --brands-file",
            file=sys.stderr,
        )
        sys.exit(2)

    # Multi-brand mode
    brands: list[str] = []
    if args.brands:
        brands.extend([b.strip() for b in args.brands if b and b.strip()])
    if args.brands_file:
        try:
            brands.extend(_read_brands_file(args.brands_file))
        except FileNotFoundError:
            print(f"[error] Brands file not found: {args.brands_file}", file=sys.stderr)
            sys.exit(2)
        except Exception as e:
            print(f"[error] Could not read brands file {args.brands_file}: {e}", file=sys.stderr)
            sys.exit(2)

    brands = _dedupe_casefold_preserve_order(brands)

    if brands:
        # Keep behavior predictable: in multi-brand mode we seed from each brand's designers page.
        if args.brand:
            print("[error] Do not combine --brand with --brands/--brands-file", file=sys.stderr)
            sys.exit(2)
        if args.start_url:
            print("[error] Do not combine --start-url with --brands/--brands-file", file=sys.stderr)
            sys.exit(2)

        saved_since_break = 0

        for i, brand in enumerate(brands, 1):
            print(f"\n[multi] Brand {i}/{len(brands)}: {brand}")
            per_brand_args = copy.deepcopy(args)
            per_brand_args.brand = brand
            per_brand_args.start_url = None
            # Keep default so crawler auto-names to Saved Data/<brand>.csv
            per_brand_args.out_csv = "perfumes.csv"

            # Carry the "saved since last cooldown" counter across brands so session-size is global.
            per_brand_args.saved_since_break = saved_since_break

            crawl(per_brand_args)

            saved_since_break = int(getattr(per_brand_args, "saved_since_break_end", saved_since_break) or 0)
        return

    crawl(args)


if __name__ == "__main__":
    main()

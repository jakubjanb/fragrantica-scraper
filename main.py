"""CLI entry point for the Fragrantica scraper.

Keeps only argparse and the program entry point, delegating the actual crawl
logic to the fragrantica_scraper.crawler module.
"""
from __future__ import annotations

import argparse
import sys

from fragrantica_scraper.crawler import crawl


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
        help="Path to a file with one proxy per line. Lines starting with # are ignored.",
    )
    parser.add_argument(
        "--rotate-every",
        type=int,
        default=0,
        help="Rotate proxy/User-Agent/Accept-Language after N processed perfume pages (0 disables).",
    )

    # Provide a friendlier message if no arguments were supplied, instead of argparse error
    if len(sys.argv) == 1:
        msg = (
            "Missing required argument: --start-url\n\n"
            "Usage examples:\n"
            "  python main.py --start-url https://www.fragrantica.com/perfume/EIGHT-BOB/EIGHT-BOB-16295.html --max-pages 10\n"
            "  python -m fragrantica_scraper --start-url https://www.fragrantica.com/perfume/Chanel/Chance-21.html --max-pages 10\n"
        )
        print(msg, file=sys.stderr)
        sys.exit(2)

    args = parser.parse_args()
    crawl(args)


if __name__ == "__main__":
    main()

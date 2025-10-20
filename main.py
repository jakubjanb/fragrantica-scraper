"""Convenience entrypoint to run the Fragrantica scraper CLI.

Examples:
    python main.py --start-url https://www.fragrantica.com/perfume/EIGHT-BOB/EIGHT-BOB-16295.html --max-pages 10

Note: You can also run `python -m fragrantica_scraper.crawler` for the same effect.
"""
import sys
from fragrantica_scraper.crawler import main as cli_main

if __name__ == '__main__':
    # Provide a friendlier message if no arguments were supplied, instead of argparse error
    if len(sys.argv) == 1:
        msg = (
            "Missing required argument: --start-url\n\n"
            "Usage examples:\n"
            "  python main.py --start-url https://www.fragrantica.com/perfume/EIGHT-BOB/EIGHT-BOB-16295.html --max-pages 10\n"
            "  python -m fragrantica_scraper.crawler --start-url https://www.fragrantica.com/perfume/Chanel/Chance-21.html --max-pages 10\n"
        )
        print(msg, file=sys.stderr)
        sys.exit(2)
    cli_main()

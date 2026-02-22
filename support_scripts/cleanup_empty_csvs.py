#!/usr/bin/env python3
"""Delete brand CSVs in 'Saved Data/' that contain only the header row and no perfume data."""

import argparse
import csv
import sys
from pathlib import Path

SAVED_DATA = Path(__file__).parent / "Saved Data"
SKIP_FILES = {"all_brands_clean.csv", "list_of_brands.csv"}
EXPECTED_FIELDS = ["brand", "name", "rating", "votes", "url", "last_crawled", "sex", "fragrance_category"]


def find_empty(directory: Path) -> list[Path]:
    empty = []
    for csv_path in sorted(directory.glob("*.csv")):
        if csv_path.name in SKIP_FILES:
            continue
        try:
            with open(csv_path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                if list(reader.fieldnames or []) != EXPECTED_FIELDS:
                    continue  # unexpected format — leave it alone
                if sum(1 for _ in reader) == 0:
                    empty.append(csv_path)
        except Exception as e:
            print(f"Warning: could not read {csv_path.name}: {e}", file=sys.stderr)
    return empty


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-y", "--yes",
        action="store_true",
        help="Delete without asking for confirmation",
    )
    args = parser.parse_args()

    empty = find_empty(SAVED_DATA)

    if not empty:
        print("No empty brand CSVs found.")
        return

    print(f"Found {len(empty)} empty CSV(s):")
    for p in empty:
        print(f"  {p.name}")

    if not args.yes:
        try:
            answer = input("\nDelete these files? [y/N] ")
        except (KeyboardInterrupt, EOFError):
            print("\nAborted.")
            sys.exit(0)
        if answer.strip().lower() != "y":
            print("Aborted.")
            sys.exit(0)

    print()
    for p in empty:
        p.unlink()
        print(f"Deleted: {p.name}")
    print(f"\nDone. {len(empty)} file(s) deleted.")


if __name__ == "__main__":
    main()

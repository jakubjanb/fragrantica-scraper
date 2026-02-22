"""
join_fragrances.py

Merges all brand CSV files from "Saved Data/" into the master all_brands_clean.csv,
deduplicates by URL, and sorts alphabetically by brand then fragrance name.

Only files directly inside "Saved Data/" are included — subdirectories (e.g. Perfumerie/)
are ignored. Files whose columns do not exactly match the master schema are skipped with
a warning so no malformed data enters the master.

Usage:
    python support_scripts/join_fragrances.py
"""

import csv
import sys
from pathlib import Path

SAVED_DATA = Path(__file__).parent.parent / "Saved Data"
MASTER_CSV = SAVED_DATA / "all_brands_clean.csv"
COLUMNS = ["brand", "name", "rating", "votes", "url", "last_crawled", "sex", "fragrance_category"]


def check_columns(path: Path) -> bool:
    """Return True only if the file's header exactly matches COLUMNS."""
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            actual = list(reader.fieldnames or [])
    except Exception as e:
        print(f"  SKIP {path.name}: could not read header — {e}", file=sys.stderr)
        return False

    if actual != COLUMNS:
        print(
            f"  SKIP {path.name}: column mismatch\n"
            f"    expected: {COLUMNS}\n"
            f"    got:      {actual}",
            file=sys.stderr,
        )
        return False
    return True


def read_csv(path: Path) -> list[dict]:
    rows = []
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if not row.get("url", "").strip():
                    continue
                rows.append(row)
    except Exception as e:
        print(f"  Warning: could not read {path.name}: {e}", file=sys.stderr)
    return rows


def collect_brand_csvs() -> list[Path]:
    """Find CSV files directly inside Saved Data/, excluding the master and subdirectories."""
    return sorted(
        p for p in SAVED_DATA.glob("*.csv")
        if p.resolve() != MASTER_CSV.resolve()
    )


def main():
    # --- Load master ---
    print(f"Reading master: {MASTER_CSV.name}")
    master_rows = read_csv(MASTER_CSV)
    print(f"  {len(master_rows)} existing rows")

    seen_urls: dict[str, dict] = {row["url"]: row for row in master_rows}
    new_count = 0

    # --- Load brand files ---
    brand_csvs = collect_brand_csvs()
    print(f"\nFound {len(brand_csvs)} brand CSV file(s) to merge:")

    for csv_path in brand_csvs:
        if not check_columns(csv_path):
            continue
        rows = read_csv(csv_path)
        added = 0
        for row in rows:
            url = row["url"].strip()
            if url not in seen_urls:
                seen_urls[url] = row
                added += 1
        print(f"  {csv_path.name}: {len(rows)} rows, {added} new")
        new_count += added

    # --- Sort: brand (case-insensitive), then name ---
    merged = sorted(
        seen_urls.values(),
        key=lambda r: (r.get("brand", "").casefold(), r.get("name", "").casefold()),
    )

    # --- Write master ---
    with open(MASTER_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(merged)

    print(f"\nDone. {new_count} new fragrances added. Master now has {len(merged)} rows.")
    print(f"Saved to: {MASTER_CSV}")


if __name__ == "__main__":
    main()

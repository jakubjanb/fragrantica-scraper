#!/usr/bin/env python3
"""
Clean name and sex columns across all CSV files in 'Saved Data/'.

Changes applied:
  - sex:  "women and men"      →  "unisex"
  - name: removes the substring "for women and men" wherever it appears,
          then strips any resulting extra whitespace.
  - name: removes a trailing repetition of the brand name, e.g.
          brand="Affinessence", name="Gingembre Latte Affinessence"
          → "Gingembre Latte"
"""

import csv
import os
import shutil
import sys
import tempfile
from pathlib import Path

SAVED_DATA = Path(__file__).parent.parent / "Saved Data"


def clean_row(row: dict) -> tuple[dict, bool]:
    """Return (cleaned_row, changed) without mutating the original."""
    changed = False
    cleaned = dict(row)

    if cleaned.get("sex") == "women and men":
        cleaned["sex"] = "unisex"
        changed = True

    original_name = cleaned.get("name", "")
    new_name = original_name.replace("for women and men", "").strip()
    # Collapse any double spaces left behind (e.g. "Name  Brand" → "Name Brand")
    new_name = " ".join(new_name.split())

    # Remove trailing brand name repetition
    # e.g. brand="Affinessence", name="Gingembre Latte Affinessence" → "Gingembre Latte"
    brand = cleaned.get("brand", "").strip()
    if brand and new_name.endswith(brand):
        candidate = new_name[: -len(brand)].strip()
        if candidate:  # never leave the name empty
            new_name = candidate

    if new_name != original_name:
        cleaned["name"] = new_name
        changed = True

    return cleaned, changed


def process_csv(path: Path) -> tuple[int, int]:
    """
    Clean a single CSV file in place (atomic write).
    Returns (rows_changed, total_rows).
    """
    rows: list[dict] = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return 0, 0
        fieldnames = list(reader.fieldnames)
        # Only process files that have both target columns
        if "name" not in fieldnames and "sex" not in fieldnames:
            return 0, 0
        for row in reader:
            rows.append(row)

    if not rows:
        return 0, 0

    changed_count = 0
    cleaned_rows: list[dict] = []
    for row in rows:
        cleaned, changed = clean_row(row)
        cleaned_rows.append(cleaned)
        if changed:
            changed_count += 1

    if changed_count == 0:
        return 0, len(rows)

    # Atomic write: temp file → rename
    fd, tmp_path = tempfile.mkstemp(suffix=".csv", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(cleaned_rows)
        shutil.move(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    return changed_count, len(rows)


def main() -> None:
    csv_files = sorted(SAVED_DATA.rglob("*.csv"))

    if not csv_files:
        print(f"No CSV files found in {SAVED_DATA}")
        sys.exit(0)

    total_files_changed = 0
    total_rows_changed = 0

    for csv_path in csv_files:
        try:
            rows_changed, total_rows = process_csv(csv_path)
        except Exception as e:
            print(f"  [error] {csv_path.relative_to(SAVED_DATA)}: {e}", file=sys.stderr)
            continue

        if rows_changed > 0:
            rel = csv_path.relative_to(SAVED_DATA)
            print(f"  {rel}: {rows_changed}/{total_rows} rows updated")
            total_files_changed += 1
            total_rows_changed += rows_changed

    if total_files_changed == 0:
        print("Nothing to clean — all files are already up to date.")
    else:
        print(f"\nDone. {total_rows_changed} row(s) updated across {total_files_changed} file(s).")


if __name__ == "__main__":
    main()

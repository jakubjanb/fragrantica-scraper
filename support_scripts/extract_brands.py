#!/usr/bin/env python3
"""Extract unique, sorted brand names from all_brands_clean.csv."""
import csv
import os

INPUT_CSV = os.path.join("Saved Data", "all_brands_clean.csv")
OUTPUT_CSV = os.path.join("Saved Data", "list_of_brands.csv")


def main() -> None:
    brands: set[str] = set()
    with open(INPUT_CSV, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            brand = row.get("brand", "").strip()
            if brand:
                brands.add(brand)

    sorted_brands = sorted(brands, key=str.casefold)

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["brand"])
        writer.writerows([[b] for b in sorted_brands])

    print(f"Saved {len(sorted_brands)} unique brands to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()

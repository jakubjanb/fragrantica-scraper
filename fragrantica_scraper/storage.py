"""CSV storage utilities.

This module isolates CSV file handling: creating the file with headers,
loading existing URLs, and appending rows.
"""
from __future__ import annotations

import csv
import os
from typing import Dict, Iterable, Set

from .config import CSV_FIELDS


def ensure_csv_with_header(path: str) -> None:
    """Ensure a CSV file exists with the expected header.

    Creates parent directories as needed.
    """
    parent = os.path.dirname(path)
    if parent and not os.path.isdir(parent):
        try:
            os.makedirs(parent, exist_ok=True)
        except Exception:
            # Non-fatal; best-effort
            pass
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writeheader()


def load_existing_urls(path: str) -> Set[str]:
    """Return a set of URLs already present in the CSV file."""
    urls: Set[str] = set()
    if not os.path.exists(path):
        return urls
    try:
        with open(path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = (row.get("url") or "").strip()
                if url:
                    urls.add(url)
    except Exception:
        # If CSV is malformed, ignore and return what we have
        pass
    return urls


def append_row(path: str, row: Dict[str, object]) -> None:
    """Append a single row to the CSV file."""
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writerow(row)

"""Centralized configuration and constants for the Fragrantica scraper.

This module holds all constants, regex patterns, user-agent lists, and other
configuration values to avoid duplication and circular imports.
"""
from __future__ import annotations

import re
from typing import Final, List, Tuple

# Domain
DOMAIN: Final[str] = "www.fragrantica.com"

# Regex patterns
PERFUME_URL_RE: Final[re.Pattern[str]] = re.compile(
    r"^https?://(?:www\.)?fragrantica\.com/perfume/[^/]+/[^/]+-\d+\.html$",
    re.IGNORECASE,
)

# Path-only matcher for tighter link filtering
PERFUME_PATH_RE: Final[re.Pattern[str]] = re.compile(
    r"^/perfume/[^/]+/[^/]+-\d+\.html$", re.IGNORECASE
)

AVOID_PREFIXES: Final[Tuple[str, ...]] = (
    "/board/",
    "/designers/",
    "/search/",
    "/news/",
    "/articles/",
    "/perfumery/",
)

RATING_VOTES_RE: Final[re.Pattern[str]] = re.compile(
    r"Perfume\s+rating\s+([0-9]+(?:\.[0-9]+)?)\s+out\s+of\s+5\s+with\s+([\d,]+)\s+votes",
    re.IGNORECASE,
)

DESIGNER_LABEL_RE: Final[re.Pattern[str]] = re.compile(r"^\s*Designer\s*", re.IGNORECASE)

# CSV fields
CSV_FIELDS: Final[List[str]] = ["brand", "name", "rating", "votes", "url", "last_crawled", "sex", "fragrance_category"]

# Defaults for networking
DEFAULT_UAS: Final[List[str]] = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]

DEFAULT_ACCEPT_LANGS: Final[List[str]] = [
    "en-US,en;q=0.9",
    "en-GB,en;q=0.9",
    "en-US,en;q=0.8",
    "en-CA,en;q=0.9",
]

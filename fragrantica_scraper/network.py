"""Networking, session management, and politeness helpers.

This module encapsulates HTTP session creation, header/proxy handling,
robots.txt checks, URL normalization, link extraction, and various
sleep/backoff utilities.
"""
from __future__ import annotations

import random
import time
from typing import Iterable, Optional, Set
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup
import urllib.robotparser as robotparser

from .config import (
    AVOID_PREFIXES,
    DEFAULT_ACCEPT_LANGS,
    DEFAULT_UAS,
    DOMAIN,
    PERFUME_PATH_RE,
)


def build_session(
    user_agent: str,
    timeout: float,
    proxy: Optional[str] = None,
    accept_language: Optional[str] = None,
) -> requests.Session:
    """Create a configured requests.Session.

    If the given user-agent matches the placeholder, a realistic UA and
    Accept-Language will be chosen for the session.
    """
    if user_agent == "Mozilla/5.0 (compatible; PerfumeBot/1.0; +https://example.com/botinfo)":
        user_agent = random.choice(DEFAULT_UAS)
    if not accept_language:
        accept_language = random.choice(DEFAULT_ACCEPT_LANGS)
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": accept_language,
            "Connection": "keep-alive",
        }
    )
    if proxy:
        s.proxies = {"http": proxy, "https": proxy}
    # mypy: ignore[attr-defined] — attribute used by our code, not part of Session API
    s.timeout = timeout  # type: ignore[attr-defined]
    return s


def can_fetch(rp: robotparser.RobotFileParser, ua: str, url: str) -> bool:
    try:
        return rp.can_fetch(ua, url)
    except Exception:
        return False


def normalize_url(url: str) -> Optional[str]:
    try:
        u = urlparse(url)
        if not u.scheme:
            return None
        if u.netloc.lower().startswith("fragrantica.com"):
            # force www
            u = u._replace(netloc=DOMAIN)
        # Remove fragments
        u = u._replace(fragment="")
        return urlunparse(u)
    except Exception:
        return None


def extract_links(base_url: str, soup: BeautifulSoup) -> Set[str]:
    links: Set[str] = set()
    for a in soup.find_all("a", href=True):
        full = urljoin(base_url, a["href"])  # absolute
        full = normalize_url(full)
        if not full:
            continue
        u = urlparse(full)
        if u.netloc != DOMAIN:
            continue
        path = u.path or "/"
        # Allow only perfume detail pages; skip sensitive/rate-limited sections
        if not PERFUME_PATH_RE.match(path):
            if any(path.startswith(p) for p in AVOID_PREFIXES):
                continue
            # Skip non-perfume paths altogether
            continue
        # Exclude obvious non-HTML assets
        if any(full.lower().endswith(ext) for ext in (".jpg", ".png", ".gif", ".svg", ".css", ".js", ".json", ".xml")):
            continue
        links.add(full)
    return links


def polite_sleep(delay_min: float, delay_max: float) -> None:
    time.sleep(random.uniform(delay_min, delay_max))


def session_sleep(total_seconds: float, jitter_ratio: float = 0.1) -> None:
    """Take a longer break between scraping sessions with jitter."""
    if total_seconds <= 0:
        return
    jitter = total_seconds * jitter_ratio
    time.sleep(random.uniform(max(0.0, total_seconds - jitter), total_seconds + jitter))


def backoff_sleep(resp: Optional[requests.Response], base_delay: float, attempt: int) -> None:
    """Honor Retry-After and apply exponential backoff with jitter."""
    retry_after: Optional[str] = None
    if resp is not None:
        try:
            retry_after = resp.headers.get("Retry-After")
        except Exception:
            retry_after = None
    if retry_after:
        try:
            seconds = float(retry_after)
            time.sleep(max(seconds, base_delay))
            return
        except Exception:
            # If not numeric, fall through to exponential backoff
            pass
    sleep_min = base_delay * (2 ** (attempt - 1))
    sleep_max = sleep_min + 1.5
    polite_sleep(sleep_min, sleep_max)

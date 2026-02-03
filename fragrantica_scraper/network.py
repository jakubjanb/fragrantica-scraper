"""Networking, session management, and politeness helpers.

This module encapsulates HTTP session creation, header/proxy handling,
robots.txt checks, URL normalization, link extraction, and various
sleep/backoff utilities.
"""
from __future__ import annotations

import random
import time
from typing import Optional, Set, TYPE_CHECKING
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup
import urllib.robotparser as robotparser

try:
    import cloudscraper  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    cloudscraper = None

if TYPE_CHECKING:  # pragma: no cover
    import cloudscraper as _cloudscraper

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
    """Create a configured HTTP session.

    Prefers `cloudscraper` (when installed) to bypass Cloudflare.
    Falls back to a plain `requests.Session` when `cloudscraper` is unavailable.

    If the given user-agent matches the placeholder, a realistic UA and
    Accept-Language will be chosen for the session.
    """
    if user_agent == "Mozilla/5.0 (compatible; PerfumeBot/1.0; +https://example.com/botinfo)":
        user_agent = random.choice(DEFAULT_UAS)
    if not accept_language:
        accept_language = random.choice(DEFAULT_ACCEPT_LANGS)

    # Create cloudscraper session with browser configuration
    # Determine browser type from UA
    browser = None
    if "Chrome" in user_agent and "Edg" not in user_agent:
        browser = "chrome"
    elif "Firefox" in user_agent:
        browser = "firefox"

    if cloudscraper is not None:
        s = cloudscraper.create_scraper(
            browser={
                "browser": browser or "chrome",
                "platform": "windows"
                if "Windows" in user_agent
                else "darwin"
                if "Mac" in user_agent
                else "linux",
                "desktop": True,
            }
        )
    else:
        s = requests.Session()

    # Additional headers for more realistic requests
    headers = {
        "User-Agent": user_agent,
        "Accept-Language": accept_language,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "DNT": "1",
    }

    s.headers.update(headers)
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


def extract_links(base_url: str, soup: BeautifulSoup, limit_perfume_links: int = 0) -> Set[str]:
    """Extract links from a page.
    
    Args:
        base_url: The URL of the page being parsed
        soup: BeautifulSoup object of the page
        limit_perfume_links: If > 0, limit the number of perfume links extracted from designer pages
                            to avoid overwhelming the queue. Designer pages themselves are not limited.
    """
    links: Set[str] = set()
    perfume_links: list[str] = []
    designer_links: list[str] = []
    
    for a in soup.find_all("a", href=True):
        full = urljoin(base_url, a["href"])  # absolute
        full = normalize_url(full)
        if not full:
            continue
        u = urlparse(full)
        if u.netloc != DOMAIN:
            continue
        path = u.path or "/"
        # Allow perfume detail pages and designer pages (for navigation)
        if PERFUME_PATH_RE.match(path):
            # This is a perfume detail page
            perfume_links.append(full)
        elif path.startswith("/designers/"):
            # Allow designer pages for navigation to perfume pages
            designer_links.append(full)
        else:
            # Skip other paths (board, search, news, articles, perfumery, etc.)
            if any(path.startswith(p) for p in AVOID_PREFIXES):
                continue
            # Skip non-perfume, non-designer paths
            continue
    
    # Add designer links (no limit)
    for link in designer_links:
        if not any(link.lower().endswith(ext) for ext in (".jpg", ".png", ".gif", ".svg", ".css", ".js", ".json", ".xml")):
            links.add(link)
    
    # Add perfume links with optional limit
    if limit_perfume_links > 0 and len(perfume_links) > limit_perfume_links:
        # Randomly sample to avoid bias toward alphabetically first perfumes
        import random
        perfume_links = random.sample(perfume_links, limit_perfume_links)
    
    for link in perfume_links:
        if not any(link.lower().endswith(ext) for ext in (".jpg", ".png", ".gif", ".svg", ".css", ".js", ".json", ".xml")):
            links.add(link)
    
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

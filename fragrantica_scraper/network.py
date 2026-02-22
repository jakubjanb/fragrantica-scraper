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
    from curl_cffi import requests as curl_requests  # type: ignore
    HAS_CURL_CFFI = True
except ImportError:
    curl_requests = None  # type: ignore[assignment]
    HAS_CURL_CFFI = False

try:
    import cloudscraper  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    cloudscraper = None

if TYPE_CHECKING:  # pragma: no cover
    import cloudscraper as _cloudscraper

# Which HTTP backend is active (for logging)
HTTP_BACKEND: str = (
    "curl-cffi" if HAS_CURL_CFFI else
    "cloudscraper" if cloudscraper is not None else
    "requests"
)

# curl_cffi impersonation config — UA MUST exactly match the impersonation target
# so Cloudflare's TLS fingerprint check and User-Agent are consistent.
CURL_CFFI_IMPERSONATE: str = "chrome131"
CURL_CFFI_UA: str = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

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

    # Determine browser type from UA
    browser = "chrome"
    if "Firefox" in user_agent:
        browser = "firefox"
    elif "Safari" in user_agent and "Chrome" not in user_agent:
        browser = "safari"

    if HAS_CURL_CFFI and curl_requests is not None:
        # curl_cffi impersonates Chrome at the TLS + HTTP/2 level — this is what
        # Cloudflare Bot Management actually checks. We MUST NOT override the
        # Sec-Fetch-*, Accept-Encoding, Accept etc. headers because curl_cffi's
        # impersonation already sets them in the exact order/format Chrome uses.
        # Overriding them would break the HTTP/2 header fingerprint.
        # Only set User-Agent (must match impersonation) and Accept-Language.
        s = curl_requests.Session(impersonate=CURL_CFFI_IMPERSONATE)
        s.headers.update({
            "User-Agent": CURL_CFFI_UA,
            "Accept-Language": accept_language,
        })
    elif cloudscraper is not None:
        s = cloudscraper.create_scraper(
            browser={
                "browser": browser if browser in ("chrome", "firefox") else "chrome",
                "platform": "windows"
                if "Windows" in user_agent
                else "darwin"
                if "Mac" in user_agent
                else "linux",
                "desktop": True,
            }
        )
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
    else:
        s = requests.Session()
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
    try:
        s.timeout = timeout  # type: ignore[attr-defined]
    except (AttributeError, TypeError):
        pass
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

"""HTML parsing and regex-based extraction utilities.

Contains functions that operate on BeautifulSoup documents or text to parse
brands, names, ratings, and derive data from URLs.
"""
from __future__ import annotations

import re
from typing import Optional, Tuple, Dict

from bs4 import BeautifulSoup

from .config import DESIGNER_LABEL_RE, RATING_VOTES_RE


def clean_space(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def parse_brand_from_page(soup: BeautifulSoup) -> Optional[str]:
    # Look for "Designer <Brand>"
    for node in soup.find_all(text=DESIGNER_LABEL_RE):
        try:
            sibling_text = node.parent.get_text(" ", strip=True)
            m = re.search(r"Designer\s+(.*)", sibling_text, re.IGNORECASE)
            if m:
                return clean_space(m.group(1))
        except Exception:
            pass
    # Try a meta tag fallback
    og_title = soup.find("meta", attrs={"property": "og:title"})
    if og_title and og_title.get("content"):
        # Often contains brand + name, but it's fuzzy. Return None to rely on URL fallback.
        _ = og_title["content"]
    return None


def parse_name_from_page(soup: BeautifulSoup) -> Optional[str]:
    # Try the H1/H2
    h1 = soup.find(["h1", "h2"])
    if h1:
        txt = clean_space(h1.get_text(" ", strip=True))
        txt = re.sub(r"\s+for\s+(men|women|unisex)\s*$", "", txt, flags=re.IGNORECASE)
        return txt if txt else None

    # Try og:title
    og_title = soup.find("meta", attrs={"property": "og:title"})
    if og_title and og_title.get("content"):
        txt = clean_space(og_title["content"])
        txt = re.sub(r"\s+for\s+(men|women|unisex)\s*$", "", txt, flags=re.IGNORECASE)
        return txt if txt else None

    return None


def parse_rating_votes_from_text(text: str) -> Tuple[Optional[float], Optional[int]]:
    m = RATING_VOTES_RE.search(text)
    if not m:
        return None, None
    rating = float(m.group(1))
    votes = int(m.group(2).replace(",", ""))
    return rating, votes


def parse_brand_name_from_url(url: str) -> Tuple[Optional[str], Optional[str]]:
    # /perfume/<brand>/<name>-<id>.html
    try:
        from urllib.parse import urlparse

        path = urlparse(url).path
        parts = [p for p in path.split("/") if p]
        if len(parts) >= 3 and parts[0].lower() == "perfume":
            brand = parts[1]
            name_and_id = parts[2]
            # remove the -<id>.html suffix
            name = re.sub(r"-\d+\.html$", "", name_and_id, flags=re.IGNORECASE)
            # de-slug
            brand = clean_space(brand.replace("-", " ").replace("%26", "&").replace("%20", " "))
            name = clean_space(name.replace("-", " ").replace("%26", "&").replace("%20", " "))
            return brand, name
    except Exception:
        pass
    return None, None


def parse_category_and_sex(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    """Extract fragrance category and target sex from the opening description.

    Parses patterns like:
        "Orphéon Eau de Parfum by Diptyque is a Woody Chypre fragrance for women and men."
    Returns:
        (category, sex) e.g. ("Woody Chypre", "women and men")

    Uses a 3-tier strategy to avoid false positives from unrelated page text:
    1. Meta description tags (cleanest source)
    2. Individual paragraph/div elements
    3. Full page text with findall (prefer matches with category)

    Never early-returns on sex-only matches — keeps searching all tiers for a
    category+sex match first, falls back to sex-only at the very end.
    """
    CATEGORY_SEX_RE = re.compile(
        r"is\s+an?\s+([^\.]+?)\s+fragrance\s+for\s+([^\.]+?)[\.\,]", re.IGNORECASE
    )
    SEX_ONLY_RE = re.compile(
        r"is\s+a\s+fragrance\s+for\s+(.+?)[\.\,]", re.IGNORECASE
    )
    sex_fallback: Optional[str] = None  # best sex-only match across all tiers

    # 1) Try meta description tags (cleanest source, no HTML noise)
    for attr in (
        {"name": "description"},
        {"property": "og:description"},
    ):
        tag = soup.find("meta", attrs=attr)
        if not (tag and tag.get("content")):
            continue
        text = tag["content"]
        m = CATEGORY_SEX_RE.search(text)
        if m and len(clean_space(m.group(1))) <= 80:
            return clean_space(m.group(1)), clean_space(m.group(2))
        if sex_fallback is None:
            m2 = SEX_ONLY_RE.search(text)
            if m2:
                sex_fallback = clean_space(m2.group(1))

    # 2) Search individual elements (avoids cross-element false positives)
    for el in soup.find_all(["p", "div", "span"]):
        text = el.get_text(" ", strip=True)
        if "fragrance for" not in text.lower():
            continue
        m = CATEGORY_SEX_RE.search(text)
        if m:
            cat = clean_space(m.group(1))
            if len(cat) <= 80:
                return cat, clean_space(m.group(2))
        if sex_fallback is None:
            m2 = SEX_ONLY_RE.search(text)
            if m2:
                sex_fallback = clean_space(m2.group(1))

    # 3) Fallback: full page text, findall to prefer matches with category
    page_text = soup.get_text(" ", strip=True)
    for cat_raw, sex_raw in CATEGORY_SEX_RE.findall(page_text):
        cat = clean_space(cat_raw)
        if len(cat) <= 80:
            return cat, clean_space(sex_raw)
    if sex_fallback is None:
        m = SEX_ONLY_RE.search(page_text)
        if m:
            sex_fallback = clean_space(m.group(1))

    # Return sex-only fallback if no category was found anywhere
    if sex_fallback:
        return None, sex_fallback
    return None, None


def scrape_perfume_page(url: str, soup: BeautifulSoup) -> Dict[str, object]:
    """Extract brand, name, rating, votes from a fragrance detail page."""
    page_text = soup.get_text(" ", strip=True)
    rating, votes = parse_rating_votes_from_text(page_text)

    brand = parse_brand_from_page(soup)
    name = parse_name_from_page(soup)

    # Fallbacks from URL when needed
    u_brand, u_name = parse_brand_name_from_url(url)
    if brand is None:
        brand = u_brand
    if name is None:
        name = u_name

    # Final cleanup
    brand = clean_space(brand or "")
    name = clean_space(name or "")

    category, sex = parse_category_and_sex(soup)

    return {
        "brand": brand or None,
        "name": name or None,
        "rating": rating,
        "votes": votes,
        "sex": sex or "",
        "fragrance_category": category or "",
    }

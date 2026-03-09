"""
StampWorld.com scraper — multi-country ready.
Uses Playwright for JS-rendered pages. Paginates, extracts stamp metadata + images.

Usage:
    python scraper.py                                  # scrape Malta (default)
    python scraper.py --country France                 # scrape France
    python scraper.py --country Malta --reset           # wipe progress and re-scrape
    python scraper.py --country Malta --reset --max-pages 2  # first 2 pages only
"""

import argparse
import json
import logging
import os
import re
import tempfile
import time
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("stampworld")

# ---------------------------------------------------------------------------
# Country registry — add new countries here
# ---------------------------------------------------------------------------
COUNTRIES = {
    "Malta": {
        "collection_path": "/en/stamps/Malta/Postage%20stamps/1860-2026",
        "year_range": "1860-2026",
    },
    "Great-Britain": {
        "collection_path": "/en/stamps/Great-Britain/Postage%20stamps/1840-2026",
        "year_range": "1840-2026",
    },
    "Netherlands": {
        "collection_path": "/en/stamps/Netherlands/Postage%20stamps/1852-2027",
        "year_range": "1852-2027",
    },
    "France": {
        "collection_path": "/en/stamps/France/Postage%20stamps/1849-2026",
        "year_range": "1849-2026",
    },
    "Germany": {
        "collection_path": "/en/stamps/Germany/Postage%20stamps/1949-2026",
        "year_range": "1949-2026",
    },
    "Belgium": {
        "collection_path": "/en/stamps/Belgium/Postage%20stamps/1849-2026",
        "year_range": "1849-2026",
    },
    "Austria": {
        "collection_path": "/en/stamps/Austria/Postage%20stamps/1850-2026",
        "year_range": "1850-2026",
    },
    "Switzerland": {
        "collection_path": "/en/stamps/Switzerland/Postage%20stamps/1849-2026",
        "year_range": "1849-2026",
    },
    "Italy": {
        "collection_path": "/en/stamps/Italy/Postage%20stamps/1861-2026",
        "year_range": "1861-2026",
    },
    "Spain": {
        "collection_path": "/en/stamps/Spain/Postage%20stamps/1850-2026",
        "year_range": "1850-2026",
    },
    "Portugal": {
        "collection_path": "/en/stamps/Portugal/Postage%20stamps/1853-2026",
        "year_range": "1853-2026",
    },
    "Sweden": {
        "collection_path": "/en/stamps/Sweden/Postage%20stamps/1855-2025",
        "year_range": "1855-2025",
    },
    "Norway": {
        "collection_path": "/en/stamps/Norway/Postage%20stamps/1855-2026",
        "year_range": "1855-2026",
    },
    "Denmark": {
        "collection_path": "/en/stamps/Denmark/Postage%20stamps/1851-2025",
        "year_range": "1851-2025",
    },
    "Finland": {
        "collection_path": "/en/stamps/Finland/Postage%20stamps/1856-2026",
        "year_range": "1856-2026",
    },
    "Poland": {
        "collection_path": "/en/stamps/Poland/Postage%20stamps/1860-2026",
        "year_range": "1860-2026",
    },
    "Czech-Republic": {
        "collection_path": "/en/stamps/Czech-Republic/Postage%20stamps/1993-2026",
        "year_range": "1993-2026",
    },
    "Hungary": {
        "collection_path": "/en/stamps/Hungary/Postage%20stamps/1871-2026",
        "year_range": "1871-2026",
    },
    "Romania": {
        "collection_path": "/en/stamps/Romania/Postage%20stamps/1858-2026",
        "year_range": "1858-2026",
    },
    "Bulgaria": {
        "collection_path": "/en/stamps/Bulgaria/Postage%20stamps/1879-2025",
        "year_range": "1879-2025",
    },
    "Greece": {
        "collection_path": "/en/stamps/Greece/Postage%20stamps/1861-2026",
        "year_range": "1861-2026",
    },
    "Turkey": {
        "collection_path": "/en/stamps/Turkey/Postage%20stamps/1863-2026",
        "year_range": "1863-2026",
    },
    "Russia": {
        "collection_path": "/en/stamps/Russia/Postage%20stamps/1857-2026",
        "year_range": "1857-2026",
    },
    "Ukraine": {
        "collection_path": "/en/stamps/Ukraine/Postage%20stamps/1918-2026",
        "year_range": "1918-2026",
    },
    "Ireland": {
        "collection_path": "/en/stamps/Ireland/Postage%20stamps/1922-2026",
        "year_range": "1922-2026",
    },
    "Luxembourg": {
        "collection_path": "/en/stamps/Luxembourg/Postage%20stamps/1852-2026",
        "year_range": "1852-2026",
    },
    "Monaco": {
        "collection_path": "/en/stamps/Monaco/Postage%20stamps/1885-2026",
        "year_range": "1885-2026",
    },
    "Liechtenstein": {
        "collection_path": "/en/stamps/Liechtenstein/Postage%20stamps/1912-2026",
        "year_range": "1912-2026",
    },
    "Iceland": {
        "collection_path": "/en/stamps/Iceland/Postage%20stamps/1873-2023",
        "year_range": "1873-2023",
    },
    "Croatia": {
        "collection_path": "/en/stamps/Croatia/Postage%20stamps/1941-2026",
        "year_range": "1941-2026",
    },
    "Slovenia": {
        "collection_path": "/en/stamps/Slovenia/Postage%20stamps/1991-2026",
        "year_range": "1991-2026",
    },
    "Slovakia": {
        "collection_path": "/en/stamps/Slovakia/Postage%20stamps/1939-2026",
        "year_range": "1939-2026",
    },
    "Serbia": {
        "collection_path": "/en/stamps/Serbia/Postage%20stamps/1866-2026",
        "year_range": "1866-2026",
    },
    "Montenegro": {
        "collection_path": "/en/stamps/Montenegro/Postage%20stamps/1874-2026",
        "year_range": "1874-2026",
    },
    "Albania": {
        "collection_path": "/en/stamps/Albania/Postage%20stamps/1913-2025",
        "year_range": "1913-2025",
    },
    "Kosovo": {
        "collection_path": "/en/stamps/Kosovo/Postage%20stamps/2000-2025",
        "year_range": "2000-2025",
    },
    "Moldova": {
        "collection_path": "/en/stamps/Moldova/Postage%20stamps/1991-2026",
        "year_range": "1991-2026",
    },
    "Belarus": {
        "collection_path": "/en/stamps/Belarus/Postage%20stamps/1992-2026",
        "year_range": "1992-2026",
    },
    "Latvia": {
        "collection_path": "/en/stamps/Latvia/Postage%20stamps/1918-2026",
        "year_range": "1918-2026",
    },
    "Lithuania": {
        "collection_path": "/en/stamps/Lithuania/Postage%20stamps/1918-2026",
        "year_range": "1918-2026",
    },
    "Estonia": {
        "collection_path": "/en/stamps/Estonia/Postage%20stamps/1918-2026",
        "year_range": "1918-2026",
    },
    "Vatican-City": {
        "collection_path": "/en/stamps/Vatican-City/Postage%20stamps/1929-2025",
        "year_range": "1929-2025",
    },
    "San-Marino": {
        "collection_path": "/en/stamps/San-Marino/Postage%20stamps/1877-2026",
        "year_range": "1877-2026",
    },
    "DDR": {
        "collection_path": "/en/stamps/DDR/Postage%20stamps/1949-1990",
        "year_range": "1949-1990",
    },
    "USSR": {
        "collection_path": "/en/stamps/USSR/Postage%20stamps/1923-1991",
        "year_range": "1923-1991",
    },
    "Czechoslovakia": {
        "collection_path": "/en/stamps/Czechoslovakia/Postage%20stamps/1918-1992",
        "year_range": "1918-1992",
    },
    "Yugoslavia": {
        "collection_path": "/en/stamps/Yugoslavia/Postage%20stamps/1918-2006",
        "year_range": "1918-2006",
    },
    "Saar": {
        "collection_path": "/en/stamps/Saar/Postage%20stamps/1950-1956",
        "year_range": "1950-1956",
    },
    "Aaland": {
        "collection_path": "/en/stamps/Aaland/Postage%20stamps/1984-2026",
        "year_range": "1984-2026",
    },
    "Faroe-Islands": {
        "collection_path": "/en/stamps/Faroe-Islands/Postage%20stamps/1975-2026",
        "year_range": "1975-2026",
    },
    "Greenland": {
        "collection_path": "/en/stamps/Greenland/Postage%20stamps/1938-2026",
        "year_range": "1938-2026",
    },
    "Gibraltar": {
        "collection_path": "/en/stamps/Gibraltar/Postage%20stamps/1886-2025",
        "year_range": "1886-2025",
    },
    "United-States": {
        "collection_path": "/en/stamps/United-States/Postage%20stamps/1847-2026",
        "year_range": "1847-2026",
    },
    # --- North & Central America + Caribbean ---
    "Anguilla": {
        "collection_path": "/en/stamps/Anguilla/Postage%20stamps/1967-2016",
        "year_range": "1967-2016",
    },
    "Antigua-And-Barbuda": {
        "collection_path": "/en/stamps/Antigua-And-Barbuda/Postage%20stamps/1981-2024",
        "year_range": "1981-2024",
    },
    "Aruba": {
        "collection_path": "/en/stamps/Aruba/Postage%20stamps/1986-2024",
        "year_range": "1986-2024",
    },
    "Bahamas": {
        "collection_path": "/en/stamps/Bahamas/Postage%20stamps/1859-2025",
        "year_range": "1859-2025",
    },
    "Barbados": {
        "collection_path": "/en/stamps/Barbados/Postage%20stamps/1852-2024",
        "year_range": "1852-2024",
    },
    "Belize": {
        "collection_path": "/en/stamps/Belize/Postage%20stamps/1973-2021",
        "year_range": "1973-2021",
    },
    "Bermuda": {
        "collection_path": "/en/stamps/Bermuda/Postage%20stamps/1848-2025",
        "year_range": "1848-2025",
    },
    "British-Virgin-Islands": {
        "collection_path": "/en/stamps/British-Virgin-Islands/Postage%20stamps/1866-2023",
        "year_range": "1866-2023",
    },
    "Canada": {
        "collection_path": "/en/stamps/Canada/Postage%20stamps/1868-2026",
        "year_range": "1868-2026",
    },
    "Cayman-Islands": {
        "collection_path": "/en/stamps/Cayman-Islands/Postage%20stamps/1901-2024",
        "year_range": "1901-2024",
    },
    "Costa-Rica": {
        "collection_path": "/en/stamps/Costa-Rica/Postage%20stamps/1863-2025",
        "year_range": "1863-2025",
    },
    "Cuba": {
        "collection_path": "/en/stamps/Cuba/Postage%20stamps/1899-2022",
        "year_range": "1899-2022",
    },
    "Curacao": {
        "collection_path": "/en/stamps/Curacao/Postage%20stamps/1873-2025",
        "year_range": "1873-2025",
    },
    "Danish-West-Indies": {
        "collection_path": "/en/stamps/Danish-West-Indies/Postage%20stamps/1856-1915",
        "year_range": "1856-1915",
    },
    "Dominica": {
        "collection_path": "/en/stamps/Dominica/Postage%20stamps/1874-2024",
        "year_range": "1874-2024",
    },
    "Dominican-Republic": {
        "collection_path": "/en/stamps/Dominican-Republic/Postage%20stamps/1865-2025",
        "year_range": "1865-2025",
    },
    "El-Salvador": {
        "collection_path": "/en/stamps/El-Salvador/Postage%20stamps/1867-2024",
        "year_range": "1867-2024",
    },
    "Grenada": {
        "collection_path": "/en/stamps/Grenada/Postage%20stamps/1861-2024",
        "year_range": "1861-2024",
    },
    "Grenada-Grenadines": {
        "collection_path": "/en/stamps/Grenada-Grenadines/Postage%20stamps/1973-2021",
        "year_range": "1973-2021",
    },
    "Guadeloupe": {
        "collection_path": "/en/stamps/Guadeloupe/Postage%20stamps/1884-1947",
        "year_range": "1884-1947",
    },
    "Guatemala": {
        "collection_path": "/en/stamps/Guatemala/Postage%20stamps/1871-2026",
        "year_range": "1871-2026",
    },
    "Haiti": {
        "collection_path": "/en/stamps/Haiti/Postage%20stamps/1881-2010",
        "year_range": "1881-2010",
    },
    "Honduras": {
        "collection_path": "/en/stamps/Honduras/Postage%20stamps/1866-2025",
        "year_range": "1866-2025",
    },
    "Jamaica": {
        "collection_path": "/en/stamps/Jamaica/Postage%20stamps/1860-2025",
        "year_range": "1860-2025",
    },
    "Leeward-Islands": {
        "collection_path": "/en/stamps/Leeward-Islands/Postage%20stamps/1890-1954",
        "year_range": "1890-1954",
    },
    "Martinique": {
        "collection_path": "/en/stamps/Martinique/Postage%20stamps/1886-1947",
        "year_range": "1886-1947",
    },
    "Mexico": {
        "collection_path": "/en/stamps/Mexico/Postage%20stamps/1856-2025",
        "year_range": "1856-2025",
    },
    "Montserrat": {
        "collection_path": "/en/stamps/Montserrat/Postage%20stamps/1876-2019",
        "year_range": "1876-2019",
    },
    "Netherlands-Antilles": {
        "collection_path": "/en/stamps/Netherlands-Antilles/Postage%20stamps/1949-2010",
        "year_range": "1949-2010",
    },
    "Netherlands-Caribbean": {
        "collection_path": "/en/stamps/Netherlands-Caribbean/Postage%20stamps/2010-2012",
        "year_range": "2010-2012",
    },
    "Nevis": {
        "collection_path": "/en/stamps/Nevis/Postage%20stamps/1980-2024",
        "year_range": "1980-2024",
    },
    "Nicaragua": {
        "collection_path": "/en/stamps/Nicaragua/Postage%20stamps/1862-2021",
        "year_range": "1862-2021",
    },
    "Panama": {
        "collection_path": "/en/stamps/Panama/Postage%20stamps/1878-2024",
        "year_range": "1878-2024",
    },
    "Puerto-Rico": {
        "collection_path": "/en/stamps/Puerto-Rico/Postage%20stamps/1873-1900",
        "year_range": "1873-1900",
    },
    "Sint-Maartin": {
        "collection_path": "/en/stamps/Sint-Maartin/Postage%20stamps/2010-2025",
        "year_range": "2010-2025",
    },
    "St.-Kitts": {
        "collection_path": "/en/stamps/St.-Kitts/Postage%20stamps/1980-2024",
        "year_range": "1980-2024",
    },
    "St.-Lucia": {
        "collection_path": "/en/stamps/St.-Lucia/Postage%20stamps/1860-2024",
        "year_range": "1860-2024",
    },
    "St.-Pierre-et-Miquelon": {
        "collection_path": "/en/stamps/St.-Pierre-et-Miquelon/Postage%20stamps/1885-2026",
        "year_range": "1885-2026",
    },
    "St.-Vincent-And-The-Grenadines": {
        "collection_path": "/en/stamps/St.-Vincent-And-The-Grenadines/Postage%20stamps/1993-2024",
        "year_range": "1993-2024",
    },
    "Trinidad-And-Tobago": {
        "collection_path": "/en/stamps/Trinidad-And-Tobago/Postage%20stamps/1913-2022",
        "year_range": "1913-2022",
    },
    "Turks-And-Caicos-Islands": {
        "collection_path": "/en/stamps/Turks-And-Caicos-Islands/Postage%20stamps/1900-2022",
        "year_range": "1900-2022",
    },
    "UN-New-York": {
        "collection_path": "/en/stamps/UN-New-York/Postage%20stamps/1951-2026",
        "year_range": "1951-2026",
    },
    # --- US territories / sub-issues ---
    "Canal-Zone": {
        "collection_path": "/en/stamps/Canal-Zone/Postage%20stamps/1904-1978",
        "year_range": "1904-1978",
    },
    "U.S.-Post-China": {
        "collection_path": "/en/stamps/U.S.-Post-China/Postage%20stamps/1919-1922",
        "year_range": "1919-1922",
    },
    "Guam": {
        "collection_path": "/en/stamps/Guam/Postage%20stamps/1899-1930",
        "year_range": "1899-1930",
    },
    "Mariana-Islands": {
        "collection_path": "/en/stamps/Mariana-Islands/Postage%20stamps/1899-1899",
        "year_range": "1899-1899",
    },
    "U.S.-Cuba": {
        "collection_path": "/en/stamps/U.S.-Cuba/Postage%20stamps/1898-1899",
        "year_range": "1898-1899",
    },
    "Hawaii": {
        "collection_path": "/en/stamps/Hawaii/Postage%20stamps/1851-1899",
        "year_range": "1851-1899",
    },
    "Confederate-States": {
        "collection_path": "/en/stamps/Confederate-States/Postage%20stamps/1861-1863",
        "year_range": "1861-1863",
    },
    # --- France & French territories ---
    "Andorra-FR": {
        "collection_path": "/en/stamps/Andorra-FR/Postage%20stamps/1931-2026",
        "year_range": "1931-2026",
    },
    "French-Oceania": {
        "collection_path": "/en/stamps/French-Oceania/Postage%20stamps/1892-1956",
        "year_range": "1892-1956",
    },
    "French-Polynesia": {
        "collection_path": "/en/stamps/French-Polynesia/Postage%20stamps/1958-2026",
        "year_range": "1958-2026",
    },
    "French-South-and-Antarctic-Terr.": {
        "collection_path": "/en/stamps/French-South-and-Antarctic-Terr./Postage%20stamps/1955-2026",
        "year_range": "1955-2026",
    },
    "Mayotte": {
        "collection_path": "/en/stamps/Mayotte/Postage%20stamps/1892-2011",
        "year_range": "1892-2011",
    },
    "New-Caledonia": {
        "collection_path": "/en/stamps/New-Caledonia/Postage%20stamps/1860-2025",
        "year_range": "1860-2025",
    },
    "Wallis-and-Futuna-Islands": {
        "collection_path": "/en/stamps/Wallis-and-Futuna-Islands/Postage%20stamps/1920-2025",
        "year_range": "1920-2025",
    },
    "Conseil-de-LEurope": {
        "collection_path": "/en/stamps/Conseil-de-LEurope/Postage%20stamps/1958-2019",
        "year_range": "1958-2019",
    },
    "UNESCO": {
        "collection_path": "/en/stamps/UNESCO/Postage%20stamps/1961-2019",
        "year_range": "1961-2019",
    },
    "Castellorizo": {
        "collection_path": "/en/stamps/Castellorizo/Postage%20stamps/1920-1920",
        "year_range": "1920-1920",
    },
    "Rouad,-Ile": {
        "collection_path": "/en/stamps/Rouad,-Ile/Postage%20stamps/1916-1916",
        "year_range": "1916-1916",
    },
    "French-Committee-of-National-Liberation": {
        "collection_path": "/en/stamps/French-Committee-of-National-Liberation/Postage%20stamps/1943-1944",
        "year_range": "1943-1944",
    },
}

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_URL = "https://www.stampworld.com"
USER_ID = os.environ.get("STAMPWORLD_USER_ID", "694157")
IMAGES_ROOT = "stamp_images"
OUTPUT_ROOT = "output"
DELAY_SECONDS = 1.5
MAX_RETRIES = 3
RETRY_BACKOFF = 2


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def sanitize_filename(name: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "_", name)


def atomic_json_write(path: str, data) -> None:
    dir_name = os.path.dirname(path) or "."
    os.makedirs(dir_name, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=dir_name, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, path)
    except Exception:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def paths_for_country(country: str):
    slug = country.replace(" ", "_").replace("-", "_")
    images_dir = os.path.join(IMAGES_ROOT, country)
    output_file = os.path.join(OUTPUT_ROOT, f"stamps_{slug.lower()}.json")
    progress_file = os.path.join(OUTPUT_ROOT, f"progress_{slug.lower()}.json")
    os.makedirs(images_dir, exist_ok=True)
    os.makedirs(OUTPUT_ROOT, exist_ok=True)
    return output_file, progress_file, images_dir


def download_image(url: str, filepath: str, retries: int = MAX_RETRIES):
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code == 200 and len(r.content) > 500:
                d = os.path.dirname(filepath)
                if d:
                    os.makedirs(d, exist_ok=True)
                with open(filepath, "wb") as f:
                    f.write(r.content)
                return filepath
            if r.status_code == 429:
                wait = RETRY_BACKOFF ** attempt
                log.warning("Image 429 for %s — backing off %ds", url, wait)
                time.sleep(wait)
                continue
            log.warning("Image HTTP %d for %s (%d/%d)", r.status_code, url, attempt, retries)
        except Exception as e:
            log.warning("Image error %s (%d/%d): %s", url, attempt, retries, e)
        if attempt < retries:
            time.sleep(RETRY_BACKOFF ** attempt)
    return None


# ---------------------------------------------------------------------------
# Dynamic column mapping from <thead>
# ---------------------------------------------------------------------------

HEADER_TO_FIELD = {
    "type": "catalogue_type", "denomination": "denomination", "d": "denomination",
    "perf.": "perforations", "perforations": "perforations",
    "colour": "colour", "color": "colour",
    "paper": "paper", "watermark": "watermark", "wmk": "watermark",
    "description": "watermark", "quantity": "quantity",
    "mint (nh)": "price_mint_nh", "mint(nh)": "price_mint_nh",
    "unused": "price_unused", "mint (h)": "price_unused", "mint(h)": "price_unused",
    "used": "price_used",
    "on cover": "price_on_cover", "on letter": "price_on_cover",
    "currency": "currency",
}

# Positional fallback — StampWorld uses empty headers for most columns.
FALLBACK_COLUMN_MAP = {
    0: "catalogue_type", 1: "denomination", 2: "perforations",
    3: "colour", 4: "paper", 5: "watermark", 6: "quantity",
    8: "price_mint_nh", 9: "price_unused", 10: "price_used",
    11: "price_on_cover", 12: "currency",
}


def build_column_map(soup: BeautifulSoup):
    """
    Try to build column map from <thead>. For columns with empty/unrecognised
    headers, fill in from the positional fallback.
    """
    thead = soup.find("thead")
    if not thead:
        return None
    ths = thead.find_all(["th", "td"])
    if len(ths) < 5:
        return None
    col_map = {}
    td_index = 0
    for th in ths:
        text = th.get_text(strip=True).lower()
        scope = th.get("scope", "")
        if scope == "row" or (th.name == "th" and td_index == 0
                              and text in ("", "no", "no.", "number", "#")):
            continue
        if text in HEADER_TO_FIELD:
            col_map[td_index] = HEADER_TO_FIELD[text]
        td_index += 1
    # Merge: fallback fills gaps the header didn't cover
    merged = FALLBACK_COLUMN_MAP.copy()
    merged.update(col_map)
    return merged


# ---------------------------------------------------------------------------
# Page validation
# ---------------------------------------------------------------------------

class PageValidationError(Exception):
    pass


def validate_page(page_num, stamps, soup, existing_ids):
    errors = []
    warnings = []

    if len(stamps) == 0:
        errors.append(f"Page {page_num}: zero stamps parsed")

    page_text = soup.get_text(separator=" ", strip=True).lower()
    for flag in ["rate limit", "captcha", "access denied", "please verify",
                 "too many requests", "403 forbidden"]:
        if flag in page_text:
            errors.append(f"Page {page_num}: detected '{flag}'")

    if errors:
        raise PageValidationError("; ".join(errors))

    if len(stamps) > 100:
        warnings.append(f"Page {page_num}: high count ({len(stamps)})")

    for i, s in enumerate(stamps):
        if not s.get("number"):
            warnings.append(f"Page {page_num}, row {i}: missing number")
        if not s.get("sw_id"):
            warnings.append(f"Page {page_num}, row {i}: missing sw_id")

    ids = [s["sw_id"] for s in stamps if s.get("sw_id")]
    if len(ids) != len(set(ids)):
        dupes = {x for x in ids if ids.count(x) > 1}
        warnings.append(f"Page {page_num}: duplicate IDs: {dupes}")

    new_ids = {s["sw_id"] for s in stamps if s.get("sw_id")}
    overlap = existing_ids & new_ids
    if overlap:
        warnings.append(f"Page {page_num}: {len(overlap)} IDs already in dataset")

    for s in stamps:
        if not s.get("denomination"):
            warnings.append(f"Page {page_num}, stamp {s.get('number','?')}: no denomination")
        if not s.get("currency"):
            warnings.append(f"Page {page_num}, stamp {s.get('number','?')}: no currency")

    with_image = sum(1 for s in stamps if s.get("image_url"))
    if stamps and with_image / len(stamps) < 0.5:
        warnings.append(f"Page {page_num}: only {with_image}/{len(stamps)} have images")

    nums = []
    for s in stamps:
        m = re.match(r'^(\d+)', s.get("number", ""))
        if m:
            nums.append(int(m.group(1)))
    for i in range(1, len(nums)):
        if nums[i] < nums[i - 1]:
            warnings.append(f"Page {page_num}: not monotonic ({nums[i-1]} → {nums[i]})")
            break

    if warnings:
        for w in warnings:
            log.warning("⚠ %s", w)
    else:
        log.info("✓ Page %d: %d stamps, %d images", page_num, len(stamps), with_image)

    return warnings


# ---------------------------------------------------------------------------
# Page parsing
# ---------------------------------------------------------------------------

def parse_page(page_content, country, images_dir, col_map=None):
    soup = BeautifulSoup(page_content, "html.parser")
    stamps = []

    if col_map is None:
        col_map = build_column_map(soup)
        if col_map:
            log.info("Column map from <thead>: %s", col_map)
        else:
            log.warning("No <thead>, using fallback column map")
            col_map = FALLBACK_COLUMN_MAP.copy()

    img_map = {}
    group_text_map = {}
    for div in soup.select("div[id^='group_box_']"):
        gid = div["id"].replace("group_box_", "")
        for img in div.select("img.img-fluid"):
            src = img.get("src", "")
            if "/media/catalogue/" not in src:
                continue
            # Extract type from alt text: "[..., type XX]"
            alt = img.get("alt", "")
            m = re.search(r'type\s+(\S+)\]', alt)
            if m:
                img_type = m.group(1)
                img_map[(gid, img_type)] = urljoin(BASE_URL, src)
            else:
                # No specific type — store as group-level fallback image
                if (gid, "__group__") not in img_map:
                    img_map[(gid, "__group__")] = urljoin(BASE_URL, src)
        # Extract group header text (year, subject, designer, etc.)
        header = div.select_one("div.table_header")
        if header:
            group_text_map[gid] = " ".join(header.get_text(" ", strip=True).split())

    rows = soup.select("tr.stamp_tr")
    country_slug = country.lower().replace(" ", "_")

    for row in rows:
        th = row.find("th")
        stamp_number = th.get_text(strip=True) if th else ""
        stamp_type = row.get("data-stamp-type", "")

        if stamp_type == "-" or re.search(r'\d+\s*[-\u2013\u2011]\s*\d+', stamp_number):
            continue

        group_id = row.get("data-stamp-group-id", "")
        stamp = {
            "sw_id": f"{country_slug}_{stamp_number}",
            "country": country,
            "number": stamp_number,
            "group_id": group_id,
            "type": stamp_type,
        }
        if group_id in group_text_map:
            stamp["group_title"] = group_text_map[group_id]

        key = (group_id, stamp_type)
        if key in img_map:
            stamp["image_url"] = img_map[key]
        elif (group_id, "__group__") in img_map:
            stamp["image_url"] = img_map[(group_id, "__group__")]

        tds = row.find_all("td")
        texts = [td.get_text(" ", strip=True) for td in tds]
        for idx, field in col_map.items():
            if idx < len(texts):
                stamp[field] = texts[idx]

        share = row.find("a", class_="addthis_button_compact")
        if share:
            stamp["detail_url"] = share.get("addthis:url", "")

        if stamp.get("image_url"):
            ext = os.path.splitext(urlparse(stamp["image_url"]).path)[-1] or ".jpg"
            safe_num = sanitize_filename(
                stamp_number.zfill(4) if stamp_number.isdigit() else stamp_number
            )
            safe_type = sanitize_filename(stamp_type)
            fn = f"{country_slug}_sw{safe_num}_{safe_type}{ext}" if safe_type else f"{country_slug}_sw{safe_num}{ext}"
            local_path = os.path.join(images_dir, fn)
            if not os.path.exists(local_path):
                result = download_image(stamp["image_url"], local_path)
                stamp["local_image"] = result
            else:
                stamp["local_image"] = local_path

        stamps.append(stamp)

    return stamps, soup, col_map


def get_next_page_number(soup, current_page):
    page_numbers = set()
    for a in soup.find_all("a", href=re.compile(r"page=")):
        m = re.search(r'page=(\d+)', a["href"])
        if m:
            page_numbers.add(int(m.group(1)))
    expected = current_page + 1
    if expected in page_numbers:
        return expected
    higher = sorted(n for n in page_numbers if n > current_page)
    return higher[0] if higher else None


# ---------------------------------------------------------------------------
# Resume / dedup / output
# ---------------------------------------------------------------------------

def load_existing(output_file, progress_file):
    all_stamps = []
    existing_ids = set()
    start_page = 1

    if os.path.exists(output_file):
        try:
            with open(output_file, encoding="utf-8") as f:
                data = json.load(f)
            all_stamps = data.get("stamps", []) if isinstance(data, dict) else data
            existing_ids = {s["sw_id"] for s in all_stamps if s.get("sw_id")}
            log.info("Loaded %d existing stamps from %s", len(all_stamps), output_file)
        except Exception as e:
            log.warning("Could not load %s: %s", output_file, e)

    if os.path.exists(progress_file):
        try:
            with open(progress_file, encoding="utf-8") as f:
                progress = json.load(f)
            start_page = progress.get("last_completed_page", 0) + 1
            log.info("Resuming from page %d", start_page)
        except Exception:
            start_page = 1

    return all_stamps, existing_ids, start_page


def deduplicate(stamps):
    seen = {}
    for s in stamps:
        seen[s.get("sw_id", id(s))] = s
    return list(seen.values())


def build_output(country, stamps, pages_scraped):
    return {
        "metadata": {
            "source": "stampworld.com",
            "country": country,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "total_stamps": len(stamps),
            "total_pages": pages_scraped,
            "scraper_version": "2.0.0",
        },
        "stamps": stamps,
    }


# ---------------------------------------------------------------------------
# Main scrape loop
# ---------------------------------------------------------------------------

def scrape_country(country, reset=False, max_pages=None):
    if country not in COUNTRIES:
        log.error("Unknown country '%s'. Available: %s", country, list(COUNTRIES.keys()))
        return

    config = COUNTRIES[country]
    output_file, progress_file, images_dir = paths_for_country(country)

    if reset:
        for f in (output_file, progress_file):
            if os.path.exists(f):
                os.remove(f)
                log.info("Removed %s", f)

    all_stamps, existing_ids, start_page = load_existing(output_file, progress_file)
    log.info("=== StampWorld Scraper v2 — %s ===", country)

    pages_done = 0
    BROWSER_RESTART_EVERY = 20  # fresh browser every N pages to avoid memory/session rot

    with sync_playwright() as p:
        browser = None
        context = None
        page = None

        def fresh_browser():
            nonlocal browser, context, page
            if browser:
                try:
                    browser.close()
                except Exception:
                    pass
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 Chrome/120 Safari/537.36"
            )
            page = context.new_page()
            log.info("Fresh browser session started.")
            return page

        page = fresh_browser()
        current_page = start_page
        col_map = None
        consecutive_empty = 0
        pages_since_restart = 0

        while True:
            if max_pages is not None and pages_done >= max_pages:
                log.info("Reached --max-pages %d, stopping.", max_pages)
                break

            # Restart browser periodically to prevent session degradation
            if pages_since_restart >= BROWSER_RESTART_EVERY:
                log.info("Restarting browser after %d pages.", pages_since_restart)
                page = fresh_browser()
                pages_since_restart = 0

            url = f"{BASE_URL}{config['collection_path']}?user={USER_ID}&page={current_page}"
            log.info("Page %d: %s", current_page, url)

            loaded = False
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    page.wait_for_selector("tr.stamp_tr", timeout=15000)
                    loaded = True
                    break
                except Exception as e:
                    wait = RETRY_BACKOFF ** attempt
                    log.warning("Page %d load fail (%d/%d): %s — retry in %ds",
                                current_page, attempt, MAX_RETRIES, e, wait)
                    time.sleep(wait)
                    # On last retry, try a fresh browser
                    if attempt == MAX_RETRIES - 1:
                        log.info("Restarting browser before final retry.")
                        page = fresh_browser()

            if not loaded:
                log.error("Page %d: failed after %d attempts, stopping.", current_page, MAX_RETRIES)
                break

            time.sleep(1)

            stamps, soup, col_map = parse_page(page.content(), country, images_dir, col_map)
            log.info("Page %d: %d stamps found", current_page, len(stamps))

            if not stamps:
                consecutive_empty += 1
                if consecutive_empty >= 2:
                    log.info("Two consecutive empty pages — end of catalogue.")
                    break
                log.warning("Empty page %d, trying next.", current_page)
                current_page += 1
                time.sleep(DELAY_SECONDS)
                continue
            consecutive_empty = 0

            try:
                validate_page(current_page, stamps, soup, existing_ids)
            except PageValidationError as e:
                log.error("Critical: %s — retrying with fresh browser", e)
                page = fresh_browser()
                pages_since_restart = 0
                time.sleep(3)
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    page.wait_for_selector("tr.stamp_tr", timeout=15000)
                except Exception:
                    log.error("Reload also failed — stopping.")
                    break
                time.sleep(2)
                stamps, soup, col_map = parse_page(page.content(), country, images_dir, col_map)
                try:
                    validate_page(current_page, stamps, soup, existing_ids)
                except PageValidationError as e2:
                    log.error("Still failing: %s — stopping.", e2)
                    break

            new_stamps = [s for s in stamps if s.get("sw_id") not in existing_ids]
            if len(new_stamps) < len(stamps):
                log.info("Page %d: %d new, %d dupes skipped",
                         current_page, len(new_stamps), len(stamps) - len(new_stamps))

            all_stamps.extend(new_stamps)
            existing_ids.update(s["sw_id"] for s in new_stamps if s.get("sw_id"))

            atomic_json_write(output_file, build_output(country, all_stamps, current_page))
            atomic_json_write(progress_file, {"last_completed_page": current_page})

            pages_done += 1
            pages_since_restart += 1

            next_pg = get_next_page_number(soup, current_page)
            if next_pg is None:
                log.info("No next page — done.")
                break
            current_page = next_pg
            time.sleep(DELAY_SECONDS)

        if browser:
            browser.close()

    all_stamps = deduplicate(all_stamps)
    atomic_json_write(output_file, build_output(country, all_stamps, current_page))
    log.info("=== Done: %d stamps → %s ===", len(all_stamps), output_file)



# ---------------------------------------------------------------------------
# Rescrape a single group
# ---------------------------------------------------------------------------

def rescrape_group(country, group_id):
    """Re-fetch the page containing group_id, update only those stamps in the JSON."""
    if country not in COUNTRIES:
        log.error("Unknown country '%s'", country)
        return

    config = COUNTRIES[country]
    output_file, progress_file, images_dir = paths_for_country(country)

    if not os.path.exists(output_file):
        log.error("No existing data at %s", output_file)
        return

    with open(output_file, encoding="utf-8") as f:
        data = json.load(f)
    all_stamps = data.get("stamps", [])

    # Find a detail_url from the group to determine which page to load
    group_stamps = [s for s in all_stamps if s.get("group_id") == group_id]
    if not group_stamps:
        log.error("group_id %s not found in %s", group_id, output_file)
        return

    # Extract page number from detail_url e.g. /stamps/Great-Britain/Postage-stamps/g0175/#0179
    page_num = None
    for s in group_stamps:
        detail = s.get("detail_url", "")
        m = re.search(r'/g(\d+)/', detail)
        if m:
            # StampWorld group page numbers don't map 1:1 to pagination pages
            # Use the progress file to find which scraper page had this group
            break

    # Scan progress to find the page — search pages around the stamp numbers
    # Fall back to scanning all pages if needed
    first_stamp_num = group_stamps[0].get("number", "")
    m = re.match(r'^(\d+)', first_stamp_num)
    stamp_num_int = int(m.group(1)) if m else 0
    # Rough estimate: ~40 stamps/page
    estimated_page = max(1, stamp_num_int // 40)
    search_pages = list(range(max(1, estimated_page - 3), estimated_page + 5))

    log.info("Searching for group %s around pages %s", group_id, search_pages)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 Chrome/120 Safari/537.36"
        )
        page = context.new_page()

        found_page = None
        for pg in search_pages:
            url = f"{BASE_URL}{config['collection_path']}?user={USER_ID}&page={pg}"
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_selector("tr.stamp_tr", timeout=15000)
            except Exception as e:
                log.warning("Page %d load error: %s", pg, e)
                continue
            soup = BeautifulSoup(page.content(), "html.parser")
            if soup.find(id=f"group_box_{group_id}"):
                found_page = pg
                log.info("Found group %s on page %d", group_id, pg)
                break

        if found_page is None:
            log.error("Could not find group %s in pages %s", group_id, search_pages)
            browser.close()
            return

        # Parse the full page but only keep stamps from our group
        col_map = build_column_map(BeautifulSoup(page.content(), "html.parser"))
        if not col_map:
            col_map = FALLBACK_COLUMN_MAP.copy()

        new_stamps, _, _ = parse_page(page.content(), country, images_dir, col_map)
        new_group = [s for s in new_stamps if s.get("group_id") == group_id]
        log.info("Re-scraped %d stamps for group %s", len(new_group), group_id)

        browser.close()

    if not new_group:
        log.error("No stamps found for group %s after rescrape", group_id)
        return

    # Replace old group stamps with new ones
    new_group_ids = {s["sw_id"] for s in new_group}
    kept = [s for s in all_stamps if s.get("group_id") != group_id]
    # Preserve ordering: insert new group at the position of the first old stamp
    insert_pos = next((i for i, s in enumerate(all_stamps) if s.get("group_id") == group_id), len(kept))
    updated = kept[:insert_pos] + new_group + kept[insert_pos:]

    atomic_json_write(output_file, build_output(country, updated, data.get("metadata", {}).get("total_pages", 0)))
    log.info("Updated %s with %d stamps for group %s", output_file, len(new_group), group_id)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="StampWorld.com scraper")
    parser.add_argument("--country", default="Malta",
                        help="Country to scrape (default: Malta)")
    parser.add_argument("--reset", action="store_true",
                        help="Wipe progress and re-scrape")
    parser.add_argument("--max-pages", type=int, default=None,
                        help="Stop after N pages (for testing)")
    parser.add_argument("--rescrape-group", type=str, default=None,
                        help="Rescrape a single group_id and update JSON in place")
    args = parser.parse_args()
    if args.rescrape_group:
        rescrape_group(args.country, args.rescrape_group)
    else:
        scrape_country(args.country, reset=args.reset, max_pages=args.max_pages)


if __name__ == "__main__":
    main()

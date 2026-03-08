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
    slug = country.replace(" ", "_")
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
                # Fallback: try to extract type from filename (e.g. /CJ-s.jpg)
                fn_match = re.search(r'/([A-Z][A-Za-z0-9]*)-s\.', src)
                if fn_match:
                    img_map[(gid, fn_match.group(1))] = urljoin(BASE_URL, src)
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
    args = parser.parse_args()
    scrape_country(args.country, reset=args.reset, max_pages=args.max_pages)


if __name__ == "__main__":
    main()

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

from territories import TERRITORIES
from config import SCRAPER_DELAY, SCRAPER_MAX_RETRIES, BROWSER_RESTART_EVERY

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
# Country registry — imported from territories.py
# ---------------------------------------------------------------------------
# TERRITORIES contains all country/territory data with collection paths

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_URL = "https://www.stampworld.com"
USER_ID = os.environ.get("STAMPWORLD_USER_ID", "694157")
IMAGES_ROOT = "stamp_images"
OUTPUT_ROOT = "output"
# SCRAPER_DELAY, SCRAPER_MAX_RETRIES, and BROWSER_RESTART_EVERY are imported from config.py
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
    """Return file paths for a given country slug.

    Args:
        country: StampWorld slug (e.g., "Great-Britain", "Malta")

    Returns:
        tuple: (output_file, progress_file, images_dir)
    """
    # Validate country exists in TERRITORIES
    if country not in TERRITORIES:
        raise KeyError(f"Unknown country '{country}'. Available: {list(TERRITORIES.keys())}")

    # Get territory data (though we don't need collection_path/year_range here)
    territory = TERRITORIES[country]

    # Convert slug to filename-safe version:
    # - Keep hyphens as-is (actual files have hyphens, not underscores)
    # - Convert to lowercase
    # - Note: Some special cases like "St.-Pierre-et-Miquelon" become "st._pierre_et_miquelon"
    #   but that's handled by the existing replace("-", "_") logic for slugs with periods
    filename_slug = country.lower()

    # Special handling for slugs with periods (like "St.-Pierre-et-Miquelon")
    # These should have hyphens converted to underscores in filenames
    if "." in country:
        filename_slug = filename_slug.replace("-", "_")

    images_dir = os.path.join(IMAGES_ROOT, country)
    output_file = os.path.join(OUTPUT_ROOT, f"stamps_{filename_slug}.json")
    progress_file = os.path.join(OUTPUT_ROOT, f"progress_{filename_slug}.json")
    os.makedirs(images_dir, exist_ok=True)
    os.makedirs(OUTPUT_ROOT, exist_ok=True)
    return output_file, progress_file, images_dir



def download_image(url: str, filepath: str, retries: int = SCRAPER_MAX_RETRIES):
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
    if country not in TERRITORIES:
        log.error("Unknown country '%s'. Available: %s", country, list(TERRITORIES.keys()))
        return

    config = TERRITORIES[country]
    output_file, progress_file, images_dir = paths_for_country(country)

    if reset:
        for f in (output_file, progress_file):
            if os.path.exists(f):
                os.remove(f)
                log.info("Removed %s", f)

    all_stamps, existing_ids, start_page = load_existing(output_file, progress_file)
    log.info("=== StampWorld Scraper v2 — %s ===", country)

    pages_done = 0
    # BROWSER_RESTART_EVERY is imported from config.py

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
            for attempt in range(1, SCRAPER_MAX_RETRIES + 1):
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    page.wait_for_selector("tr.stamp_tr", timeout=15000)
                    loaded = True
                    break
                except Exception as e:
                    wait = RETRY_BACKOFF ** attempt
                    log.warning("Page %d load fail (%d/%d): %s — retry in %ds",
                                current_page, attempt, SCRAPER_MAX_RETRIES, e, wait)
                    time.sleep(wait)
                    # On last retry, try a fresh browser
                    if attempt == SCRAPER_MAX_RETRIES - 1:
                        log.info("Restarting browser before final retry.")
                        page = fresh_browser()

            if not loaded:
                log.error("Page %d: failed after %d attempts, stopping.", current_page, SCRAPER_MAX_RETRIES)
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
                time.sleep(SCRAPER_DELAY)
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
            time.sleep(SCRAPER_DELAY)

        if browser:
            browser.close()

    all_stamps = deduplicate(all_stamps)
    atomic_json_write(output_file, build_output(country, all_stamps, current_page))
    log.info("=== Done: %d stamps → %s ===", len(all_stamps), output_file)



# ---------------------------------------------------------------------------
# Delta scraping: only scrape missing stamps between available and scraped
# ---------------------------------------------------------------------------

def scrape_delta(country):
    """
    Compare available count (from territories.py) with scraped count.
    If available > scraped, continue scraping from where we left off.
    """
    if country not in TERRITORIES:
        log.error("Unknown country '%s'", country)
        return

    config = TERRITORIES[country]
    available = config.get("available", 0)
    
    output_file, progress_file, images_dir = paths_for_country(country)
    
    if not os.path.exists(output_file):
        log.info("No existing data for %s, doing full scrape instead", country)
        scrape_country(country)
        return
    
    with open(output_file, encoding="utf-8") as f:
        data = json.load(f)
    
    scraped = len(data.get("stamps", []))
    
    if scraped >= available:
        log.info("Already have %d/%d stamps for %s", scraped, available, country)
        return
    
    delta = available - scraped
    log.info("Delta scrape for %s: have %d, need %d more (delta: %d)", 
             country, scraped, available, delta)
    
    # Continue scraping from where we left off
    scrape_country(country, reset=False)


# ---------------------------------------------------------------------------
# Rescrape a single group
# ---------------------------------------------------------------------------

def rescrape_group(country, group_id):
    """Re-fetch the page containing group_id, update only those stamps in the JSON."""
    if country not in TERRITORIES:
        log.error("Unknown country '%s'", country)
        return

    config = TERRITORIES[country]
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
    parser.add_argument("--delta", action="store_true",
                        help="Only scrape missing stamps (available - scraped)")
    args = parser.parse_args()
    if args.rescrape_group:
        rescrape_group(args.country, args.rescrape_group)
    elif args.delta:
        scrape_delta(args.country)
    else:
        scrape_country(args.country, reset=args.reset, max_pages=args.max_pages)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Fetch all territories from the StampWorld sitemap, get stamp counts,
and update territories.py with "available" counts. Also adds missing
territory stubs.

Usage:
    python fetch_available_counts.py              # update all
    python fetch_available_counts.py --dry-run    # print without writing
    python fetch_available_counts.py --slug Malta # single territory
    python fetch_available_counts.py --missing    # only add missing entries
    python fetch_available_counts.py --counts-only # only update existing counts
"""
import argparse
import re
import time

from urllib.request import urlopen, Request
from urllib.error import URLError

BASE_URL = "https://www.stampworld.com"
DELAY = 0.8
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def fetch(url: str) -> str:
    req = Request(url, headers=HEADERS)
    try:
        with urlopen(req, timeout=15) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except URLError as e:
        print(f"  WARN: {url}: {e}")
        return ""


def get_sitemap_entries() -> list[tuple[str, str]]:
    """
    Parse sitemap and return list of (slug, display_name) tuples.
    Slugs come from /en/sitemap/catalogue/<slug>/ links.
    """
    html = fetch(f"{BASE_URL}/en/sitemap/")
    # Links look like: href="/en/sitemap/catalogue/Aaland/">Aaland</a>
    entries = re.findall(r'href="/en/sitemap/catalogue/([^/\"]+)/">([^<]+)<', html)
    return entries  # [(slug, display_name), ...]


def get_stamp_count(slug: str) -> tuple[int, str]:
    """
    Fetch stamp count and year range for a territory slug.
    Returns (count, year_range).
    """
    # Ensure spaces become hyphens in the URL (sitemap slugs may contain spaces)
    url_slug = slug.replace(" ", "-")
    url = f"{BASE_URL}/en/stamps/{url_slug}/Postage%20stamps"
    html = fetch(url)
    if not html:
        return 0, ""

    # "N stamps." pattern
    m = re.search(r'([\d,]+)\s+stamps?\.', html, re.IGNORECASE)
    count = int(m.group(1).replace(",", "")) if m else 0

    # Year range from "(1840 - 2026)" pattern
    yr = re.search(r'\((\d{4})\s*[-–]\s*(\d{4})\)', html)
    year_range = f"{yr.group(1)}-{yr.group(2)}" if yr else ""

    return count, year_range


def patch_territories(results: list[tuple[str, str, int, str, bool]]) -> None:
    """Write available counts (and new stubs) back into territories.py."""
    with open("territories.py", "r", encoding="utf-8") as f:
        src = f.read()

    updated = 0
    added = 0

    for slug, display_name, count, year_range, in_territories in results:
        if count == 0:
            continue

        if in_territories:
            # Find this slug's dict block and update/insert "available"
            pattern = rf'("{re.escape(slug)}":\s*\{{)(.*?)(    \}})'

            def replacer(m, _c=count):
                body = m.group(2)
                if '"available"' in body:
                    body = re.sub(r'"available":\s*\d+', f'"available": {_c}', body)
                else:
                    body = body.rstrip('\n') + f'\n        "available": {_c},\n    '
                return m.group(1) + body + m.group(3)

            new_src = re.sub(pattern, replacer, src, flags=re.DOTALL)
            if new_src != src:
                src = new_src
                updated += 1
        else:
            # Add new stub before the closing brace of TERRITORIES dict
            url_slug = slug.replace(" ", "-")
            collection_path = f"/en/stamps/{url_slug}/Postage%20stamps"
            if year_range:
                collection_path += f"/{year_range}"
            stub = (
                f'    "{url_slug}": {{\n'
                f'        "display_name": "{display_name}",\n'
                f'        "region": "Unknown",\n'
                f'        "collection_path": "{collection_path}",\n'
                f'        "year_range": "{year_range}",\n'
                f'        "available": {count},\n'
                f'    }},\n'
            )
            # Find the closing brace of TERRITORIES (first standalone "}" after "TERRITORIES = {")
            terr_start = src.index("TERRITORIES = {")
            close_pos = src.index("\n}", terr_start)
            src = src[:close_pos] + "\n" + stub + src[close_pos:]
            added += 1

    with open("territories.py", "w", encoding="utf-8") as f:
        f.write(src)

    print(f"\nDone: {updated} updated, {added} new stubs added.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--slug", help="Only process one slug")
    parser.add_argument("--missing", action="store_true", help="Only add missing entries")
    parser.add_argument("--counts-only", action="store_true", help="Only update existing counts")
    args = parser.parse_args()

    import territories as terr
    existing_slugs = set(terr.TERRITORIES.keys())

    if args.slug:
        entries = [(args.slug, args.slug.replace("-", " "))]
    else:
        print("Fetching sitemap...", flush=True)
        entries = get_sitemap_entries()
        print(f"Found {len(entries)} territories on sitemap", flush=True)

    results = []
    for i, (slug, display_name) in enumerate(entries):
        in_territories = slug in existing_slugs

        if args.missing and in_territories:
            continue
        if args.counts_only and not in_territories:
            continue

        existing = terr.TERRITORIES.get(slug, {}).get("available", 0)
        print(f"[{i+1}/{len(entries)}] {slug} (was {existing})...", end=" ", flush=True)

        count, year_range = get_stamp_count(slug)
        print(f"{count}" + (f"  [{year_range}]" if year_range else "  [no year range found]"), flush=True)
        results.append((slug, display_name, count, year_range, in_territories))
        time.sleep(DELAY)

    if args.dry_run:
        print(f"\nDry run — {len(results)} processed, not writing.")
        return

    patch_territories(results)


if __name__ == "__main__":
    main()

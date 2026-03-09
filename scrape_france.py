"""
Batch scrape France and all French territories/sub-issues sequentially.
Skips countries that already have a completed output file.
Run: python scrape_france.py
"""
import subprocess
import sys
import os
import json
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("batch")

FRANCE_COUNTRIES = [
    # Small / historical first
    "Rouad,-Ile",
    "Castellorizo",
    "French-Committee-of-National-Liberation",
    "Conseil-de-LEurope",
    "UNESCO",
    "Andorra-FR",
    "Mayotte",
    "French-Oceania",
    "French-South-and-Antarctic-Terr.",
    "Wallis-and-Futuna-Islands",
    "New-Caledonia",
    "French-Polynesia",
    # Martinique & St. Pierre et Miquelon already in North America list
    # Saar already in Europe list
    # Monaco already in Europe list
    "France",
]

seen = set()
FRANCE_COUNTRIES = [c for c in FRANCE_COUNTRIES if not (c in seen or seen.add(c))]

OUTPUT_DIR = "output"


def is_done(country):
    slugs = [
        country.lower().replace("-", "_").replace(".", "_").replace(",", "_"),
        country.lower().replace(" ", "_"),
    ]
    for slug in slugs:
        path = os.path.join(OUTPUT_DIR, f"stamps_{slug}.json")
        if not os.path.exists(path):
            continue
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            stamps = data.get("stamps", [])
            progress = os.path.join(OUTPUT_DIR, f"progress_{slug}.json")
            if len(stamps) > 0 and not os.path.exists(progress):
                return True
            if os.path.exists(progress):
                return False
            return len(stamps) > 0
        except Exception:
            continue
    return False


def main():
    total = len(FRANCE_COUNTRIES)
    completed = 0
    failed = 0

    for i, country in enumerate(FRANCE_COUNTRIES, 1):
        if is_done(country):
            log.info("[%d/%d] %s — already done, skipping", i, total, country)
            completed += 1
            continue

        log.info("[%d/%d] Starting %s", i, total, country)
        start = time.time()

        try:
            result = subprocess.run(
                [sys.executable, "scraper.py", "--country", country],
                capture_output=False,
                timeout=3600,
            )
            elapsed = time.time() - start
            if result.returncode == 0:
                log.info("[%d/%d] %s done in %.0fs", i, total, country, elapsed)
                completed += 1
            else:
                log.error("[%d/%d] %s FAILED (exit %d)", i, total, country, result.returncode)
                failed += 1
        except subprocess.TimeoutExpired:
            log.error("[%d/%d] %s TIMED OUT", i, total, country)
            failed += 1
        except Exception as e:
            log.error("[%d/%d] %s ERROR: %s", i, total, country, str(e))
            failed += 1

        time.sleep(10)

    log.info("=== Batch complete ===")
    log.info("Completed: %d, Failed: %d, Total: %d", completed, failed, total)


if __name__ == "__main__":
    main()

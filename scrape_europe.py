"""
Batch scrape all European countries sequentially.
Skips countries that already have a completed output file.
Run: python scrape_europe.py
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

# European countries (ordered by estimated size, smaller first)
EUROPE_COUNTRIES = [
    # Very small countries
    "Saar", "Kosovo", "Aaland", "Faroe-Islands", "Liechtenstein",
    "Monaco", "San-Marino", "Vatican-City", "Montenegro", "Slovenia",
    "Luxembourg", "Iceland", "Albania", "Moldova", "Gibraltar",
    "Malta", "Estonia", "Latvia", "Lithuania", "Belarus",
    
    # Medium-sized countries
    "Croatia", "Slovakia", "Ireland", "Denmark", "Norway", "Sweden",
    "Finland", "Switzerland", "Austria", "Belgium", "Netherlands",
    "Portugal", "Greece", "Hungary", "Bulgaria", "Romania", "Serbia",
    "Ukraine", "Czechoslovakia", "Yugoslavia", "DDR", "USSR",
    
    # Larger countries
    "Czech-Republic", "Poland", "Italy", "Spain", "Turkey",
    "France", "Germany", "Russia",
    
    # Additional from REGIONS
    "Aegean Islands", "Alderney", "Andorra ES", "Andorra FR", "Azores",
    "B. Herzegovina", "Cr. Post Mostar", "Cyprus Greek", "Cyprus Turkish",
    "Fiume", "Great Britain", "Greenland", "Guernsey", "Isle of Man",
    "Jersey", "Macedonia", "Madeira", "San Marino", "Serbian Rep. B and H",
    "UN Geneva", "UN Vienna",
]

# Remove any duplicates
seen = set()
EUROPE_COUNTRIES = [c for c in EUROPE_COUNTRIES if not (c in seen or seen.add(c))]

OUTPUT_DIR = "output"


def is_done(country):
    """Check if a country has already been scraped."""
    # Try multiple slug patterns
    slugs = [
        country.lower().replace("-", "_"),
        country.lower().replace(" ", "_"),
        country.lower().replace(" ", "-"),
    ]
    
    for slug in slugs:
        path = os.path.join(OUTPUT_DIR, f"stamps_{slug}.json")
        if not os.path.exists(path):
            continue
        
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            stamps = data.get("stamps", [])
            
            # Check progress file
            progress = os.path.join(OUTPUT_DIR, f"progress_{slug}.json")
            if len(stamps) > 0 and not os.path.exists(progress):
                return True  # Completed scrape, no progress file
            
            # If progress file exists, treat as resumable (not done)
            if os.path.exists(progress):
                return False
                
            return len(stamps) > 0
        except Exception:
            continue
    
    return False


def main():
    total = len(EUROPE_COUNTRIES)
    completed = 0
    failed = 0
    
    for i, country in enumerate(EUROPE_COUNTRIES, 1):
        if is_done(country):
            log.info("[%d/%d] %s — already done, skipping", i, total, country)
            completed += 1
            continue

        log.info("[%d/%d] Starting %s", i, total, country)
        start = time.time()
        
        try:
            result = subprocess.run(
                [sys.executable, "scraper.py", "--country", country],
                capture_output=False,  # stream output live
                timeout=3600,  # 1 hour timeout per country
            )
            elapsed = time.time() - start
            
            if result.returncode == 0:
                log.info("[%d/%d] %s done in %.0fs", i, total, country, elapsed)
                completed += 1
            else:
                log.error("[%d/%d] %s FAILED (exit %d)", i, total, country, result.returncode)
                failed += 1
                
        except subprocess.TimeoutExpired:
            log.error("[%d/%d] %s TIMED OUT after 1 hour", i, total, country)
            failed += 1
        except Exception as e:
            log.error("[%d/%d] %s ERROR: %s", i, total, country, str(e))
            failed += 1

        # Brief pause between countries to avoid rate limiting
        time.sleep(10)
    
    log.info("=== Batch complete ===")
    log.info("Completed: %d, Failed: %d, Total: %d", completed, failed, total)


if __name__ == "__main__":
    main()

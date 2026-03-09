"""
Batch scrape all African countries sequentially.
Skips countries that already have a completed output file.
Run: python scrape_africa.py
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

# African countries (ordered by estimated size, smaller first)
AFRICA_COUNTRIES = [
    # Small countries and islands
    "Ascension", "Mayotte", "Reunion", "St. Helena", "Tristan da Cunha",
    "Cape Verde", "Comoro Islands", "Djibouti", "Equatorial Guinea",
    "Gabon", "Gambia", "Guinea Bissau", "Lesotho", "Mauritius",
    "Sao Tome And Principe", "Seychelles", "Swaziland", "Western Sahara",
    
    # Medium-sized countries
    "Benin", "Botswana", "Burkina Faso", "Burundi", "Cent. African Rep.",
    "Chad", "Congo, Rep.", "Eritrea", "Guinea", "Ivory Coast",
    "Liberia", "Libya", "Malawi", "Mali", "Mauritania", "Namibia",
    "Niger", "Rwanda", "Senegal", "Sierra Leone", "Somalia",
    "South Sudan", "Sudan", "Togo", "Tunisia", "Uganda", "Zambia",
    "Zimbabwe",
    
    # Larger countries
    "Algeria", "Angola", "Cameroon", "Congo, Dr", "Egypt", "Ethiopia",
    "Ghana", "Kenya", "Madagascar", "Morocco", "Mozambique",
    "Nigeria", "South Africa", "Tanzania",
]

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
    total = len(AFRICA_COUNTRIES)
    completed = 0
    failed = 0
    
    for i, country in enumerate(AFRICA_COUNTRIES, 1):
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
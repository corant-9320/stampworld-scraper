"""Quick sanity check on scraped output — works with both old and new format."""

import json
import sys
from collections import Counter

path = sys.argv[1] if len(sys.argv) > 1 else "output/stamps_malta.json"

with open(path, encoding="utf-8") as f:
    raw = json.load(f)

# Handle both old (list) and new (dict with metadata) formats
if isinstance(raw, dict):
    meta = raw.get("metadata", {})
    data = raw.get("stamps", [])
    print(f"Source: {meta.get('source')}  Country: {meta.get('country')}")
    print(f"Scraped: {meta.get('scraped_at')}  Pages: {meta.get('total_pages')}")
    print(f"Version: {meta.get('scraper_version')}")
    print()
else:
    data = raw

imgs = [s for s in data if s.get("image_url")]
urls = [s["image_url"] for s in imgs]
dupes = {u: c for u, c in Counter(urls).items() if c > 1}

print(f"Stamps: {len(data)}")
print(f"With images: {len(imgs)}")
print(f"Unique image URLs: {len(set(urls))}")
print(f"Duplicate image URLs: {len(dupes)}")

# Check for missing fields
id_field = "sw_id" if data and "sw_id" in data[0] else "id"
missing_id = sum(1 for s in data if not s.get(id_field))
missing_denom = sum(1 for s in data if not s.get("denomination"))
missing_img = sum(1 for s in data if not s.get("image_url"))
print(f"Missing {id_field}: {missing_id}")
print(f"Missing denomination: {missing_denom}")
print(f"Missing image_url: {missing_img}")

print(f"\nFirst 5 stamps:")
for s in data[:5]:
    print(f"  {s.get(id_field, '?')} #{s.get('number','')} type={s.get('type','')} → {s.get('image_url', 'NO IMAGE')[:60]}")

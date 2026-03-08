"""Check that column data is being parsed correctly."""

import json
import sys

path = sys.argv[1] if len(sys.argv) > 1 else "output/stamps_malta.json"

with open(path, encoding="utf-8") as f:
    raw = json.load(f)

data = raw.get("stamps", raw) if isinstance(raw, dict) else raw
print(f"Total stamps: {len(data)}\n")

# Show a stamp with colour + quantity populated
for s in data:
    if s.get("colour") and s.get("quantity"):
        print("Stamp with colour + quantity:")
        print(json.dumps(s, indent=2))
        break

# Show a stamp with perforations
for s in data:
    if s.get("perforations"):
        print("\nStamp with perforations:")
        print(json.dumps(s, indent=2))
        break

# Field coverage summary
fields = ["denomination", "perforations", "colour", "paper", "watermark",
          "quantity", "price_mint_nh", "price_unused", "price_used",
          "price_on_cover", "currency", "image_url", "local_image"]
print("\nField coverage:")
for f in fields:
    count = sum(1 for s in data if s.get(f) and s[f] not in ("", "-"))
    print(f"  {f:20s}: {count:4d}/{len(data)} ({count/len(data)*100:.0f}%)")

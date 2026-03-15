# Refactoring Plan

Reviewed 2026-03-09. These are ordered by impact — do them top-down.

---

## 1. Consolidate batch scrapers into shared module

**Problem**: `scrape_africa.py`, `scrape_asia.py`, `scrape_europe.py`, `scrape_france.py`, `scrape_north_america.py`, `scrape_oceania.py`, `scrape_south_america.py` are 90% identical. Each duplicates `is_done()`, the subprocess loop, logging setup, and progress checking. Only the country list differs. ~500 lines of copy-paste.

**Fix**: Create `batch_scraper.py` with:
```python
def is_done(country, output_dir="output") -> bool: ...
def run_batch(countries: list, label: str, output_dir="output"): ...
```

Then each regional file becomes:
```python
from batch_scraper import run_batch
AFRICA = ["Algeria", "Angola", ...]
if __name__ == "__main__":
    run_batch(AFRICA, "Africa")
```

**Files affected**: All 7 `scrape_*.py` files → reduced to ~10 lines each. New `batch_scraper.py` (~80 lines).

---

## 2. Single source of truth for territories

**Problem**: Country/region data is defined in 4 separate places that aren't synchronised:
- `scraper.py` → `COUNTRIES` dict (StampWorld slugs + collection paths, ~150 entries)
- `viewer.py` → `REGIONS` dict (display names grouped by region, ~200 entries)
- Each `scrape_*.py` → hardcoded country lists (display names, different format)
- `matcher/stamp_text_countries.py` → OCR inscription-to-slug mappings

Adding a new country currently requires editing 3-4 files. Display names use spaces (`Great Britain`), scraper keys use hyphens (`Great-Britain`), and the viewer has helper functions to convert between them.

**Fix**: Create `territories.py` as the master registry:
```python
TERRITORIES = {
    "Great-Britain": {
        "display_name": "Great Britain",
        "region": "Europe",
        "collection_path": "/en/stamps/Great-Britain/Postage%20stamps/1840-2026",
        "year_range": "1840-2026",
    },
    ...
}

def get_by_region(region: str) -> list[str]: ...
def slug_to_display(slug: str) -> str: ...
def display_to_slug(name: str) -> str: ...
```

Then:
- `scraper.py` imports `TERRITORIES` instead of defining `COUNTRIES`
- `viewer.py` imports region groupings instead of defining `REGIONS`
- `scrape_*.py` files import country lists by region
- Slug ↔ display name conversion is centralised

**Files affected**: `scraper.py`, `viewer.py`, all `scrape_*.py`, `territory_catalog.py` (replace with this).

**Note**: `territory_catalog.py` already exists but is incomplete/abandoned. Replace it with this.

---

## 3. Consolidate index builders

**Problem**: Three build scripts with identical boilerplate:
- `build_index.py` (v1 ORB — abandoned, but still in repo)
- `build_index_v2.py` (histogram/hash)
- `build_index_v3.py` (CNN embeddings)

Each independently: loads all `stamps_*.json` files, filters by `--country`, validates image paths exist, reports progress, saves to `descriptor_index/`. Only the feature extraction step differs.

**Fix**: Extract shared logic into `matcher/index_builder.py`:
```python
def load_stamps(output_dir, country_filter=None) -> list[tuple[dict, str]]:
    """Load stamp records, filter by country, validate image paths."""
    ...

def build_with_progress(stamps, extract_fn, save_fn, batch_size=1):
    """Generic build loop with progress reporting."""
    ...
```

Then each build script becomes a thin wrapper calling the shared loader + its specific extraction function.

**Also**: Delete `build_index.py` (v1 ORB) — it's dead code. The ORB approach was abandoned.

**Files affected**: `build_index_v2.py`, `build_index_v3.py` → simplified. New `matcher/index_builder.py`. Delete `build_index.py`.

---

## 4. Common interface for matcher indices

**Problem**: Three index classes with different APIs:

| Class | Add method | Query method | Record type | Return type |
|-------|-----------|-------------|-------------|-------------|
| `DescriptorStore` | `add_image(record, descriptors)` | N/A (uses FLANNMatcher) | `ImageRecord` | `MatchResult` |
| `HistogramIndex` | `add(record, features)` | `query(features, top_k, country)` | `IndexRecord` | `list[dict]` |
| `CNNIndex` | `add(record, embedding)` | `query(embedding, top_k, country, ...)` | `IndexRecord` | `list[dict]` |

`viewer.py` uses `isinstance()` checks to branch between v2 and v3 paths. Each path has different preprocessing (histogram bins, OCR, colour hist computation).

**Fix**: Define a base class in `matcher/base_index.py`:
```python
class BaseIndex(ABC):
    @abstractmethod
    def query(self, image_bytes: bytes, top_k: int, country: str = None) -> list[dict]: ...
    @abstractmethod
    def save(self, directory: str): ...
    @classmethod
    @abstractmethod
    def load(cls, directory: str) -> "BaseIndex": ...
```

Each index handles its own preprocessing internally (CNN does embedding + colour hist + OCR; histogram does its own feature extraction). The viewer just calls `index.query(image_bytes, top_k, country)` without caring which version.

**Files affected**: `matcher/cnn_matcher.py`, `matcher/histogram_matcher.py`, `viewer.py` (simplified match endpoint).

**Also**: Delete `matcher/descriptor_store.py` and `matcher/flann_matcher.py` — dead code from abandoned ORB approach.

---

## 5. Centralise configuration

**Problem**: Magic numbers scattered across files:

| Constant | Location | Value |
|----------|----------|-------|
| CNN floor/ceiling | `matcher/cnn_matcher.py` | 0.75 / 0.95 |
| Signal weights | `matcher/cnn_matcher.py` query() defaults | 0.40 / 0.35 / 0.10 / 0.15 |
| Confidence sigmoid | `matcher/cnn_matcher.py` | `12 * (raw - 0.45)` |
| HSV histogram bins | `matcher/cnn_matcher.py` `_get_color_hist()` | (36, 12, 12) |
| HSV histogram bins | `matcher/histogram_matcher.py` | (18, 3, 3) |
| HSV histogram bins | `viewer.py` match endpoint | (36, 12, 12) |
| Batch size | `build_index_v3.py` | 32 |
| Scraper delay | `scraper.py` | 1.5s |
| Browser restart interval | `scraper.py` | every 20 pages |
| Max retries | `scraper.py` | 3 |
| Image resize target | `matcher/preprocess.py` | 512 |
| Image resize target | `build_index_v2.py` | 256 |
| CNN input size | `matcher/cnn_matcher.py` | 224 |

Note the histogram bins mismatch: v2 index uses (18,3,3) but the CNN re-ranker and viewer both use (36,12,12). This means the v2 index and the CNN colour re-ranking use different bin counts.

**Fix**: Create `config.py`:
```python
# Scraper
SCRAPER_DELAY = 1.5
SCRAPER_MAX_RETRIES = 3
BROWSER_RESTART_EVERY = 20

# Matching
CNN_FLOOR = 0.75
CNN_CEIL = 0.95
SIGNAL_WEIGHTS = {"cnn": 0.40, "color": 0.35, "text": 0.10, "country": 0.15}
CONFIDENCE_SIGMOID_SCALE = 12
CONFIDENCE_SIGMOID_CENTER = 0.45
HSV_BINS = (36, 12, 12)
CNN_BATCH_SIZE = 32

# Paths
OUTPUT_DIR = "output"
IMAGES_DIR = "stamp_images"
INDEX_DIR = "descriptor_index"
```

**Files affected**: Everything that uses these constants.

---

## 6. Clean up dead code

**Problem**: Several files are dead/abandoned/empty:

| File | Status | Action |
|------|--------|--------|
| `build_index.py` | v1 ORB builder, abandoned | Delete |
| `matcher/descriptor_store.py` | v1 ORB store, abandoned | Delete |
| `matcher/flann_matcher.py` | v1 ORB matcher, abandoned | Delete |
| `matcher/orb_extractor.py` | v1 ORB extractor, abandoned | Delete |
| `matcher/preprocess.py` | Only used by v1 ORB | Delete |
| `scrape_all.py` | Empty file | Delete |
| `discover_countries.py` | Empty file | Delete |
| `check_columns.py` | One-off utility | Move to `tools/` |
| `check_results.py` | One-off utility | Move to `tools/` |
| `territory_catalog.py` | Incomplete, replaced by #2 | Delete after #2 |

**Impact**: 5 dead matcher files (~400 lines), 3 empty/abandoned files. Removing them reduces confusion about what's active.

---

## 7. viewer.py REGIONS uses display names, not slugs

**Problem**: The `REGIONS` dict in `viewer.py` uses display names with spaces (`"Great Britain"`, `"Czech Republic"`). The scraped JSON data uses StampWorld slugs with hyphens (`"Great-Britain"`, `"Czech-Republic"`). Two helper functions paper over this:

```python
def country_to_slug(name):     # "Great Britain" → "Great_Britain"
def country_to_sw_slug(name):  # "Great Britain" → "Great-Britain"
```

But `load_scraped_data()` tries 3 different filename patterns to find the JSON file, which is fragile. Some names have dots (`"St. Kitts"`, `"Congo, Rep."`) that don't convert cleanly.

**Fix**: This is solved by #2 (territories.py). Once there's a single registry with both display names and slugs, the viewer imports the mapping and does exact lookups instead of guessing filename patterns.

---

## Summary: file changes after all refactoring

**New files**:
- `config.py` — all constants
- `territories.py` — master country/region registry (replaces `territory_catalog.py`)
- `batch_scraper.py` — shared batch scraping logic
- `matcher/base_index.py` — abstract index interface
- `matcher/index_builder.py` — shared stamp loading for index builders
- `tools/` directory for utility scripts

**Deleted files**:
- `build_index.py` (dead v1)
- `matcher/descriptor_store.py` (dead v1)
- `matcher/flann_matcher.py` (dead v1)
- `matcher/orb_extractor.py` (dead v1)
- `matcher/preprocess.py` (dead v1)
- `scrape_all.py` (empty)
- `discover_countries.py` (empty)
- `territory_catalog.py` (replaced by `territories.py`)

**Simplified files**:
- All `scrape_*.py` → ~10 lines each
- `build_index_v2.py`, `build_index_v3.py` → thin wrappers
- `viewer.py` → no isinstance branching, no REGIONS dict, no slug guessing
- `scraper.py` → no COUNTRIES dict (imported from territories.py)

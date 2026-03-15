"""
Stamp matcher viewer — match uploaded stamps against the index.
Run: python viewer.py
Then open http://localhost:5000
"""
import glob
import json
import os
import time
from collections import defaultdict
from datetime import datetime
from flask import Flask, send_from_directory, jsonify, request

app = Flask(__name__, static_folder="viewer_static")

from config import OUTPUT_DIR, IMAGES_DIR, INDEX_DIR

# Global cache for descriptor store and matcher
_histogram_index = None
_store_loaded_at = None


def load_scraped_data(country_slug):
    """Load scraped JSON for a country. Returns dict or None."""
    slug_lower = country_slug.lower()
    candidates = [
        f"stamps_{slug_lower}.json",
        f"stamps_{slug_lower.replace('-', '_')}.json",
    ]
    for filename in candidates:
        filepath = os.path.join(OUTPUT_DIR, filename)
        if os.path.exists(filepath):
            with open(filepath, encoding="utf-8") as f:
                return json.load(f)
    return None


@app.route("/")
def index():
    return send_from_directory("viewer_static", "index.html")


@app.route("/api/country/<slug>")
def api_country(slug):
    """Return all stamps for a country."""
    data = load_scraped_data(slug)
    if not data:
        return jsonify({"error": "Not found"}), 404
    stamps = data.get("stamps", data) if isinstance(data, dict) else data
    meta = data.get("metadata", {}) if isinstance(data, dict) else {}
    return jsonify({"metadata": meta, "stamps": stamps})


@app.route("/api/data-overview")
def api_data_overview():
    """Return flat territory list with scraped/indexed/available counts."""
    try:
        import territories
    except ImportError:
        return jsonify({"territories": []})

    indexed_by_country = defaultdict(int)
    for manifest_name in ("manifest_v3.json", "manifest_v2.json"):
        manifest_path = os.path.join(INDEX_DIR, manifest_name)
        if os.path.exists(manifest_path):
            with open(manifest_path, encoding="utf-8") as f:
                manifest = json.load(f)
            for entry in manifest.get("entries", []):
                c = entry.get("country", "")
                if c:
                    indexed_by_country[c] += 1
            break

    result = []
    for slug, data in territories.TERRITORIES.items():
        scraped_data = load_scraped_data(slug)
        scraped_count = len(scraped_data.get("stamps", [])) if scraped_data else 0
        result.append({
            "slug": slug,
            "display_name": data["display_name"],
            "region": data.get("region", "Unknown"),
            "scraped": scraped_count,
            "indexed": indexed_by_country.get(slug, 0),
            "available": data.get("available", 0),
        })

    result = [t for t in result if t["available"] > 0]
    result.sort(key=lambda x: x["display_name"])
    return jsonify({"territories": result})


def enrich_matches(matches, index=None):
    """Add full stamp metadata to match results."""
    lookup = {}
    for jp in glob.glob(os.path.join(OUTPUT_DIR, "stamps_*.json")):
        with open(jp, encoding="utf-8") as f:
            data = json.load(f)
        for s in data.get("stamps", []):
            key = s.get("local_image", "").replace("\\", "/")
            if key:
                lookup[key] = s

    for m in matches:
        key = m.get("local_image", "").replace("\\", "/")
        stamp = lookup.get(key, {})
        m["colour"] = stamp.get("colour", "")
        m["denomination"] = stamp.get("denomination", "")
        m["perforations"] = stamp.get("perforations", "")
        m["watermark"] = stamp.get("watermark", "")
        m["paper"] = stamp.get("paper", "")
        m["catalogue_type"] = stamp.get("catalogue_type", "")
        m["price_mint_nh"] = stamp.get("price_mint_nh", "")
        m["price_unused"] = stamp.get("price_unused", "")
        m["price_used"] = stamp.get("price_used", "")
        m["price_on_cover"] = stamp.get("price_on_cover", "")
        m["currency"] = stamp.get("currency", "")
    return matches


def load_descriptor_store():
    """Load v3 CNN index from disk, falling back to v2 histogram index."""
    global _histogram_index, _store_loaded_at

    if _histogram_index is not None:
        return _histogram_index

    try:
        v3_manifest = os.path.join(INDEX_DIR, "manifest_v3.json")
        if os.path.exists(v3_manifest):
            from matcher.cnn_matcher import CNNIndex
            print(f"Loading v3 CNN index from {INDEX_DIR}...")
            _histogram_index = CNNIndex.load(INDEX_DIR)
            _store_loaded_at = datetime.utcnow()
            print(f"Loaded {len(_histogram_index)} images (CNN v3)")
            return _histogram_index

        v2_manifest = os.path.join(INDEX_DIR, "manifest_v2.json")
        if os.path.exists(v2_manifest):
            from matcher.histogram_matcher import HistogramIndex
            print(f"Loading v2 histogram index from {INDEX_DIR}...")
            _histogram_index = HistogramIndex.load(INDEX_DIR)
            _store_loaded_at = datetime.utcnow()
            print(f"Loaded {len(_histogram_index)} images (histogram v2)")
            return _histogram_index

        raise RuntimeError("No index found. Run: python build_index_v3.py")

    except ImportError as e:
        raise RuntimeError(f"Matcher package not available: {e}")
    except Exception as e:
        raise RuntimeError(f"Failed to load index: {e}")


@app.route("/api/match", methods=["POST"])
def api_match():
    """Match an uploaded stamp image against the reference index."""
    start_time = time.time()

    if "image" not in request.files:
        return jsonify({"error": "No image file provided"}), 400

    image_file = request.files["image"]
    if image_file.filename == "":
        return jsonify({"error": "No selected file"}), 400

    image_file.seek(0, 2)
    file_size = image_file.tell()
    image_file.seek(0)
    if file_size > 10 * 1024 * 1024:
        return jsonify({"error": "File too large (max 10MB)"}), 400

    allowed_extensions = {".jpg", ".jpeg", ".png"}
    file_ext = os.path.splitext(image_file.filename.lower())[1]
    if file_ext not in allowed_extensions:
        return jsonify({"error": "Invalid file type. Upload JPEG or PNG"}), 400

    country_filter = request.form.get("country")
    try:
        top_k = int(request.form.get("top_k", 10))
    except ValueError:
        top_k = 10

    try:
        index = load_descriptor_store()
    except RuntimeError as e:
        return jsonify({"error": str(e)}), 503

    image_bytes = image_file.read()

    try:
        matches = index.query(image_bytes, top_k=top_k, country=country_filter)
        matches = enrich_matches(matches, index=index)
        elapsed_ms = int((time.time() - start_time) * 1000)

        return jsonify({
            "matches": matches,
            "elapsed_ms": elapsed_ms,
        })
    except Exception as e:
        return jsonify({"error": f"Matching failed: {e}"}), 500


@app.route("/api/match/index-status")
def api_index_status():
    """Return current index stats."""
    try:
        index = load_descriptor_store()

        manifest_path = os.path.join(INDEX_DIR, "manifest_v2.json")
        built_at = None
        if os.path.exists(manifest_path):
            with open(manifest_path, "r", encoding="utf-8") as f:
                manifest = json.load(f)
            built_at = manifest.get("built_at")

        countries = sorted(set(r.country for r in index.records))

        return jsonify({
            "total_images": len(index),
            "total_descriptors": 0,
            "countries": countries,
            "built_at": built_at,
            "loaded_at": _store_loaded_at.isoformat() + "Z" if _store_loaded_at else None,
        })
    except RuntimeError as e:
        return jsonify({"error": str(e)}), 503
    except Exception as e:
        return jsonify({"error": f"Failed to get index status: {e}"}), 500


@app.route("/images/<path:filepath>")
def serve_image(filepath):
    return send_from_directory(IMAGES_DIR, filepath)


if __name__ == "__main__":
    print("Stamp Matcher: http://localhost:5000")
    app.run(debug=True, port=5000, threaded=True)

"""
Stamp collection viewer — browse scraped data, see what's done vs. remaining.
Run: python viewer.py
Then open http://localhost:5000
"""
import glob
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from flask import Flask, send_from_directory, jsonify, request

app = Flask(__name__, static_folder="viewer_static")

OUTPUT_DIR = "output"
IMAGES_DIR = "stamp_images"
INDEX_DIR = "descriptor_index"

# Global cache for descriptor store and matcher
_histogram_index = None
_store_loaded_at = None

# Country lists by region
REGIONS = {
    "British Territories": [
        "Great Britain",
        "Alderney", "British Antarctic", "Gibraltar", "Guernsey", "Isle of Man", "Jersey",
        "British Military Administration", "British Colonies",
        "British Post Offices Abroad", "British South Africa Company",
        "Bushir", "Straits Settlements",
    ],
    "Europe": [
        "Aaland", "Aegean Islands", "Albania", "Alderney", "Andorra ES", "Andorra FR",
        "Austria", "Azores", "Belarus", "Belgium", "B. Herzegovina", "Bulgaria",
        "Croatia", "Cr. Post Mostar", "Cyprus Greek", "Cyprus Turkish",
        "Czech Republic", "Czechoslovakia", "DDR", "Denmark", "Estonia",
        "Faroe Islands", "Finland", "Fiume", "France", "Germany", "Gibraltar",
        "Great Britain", "Greece", "Greenland", "Guernsey", "Hungary", "Iceland",
        "Ireland", "Isle of Man", "Italy", "Jersey", "Kosovo", "Latvia",
        "Liechtenstein", "Lithuania", "Luxembourg", "Macedonia", "Madeira",
        "Malta", "Moldova", "Monaco", "Montenegro", "Netherlands", "Norway",
        "Poland", "Portugal", "Romania", "Russia", "Saar", "San Marino",
        "Serbia", "Serbian Rep. B and H", "Slovakia", "Slovenia", "Spain",
        "Sweden", "Switzerland", "Turkey", "Ukraine", "UN Geneva", "UN Vienna",
        "USSR", "Vatican City", "Yugoslavia",
    ],
    "Americas": [
        "Anguilla", "Antigua and Barb", "Aruba", "Bahamas", "Barbados", "Belize",
        "Bermuda", "Br. Virgin Islands", "Canada", "Cayman Islands", "Costa Rica",
        "Cuba", "Curacao", "Danish West Indies", "Dominica", "Dominican Rep.",
        "El Salvador", "Grenada", "Grenada Grenadines", "Guadeloupe", "Guatemala",
        "Haiti", "Honduras", "Jamaica", "Leeward Islands", "Martinique", "Mexico",
        "Montserrat", "Netherlands Antilles", "Netherlands Caribbean", "Nevis",
        "Nicaragua", "Panama", "Puerto Rico", "Sint Maartín", "St. Kitts",
        "St. Lucia", "St. Pierre et Miquelon", "St. Vincent And The Grenadines",
        "Trinidad And Tobago", "Turks And Caicos Islands", "UN New York",
        "United States",
    ],
    "South America": [
        "Argentina", "Bolivia", "Brazil", "British Antarctic", "Chile", "Colombia",
        "Ecuador", "Falkland Islands", "French Guyana", "French South and Antarctic Terr.",
        "Guyana", "Paraguay", "Peru", "Suriname", "Uruguay", "Venezuela",
    ],
    "Asia": [
        "Afghanistan", "Armenia", "Azerbaijan", "Bahrain", "Bangladesh", "Bhutan",
        "Br. Indian Ocean", "Brunei", "Cambodia", "P. R. China", "Georgia",
        "Hong Kong", "India", "Indochina", "Indonesia", "Iran", "Iraq", "Israel",
        "Japan", "Japan 2020-Present", "Jordan", "Kazakhstan", "Kuwait",
        "Kyrgyzstan", "Laos", "Lebanon", "Macau", "Malaysia", "Maldives",
        "Mongolia", "Myanmar", "Nepal", "North Korea", "Oman", "Pakistan",
        "Palestine", "Philippines", "Qatar", "Saudi Arabia", "Singapore",
        "South Korea", "Sri Lanka", "Syria", "Taiwan", "Tajikistan", "Thailand",
        "Timor Leste", "Turkmenistan", "United Arab Emirates", "Uzbekistan",
        "Vietnam", "Yemen",
    ],
    "Oceania": [
        "Aitutaki", "Australia", "Aus. Antarctic", "Christmas Island", "Cocos Islands",
        "Cook Islands", "Fiji", "French Oceania", "French Polynesia", "Guam",
        "Kiribati", "Marshall Islands", "Micronesia", "Nauru", "New Caledonia",
        "New Zealand", "Niuafoou", "Niue", "Norfolk Island", "Palau",
        "Papua New Guinea", "Penrhyn Island", "Pitcairn Islands", "Ross Dependency",
        "Samoa", "Solomon Islands", "Tokelau Islands", "Tonga", "Tuvalu",
        "Vanuatu", "Wallis and Futuna Islands",
    ],
    "Africa": [
        "Algeria", "Angola", "Ascension", "Benin", "Botswana", "Burkina Faso",
        "Burundi", "Cameroon", "Cape Verde", "Cent. African Rep.", "Chad",
        "Comoro Islands", "Congo, Dr", "Congo, Rep.", "Djibouti", "Egypt",
        "Equatorial Guinea", "Eritrea", "Ethiopia", "Gabon", "Gambia", "Ghana",
        "Guinea", "Guinea Bissau", "Ivory Coast", "Kenya", "Lesotho", "Liberia",
        "Libya", "Madagascar", "Malawi", "Mali", "Mauritania", "Mauritius",
        "Mayotte", "Morocco", "Mozambique", "Namibia", "Niger", "Nigeria",
        "Reunion", "Rwanda", "Sao Tome And Principe", "Senegal", "Seychelles",
        "Sierra Leone", "Somalia", "South Africa", "South Sudan", "St. Helena",
        "Sudan", "Swaziland", "Tanzania", "Togo", "Tristan da Cunha", "Tunisia",
        "Uganda", "Western Sahara", "Zambia", "Zimbabwe",
    ],
}

# Flat list for backward compat
ALL_COUNTRIES = [c for region in REGIONS.values() for c in region]


def country_to_slug(name):
    """Convert display name to the slug used in filenames."""
    return name.replace(" ", "_")


def country_to_sw_slug(name):
    """Convert display name to StampWorld URL slug."""
    return name.replace(" ", "-")


def load_scraped_data(country_slug):
    """Load scraped JSON for a country. Returns dict or None."""
    # Try various filename patterns
    for pattern in [
        os.path.join(OUTPUT_DIR, f"stamps_{country_slug.lower()}.json"),
        os.path.join(OUTPUT_DIR, f"stamps_{country_slug.lower().replace('-', '_')}.json"),
        os.path.join(OUTPUT_DIR, f"stamps_{country_slug.lower().replace('_', '-')}.json"),
    ]:
        if os.path.exists(pattern):
            with open(pattern, encoding="utf-8") as f:
                return json.load(f)
    return None


@app.route("/")
def index():
    return send_from_directory("viewer_static", "index.html")


@app.route("/api/overview")
def api_overview():
    """Summary of all countries by region — scraped vs. remaining."""
    result = {}

    for region, countries in REGIONS.items():
        scraped = []
        remaining = []
        for name in countries:
            slug = country_to_slug(name)
            sw_slug = country_to_sw_slug(name)
            data = load_scraped_data(slug) or load_scraped_data(sw_slug)

            if data and isinstance(data, dict) and data.get("stamps"):
                meta = data.get("metadata", {})
                stamps = data["stamps"]
                with_images = sum(1 for s in stamps if s.get("local_image"))
                scraped.append({
                    "name": name,
                    "slug": sw_slug,
                    "total_stamps": len(stamps),
                    "total_images": with_images,
                    "pages_scraped": meta.get("total_pages"),
                    "scraped_at": meta.get("scraped_at"),
                })
            else:
                remaining.append({
                    "name": name,
                    "slug": sw_slug,
                    "sw_url": f"https://www.stampworld.com/en/stamps/{sw_slug}/",
                })

        result[region] = {"scraped": scraped, "remaining": remaining}

    return jsonify(result)


@app.route("/api/country/<slug>")
def api_country(slug):
    """Return all stamps for a country."""
    data = load_scraped_data(slug) or load_scraped_data(slug.replace("-", "_"))
    if not data:
        return jsonify({"error": "Not found"}), 404
    stamps = data.get("stamps", data) if isinstance(data, dict) else data
    meta = data.get("metadata", {}) if isinstance(data, dict) else {}
    return jsonify({"metadata": meta, "stamps": stamps})


@app.route("/api/rescrape-group", methods=["POST"])
def api_rescrape_group():
    """Rescrape a single group by group_id and update the JSON in place."""
    body = request.get_json()
    country_slug = body.get("country")   # e.g. "Great-Britain"
    group_id = body.get("group_id")
    if not country_slug or not group_id:
        return jsonify({"error": "country and group_id required"}), 400

    # Find the detail_url from existing data to know which page to hit
    data = load_scraped_data(country_slug) or load_scraped_data(country_slug.replace("-", "_"))
    if not data:
        return jsonify({"error": "Country data not found"}), 404

    stamps = data.get("stamps", [])
    group_stamps = [s for s in stamps if s.get("group_id") == group_id]
    if not group_stamps:
        return jsonify({"error": f"No stamps found for group_id {group_id}"}), 404

    # Run scraper with --rescrape-group flag
    cmd = [sys.executable, "scraper.py", "--country", country_slug,
           "--rescrape-group", group_id]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            return jsonify({"error": result.stderr[-500:]}), 500
        return jsonify({"ok": True, "log": result.stdout[-1000:]})
    except subprocess.TimeoutExpired:
        return jsonify({"error": "Scrape timed out"}), 500


# Global cache for stamp metadata lookup
_stamp_lookup = None
_ocr_reader = None


def _get_ocr_reader():
    """Lazy-load EasyOCR reader (downloads model on first use)."""
    global _ocr_reader
    if _ocr_reader is None:
        import easyocr
        _ocr_reader = easyocr.Reader(["en"], gpu=False, verbose=False)
    return _ocr_reader


def _run_ocr(image_bytes: bytes) -> str:
    """Run OCR on image bytes, return extracted text as a single string."""
    try:
        reader = _get_ocr_reader()
        import numpy as np
        import cv2
        arr = np.frombuffer(image_bytes, dtype=np.uint8)
        img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if img is None:
            return ""
        results = reader.readtext(img, detail=0)
        text = " ".join(results).strip()
        print(f"  OCR detected: {text!r}")
        return text
    except Exception as e:
        print(f"  OCR failed: {e}")
        return ""


def _build_stamp_lookup():
    """Build a dict keyed by local_image path for fast metadata lookup."""
    global _stamp_lookup
    if _stamp_lookup is not None:
        return _stamp_lookup

    _stamp_lookup = {}
    for jp in glob.glob(os.path.join(OUTPUT_DIR, "stamps_*.json")):
        with open(jp, encoding="utf-8") as f:
            data = json.load(f)
        for s in data.get("stamps", []):
            key = s.get("local_image", "").replace("\\", "/")
            if key:
                _stamp_lookup[key] = s
    return _stamp_lookup


def enrich_matches(matches):
    """Add full stamp metadata (colour, prices, denomination, etc.) to match results."""
    lookup = _build_stamp_lookup()
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
        # Try v3 (CNN) first
        v3_manifest = os.path.join(INDEX_DIR, "manifest_v3.json")
        if os.path.exists(v3_manifest):
            from matcher.cnn_matcher import CNNIndex
            print(f"Loading v3 CNN index from {INDEX_DIR}...")
            _histogram_index = CNNIndex.load(INDEX_DIR)
            _store_loaded_at = datetime.utcnow()
            print(f"Loaded {len(_histogram_index)} images (CNN v3)")
            return _histogram_index

        # Fall back to v2 (histogram)
        v2_manifest = os.path.join(INDEX_DIR, "manifest_v2.json")
        if os.path.exists(v2_manifest):
            from matcher.histogram_matcher import HistogramIndex
            print(f"Loading v2 histogram index from {INDEX_DIR}...")
            _histogram_index = HistogramIndex.load(INDEX_DIR)
            _store_loaded_at = datetime.utcnow()
            print(f"Loaded {len(_histogram_index)} images (histogram v2)")
            return _histogram_index

        raise RuntimeError(
            "No index found. Run: python build_index_v3.py"
        )

    except ImportError as e:
        raise RuntimeError(f"Matcher package not available: {e}")
    except Exception as e:
        raise RuntimeError(f"Failed to load index: {e}")


@app.route("/api/match", methods=["POST"])
def api_match():
    """
    Match an uploaded stamp image against the reference index.
    
    Form fields:
      - image: file upload (JPEG/PNG, max 10MB)
      - country: optional country filter string
      - top_k: optional int, default 10
    
    Returns JSON with matches array and metadata.
    """
    start_time = time.time()
    
    # Check if image file is present
    if "image" not in request.files:
        return jsonify({"error": "No image file provided"}), 400
    
    image_file = request.files["image"]
    
    # Validate file
    if image_file.filename == "":
        return jsonify({"error": "No selected file"}), 400
    
    # Check file size (max 10MB)
    image_file.seek(0, 2)  # Seek to end
    file_size = image_file.tell()
    image_file.seek(0)  # Reset to beginning
    
    if file_size > 10 * 1024 * 1024:  # 10MB
        return jsonify({"error": "File too large (max 10MB)"}), 400
    
    # Check file type
    allowed_extensions = {".jpg", ".jpeg", ".png"}
    file_ext = os.path.splitext(image_file.filename.lower())[1]
    if file_ext not in allowed_extensions:
        return jsonify({"error": "Invalid file type. Please upload JPEG or PNG"}), 400
    
    # Get optional parameters
    country_filter = request.form.get("country")
    try:
        top_k = int(request.form.get("top_k", 10))
    except ValueError:
        top_k = 10
    
    # Load index
    ocr_text = ""
    try:
        index = load_descriptor_store()
    except RuntimeError as e:
        return jsonify({"error": str(e)}), 503
    
    # Read image bytes
    image_bytes = image_file.read()
    
    try:
        index = load_descriptor_store()

        # Detect index type and match accordingly
        from matcher.cnn_matcher import CNNIndex
        if isinstance(index, CNNIndex):
            # V3 CNN path — also compute colour histogram and OCR for re-ranking
            from matcher.cnn_matcher import compute_embedding_from_bytes
            import cv2
            import numpy as np

            embedding = compute_embedding_from_bytes(image_bytes)
            if embedding is None:
                return jsonify({"error": "Could not process image"}), 400

            # Compute colour histogram from uploaded image
            arr = np.frombuffer(image_bytes, dtype=np.uint8)
            img_bgr = cv2.imdecode(arr, cv2.IMREAD_COLOR)
            color_hist = None
            if img_bgr is not None:
                h, w = img_bgr.shape[:2]
                if max(h, w) > 256:
                    scale = 256 / max(h, w)
                    img_bgr = cv2.resize(img_bgr, (int(w * scale), int(h * scale)),
                                         interpolation=cv2.INTER_AREA)
                hsv = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2HSV)
                color_hist = cv2.calcHist([hsv], [0, 1, 2], None,
                                          [36, 12, 12],
                                          [0, 180, 0, 256, 0, 256])
                cv2.normalize(color_hist, color_hist)
                color_hist = color_hist.flatten().astype(np.float32)

            # Run OCR on uploaded image
            ocr_text = _run_ocr(image_bytes)

            # Get stamp metadata lookup for text matching
            stamp_metadata = _build_stamp_lookup()

            matches = index.query(embedding, top_k=top_k,
                                  country=country_filter,
                                  color_hist=color_hist,
                                  ocr_text=ocr_text,
                                  stamp_metadata=stamp_metadata)
            matches = enrich_matches(matches)
            # Include OCR text in response for debugging
            for m in matches:
                m["query_ocr"] = ocr_text or ""
        else:
            # V2 histogram path (fallback)
            import cv2
            import numpy as np
            from matcher.histogram_matcher import compute_features

            arr = np.frombuffer(image_bytes, dtype=np.uint8)
            img_bgr = cv2.imdecode(arr, cv2.IMREAD_COLOR)
            if img_bgr is None:
                return jsonify({"error": "Could not decode image"}), 400

            h, w = img_bgr.shape[:2]
            if max(h, w) > 256:
                scale = 256 / max(h, w)
                img_bgr = cv2.resize(img_bgr, (int(w * scale), int(h * scale)),
                                     interpolation=cv2.INTER_AREA)

            gray = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2GRAY)
            features = compute_features(gray, color_bgr=img_bgr)
            matches = index.query(features, top_k=top_k, country=country_filter)
            matches = enrich_matches(matches)

        elapsed_ms = int((time.time() - start_time) * 1000)

        return jsonify({
            "matches": matches,
            "query_features": 0,
            "elapsed_ms": elapsed_ms,
            "ocr_text": ocr_text,
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
            "orb_max_features": 0,
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
    print("Stamp Viewer: http://localhost:5000")
    app.run(debug=True, port=5000)

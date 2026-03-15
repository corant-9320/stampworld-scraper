"""
Scraper viewer — browse territories, trigger scraping, monitor progress.
Run: python scraper_viewer.py
Then open http://localhost:5001
"""
import json
import os
import subprocess
import sys
from collections import defaultdict
from flask import Flask, send_from_directory, jsonify, request

app = Flask(__name__, static_folder="viewer_static")

from config import OUTPUT_DIR, IMAGES_DIR


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
    return send_from_directory("viewer_static", "scraper.html")


@app.route("/api/data-overview")
def api_data_overview():
    """Return flat territory list with scraped/available counts."""
    try:
        import territories
    except ImportError:
        return jsonify({"territories": []})

    result = []
    for slug, data in territories.TERRITORIES.items():
        scraped_data = load_scraped_data(slug)
        scraped_count = len(scraped_data.get("stamps", [])) if scraped_data else 0
        result.append({
            "slug": slug,
            "display_name": data["display_name"],
            "region": data.get("region", "Unknown"),
            "scraped": scraped_count,
            "available": data.get("available", 0),
        })

    result = [t for t in result if t["available"] > 0]
    result.sort(key=lambda x: x["display_name"])
    return jsonify({"territories": result})


@app.route("/api/country/<slug>")
def api_country(slug):
    """Return all stamps for a country."""
    data = load_scraped_data(slug)
    if not data:
        return jsonify({"error": "Not found"}), 404
    stamps = data.get("stamps", data) if isinstance(data, dict) else data
    meta = data.get("metadata", {}) if isinstance(data, dict) else {}
    return jsonify({"metadata": meta, "stamps": stamps})


@app.route("/api/rescrape-group", methods=["POST"])
def api_rescrape_group():
    """Rescrape a single group by group_id."""
    body = request.get_json()
    country_slug = body.get("country")
    group_id = body.get("group_id")
    if not country_slug or not group_id:
        return jsonify({"error": "country and group_id required"}), 400

    data = load_scraped_data(country_slug)
    if not data:
        return jsonify({"error": "Country data not found"}), 404

    stamps = data.get("stamps", [])
    group_stamps = [s for s in stamps if s.get("group_id") == group_id]
    if not group_stamps:
        return jsonify({"error": f"No stamps found for group_id {group_id}"}), 404

    cmd = [sys.executable, "scraper.py", "--country", country_slug,
           "--rescrape-group", group_id]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            return jsonify({"error": result.stderr[-500:]}), 500
        return jsonify({"ok": True, "log": result.stdout[-1000:]})
    except subprocess.TimeoutExpired:
        return jsonify({"error": "Scrape timed out"}), 500


@app.route("/api/scrape", methods=["POST"])
def api_scrape():
    """Trigger scraping with SSE progress streaming."""
    from flask import Response, stream_with_context
    import json as _json

    body = request.get_json()
    slugs = body.get("slugs", []) if body else []
    if not slugs:
        return jsonify({"error": "slugs required"}), 400

    def generate():
        for i, slug in enumerate(slugs):
            existing_data = load_scraped_data(slug)
            use_delta = existing_data is not None
            mode = "delta" if use_delta else "full"
            yield _json.dumps({"slug": slug, "status": "running",
                               "message": f"Scraping {slug} ({mode})...",
                               "index": i, "total": len(slugs)}) + "\n"

            cmd = [sys.executable, "scraper.py", "--country", slug]
            if use_delta:
                cmd.append("--delta")

            try:
                proc = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
                if proc.returncode == 0:
                    data = load_scraped_data(slug)
                    stamps_scraped = len(data.get("stamps", [])) if data else 0
                    yield _json.dumps({"slug": slug, "status": "success",
                                       "message": f"{slug}: {stamps_scraped:,} stamps",
                                       "stamps_scraped": stamps_scraped,
                                       "index": i, "total": len(slugs)}) + "\n"
                else:
                    yield _json.dumps({"slug": slug, "status": "error",
                                       "message": f"{slug}: {proc.stderr[-200:].strip()}",
                                       "index": i, "total": len(slugs)}) + "\n"
            except subprocess.TimeoutExpired:
                yield _json.dumps({"slug": slug, "status": "error",
                                   "message": f"{slug}: timed out",
                                   "index": i, "total": len(slugs)}) + "\n"
            except Exception as e:
                yield _json.dumps({"slug": slug, "status": "error",
                                   "message": f"{slug}: {e}",
                                   "index": i, "total": len(slugs)}) + "\n"

        yield _json.dumps({"done": True, "total": len(slugs)}) + "\n"

    return Response(stream_with_context(generate()),
                    mimetype="application/x-ndjson")


@app.route("/api/scrape-progress/<slug>")
def api_scrape_progress(slug):
    """Return current scraped stamp count for a territory."""
    data = load_scraped_data(slug)
    count = len(data.get("stamps", [])) if data else 0
    return jsonify({"slug": slug, "count": count})


@app.route("/images/<path:filepath>")
def serve_image(filepath):
    return send_from_directory(IMAGES_DIR, filepath)


if __name__ == "__main__":
    print("Scraper Viewer: http://localhost:5001")
    app.run(debug=False, port=5001, threaded=True)

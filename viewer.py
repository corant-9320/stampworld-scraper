"""
Stamp collection viewer — browse scraped data, see what's done vs. remaining.
Run: python viewer.py
Then open http://localhost:5000
"""
import glob
import json
import os
from flask import Flask, send_from_directory, jsonify

app = Flask(__name__, static_folder="viewer_static")

OUTPUT_DIR = "output"
IMAGES_DIR = "stamp_images"

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


@app.route("/images/<path:filepath>")
def serve_image(filepath):
    return send_from_directory(IMAGES_DIR, filepath)


if __name__ == "__main__":
    print("Stamp Viewer: http://localhost:5000")
    app.run(debug=True, port=5000)

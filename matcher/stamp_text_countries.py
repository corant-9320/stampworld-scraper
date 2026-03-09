"""
Mapping of text found on stamps to country names.

Sourced from Mystic Stamp's Foreign Stamp Identifier:
https://info.mysticstamp.com/learn/foreign-stamp-identifier/
Content was rephrased for compliance with licensing restrictions.

Country values use StampWorld URL slugs (hyphens for spaces).
See https://www.stampworld.com/en/sitemap/ for the full list.

Used by the OCR matching pipeline to identify which country a stamp
belongs to based on text visible on the stamp.
"""

# Maps lowercase inscription text -> StampWorld country slug
# Multiple entries can map to the same country.
STAMP_TEXT_TO_COUNTRY = {
    # A
    "acores": "Azores",
    "afghan": "Afghanistan",
    "afghanes": "Afghanistan",
    "africa correios": "Portuguese-Africa",
    "afrique equatoriale francaise": "French-Equatorial-Africa",
    "afrique occidentale francaise": "French-West-Africa",
    "archipel des comores": "Comoro-Islands",
    "att": "Thailand",
    "atts": "Thailand",
    "aur": "Iceland",
    "australian antarctic territory": "Australian-Antarctic",
    "avisporto": "Denmark",
    # B
    "baden": "Germany",
    "baht": "Thailand",
    "bani": "Romania",
    "bayern": "Germany",
    "bayr": "Germany",
    "belgie": "Belgium",
    "belgien": "Belgium",
    "belgique": "Belgium",
    "belgisch congo": "Belgian-Congo",
    "benadir": "Somalia",
    "benin": "Benin",
    "bohmen und mahren": "Bohemia-and-Moravia",
    "bophuthatswana": "Bophuthatswana",
    "bosnien-herzegovina": "Bosnia-Herzegovina",
    "briefpost": "Germany",
    "british new guinea": "British-New-Guinea",
    "burkina faso": "Burkina-Faso",
    "burundi": "Burundi",
    # C
    "cambodge": "Cambodia",
    "cambodia": "Cambodia",
    "cccp": "USSR",
    "cechy a morava": "Bohemia-and-Moravia",
    "cesko-slovensko": "Czechoslovakia",
    "chiffre tax": "France",
    "ciskei": "Ciskei",
    "colonia eritrea": "Italian-Eritrea",
    "colonies postes": "French-Colonies",
    "comores": "Comoro-Islands",
    "confederatio helvetica": "Switzerland",
    "congo belge": "Belgian-Congo",
    "congo francais": "French-Congo",
    "cook islands": "Cook-Islands",
    "coree": "South-Korea",
    "corean": "South-Korea",
    "correio": "Portugal",
    "correo espanol": "Spain",
    "correos de colombia": "Colombia",
    "correos y telegs": "Spain",
    "cote de somalis": "French-Somali-Coast",
    "cote d'ivoire": "Ivory-Coast",
    "curacao": "Curacao",
    # D
    "ddr": "DDR",
    "deficit": "Peru",
    "deutsche bundespost": "Germany",
    "deutsche demokratische republik": "DDR",
    "deutsche feldpost": "Germany",
    "deutsche post": "Germany",
    "deutsche reichs-post": "Germany",
    "deutsches reich": "Germany",
    "deutsch-neu-guinea": "German-New-Guinea",
    "deutschosterreich": "Austria",
    "djibouti": "Djibouti",
    "doplatit": "Czechoslovakia",
    "doplatne": "Czechoslovakia",
    "drzava": "Yugoslavia",
    "drzavna": "Yugoslavia",
    # E
    "eesti": "Estonia",
    "eire": "Ireland",
    "er": "Great-Britain",
    "e.r.": "Great-Britain",
    "e r": "Great-Britain",
    "eiir": "Great-Britain",
    "e ii r": "Great-Britain",
    "empire franc": "France",
    "empire francaise": "France",
    "equateur": "Ecuador",
    "espana": "Spain",
    "espanola": "Spain",
    "estado da india": "Portuguese-India",
    "etat francais": "France",
    "etat du cameroun": "Cameroon",
    "ethiopie": "Ethiopia",
    "ethiopiennes": "Ethiopia",
    "etiopia": "Ethiopia",
    # F
    "faridkot": "Faridkot",
    "filipas": "Philippines",
    "filipinas": "Philippines",
    "filler": "Hungary",
    "franco": "Spain",
    "frimarke": "Sweden",
    "furstentum liechtenstein": "Liechtenstein",
    # G
    "georgie": "Georgia",
    "georgienne": "Georgia",
    "grand liban": "Lebanon",
    "gronland": "Greenland",
    "grossdeutsches reich": "Germany",
    "guinee": "Guinea",
    "guinee francaise": "French-Guinea",
    "guyane francaise": "French-Guyane",
    # H
    "haute volta": "Upper-Volta",
    "helvetia": "Switzerland",
    "hrvatska": "Croatia",
    # I
    "island": "Iceland",
    "italia": "Italy",
    "italiane": "Italy",
    # J
    "jugoslavija": "Yugoslavia",
    # K
    "kamerun": "Cameroon",
    "kampuchea": "Cambodia",
    "kibris cumhuriyeti": "Cyprus-Greek",
    "kiribati": "Kiribati",
    "krone": "Austria",
    "kronen": "Austria",
    "kurus": "Turkey",
    # L
    "latvija": "Latvia",
    "latwija": "Latvia",
    "leva": "Bulgaria",
    "liban": "Lebanon",
    "libanaise": "Lebanon",
    "libia": "Libya",
    "lietuva": "Lithuania",
    "lietvos": "Lithuania",
    "lisboa": "Portugal",
    "losen": "Sweden",
    # M
    "macau": "Macau",
    "madrid": "Spain",
    "magyar": "Hungary",
    "magyarorszag": "Hungary",
    "malacca": "Malacca/Melaka",
    "malagasy": "Madagascar",
    "malgache": "Madagascar",
    "mapka": "Russia",
    "mark": "Finland",
    "markkaa": "Finland",
    "maroc": "Morocco",
    "marruecos": "Morocco",
    "mejico": "Mexico",
    "mexicano": "Mexico",
    "mocambique": "Mozambique",
    "monaco": "Monaco",
    # N
    "namibia": "Namibia",
    "naciones unidas": "UN-New-York",
    "nations unies": "UN-Geneva",
    "nederland": "Netherlands",
    "ned-indie": "Netherlands-Indies",
    "nippon": "Japan",
    "noreg": "Norway",
    "norge": "Norway",
    "nouvelle caledonie": "New-Caledonia",
    "nouvelles hebrides": "New-Hebrides",
    # O
    "osterreich": "Austria",
    "osterreichische post": "Austria",
    # P
    "papua": "Papua-New-Guinea",
    "pen": "Finland",
    "penni": "Finland",
    "pennias": "Finland",
    "persane": "Iran",
    "peruana": "Peru",
    "pesetas": "Spain",
    "pfennig": "Germany",
    "poczta": "Poland",
    "poczta polska": "Poland",
    "polska": "Poland",
    "polynesie francaise": "French-Polynesia",
    "porteado": "Portugal",
    "postage": "Great-Britain",
    "postage revenue": "Great-Britain",
    "postes": "France",
    "poste vaticane": "Vatican-City",
    "postzeegel": "Netherlands",
    "principaute de monaco": "Monaco",
    "pto rico": "Puerto-Rico",
    "puerto rico": "Puerto-Rico",
    # Q
    "quindar": "Albania",
    "quintar": "Albania",
    # R
    "recargo": "Spain",
    "reich": "Germany",
    "reichspost": "Germany",
    "reis": "Portugal",
    "rep di s marino": "San-Marino",
    "repubblica italiana": "Italy",
    "republique francaise": "France",
    "republica de guinea ecuatorial": "Equatorial-Guinea",
    "republica dominicana": "Dominican-Republic",
    "republica espanola": "Spain",
    "republica oriental": "Uruguay",
    "republica peruana": "Peru",
    "republica portuguesa": "Portugal",
    "republiek van suid-afrika": "South-Africa",
    "republik indonesia": "Indonesia",
    "republique arabe unie": "UAR",
    "republique centrafricaine": "Central-African-Republic",
    "republique de cote d'ivoire": "Ivory-Coast",
    "republique de guinee": "Guinea",
    "republique de haute volta": "Upper-Volta",
    "republique democratique du congo": "Congo,-Dr",
    "republique d'haiti": "Haiti",
    "republique du congo": "Congo,-Rep.",
    "republique du dahomey": "Dahomey",
    "republique du mali": "Mali",
    "republique du niger": "Niger",
    "republique du senegal": "Senegal",
    "republique du tchad": "Chad",
    "republique du togo": "Togo",
    "republique gabonaise": "Gabon",
    "republique islamique de mauritanie": "Mauritania",
    "republique libanaise": "Lebanon",
    "republique malgache": "Madagascar",
    "republique rwandaise": "Rwanda",
    "republique togolaise": "Togo",
    "republique tunisienne": "Tunisia",
    "rf": "France",
    "rial": "Iran",
    "rials": "Iran",
    "romana": "Romania",
    "romania": "Romania",
    "romina": "Romania",
    "ross dependency": "Ross-Dependency",
    "royaume du burundi": "Burundi",
    "royaume du cambodge": "Cambodia",
    "royaume du laos": "Laos",
    "royaume du maroc": "Morocco",
    "rsm": "San-Marino",
    "rwanda": "Rwanda",
    # S
    "saargebiet": "Saargebiet",
    "saarland": "Saarland",
    "saarpost": "Saar",
    "sahara espanol": "Spanish-Sahara",
    "shqiperia": "Albania",
    "shqiperise": "Albania",
    "shqipni": "Albania",
    "shqipnija": "Albania",
    "skilling": "Norway",
    "slovenska posta": "Slovakia",
    "slovensko": "Slovakia",
    "s marino": "San-Marino",
    "soudan": "Sudan",
    "soudan francais": "French-Sudan",
    "sri lanka": "Sri-Lanka",
    "suidafrika": "South-Africa",
    "suidwes afrika": "South-West-Africa",
    "suomi": "Finland",
    "suriname": "Suriname",
    "sverige": "Sweden",
    "syrie": "Syria",
    "syrienne": "Syria",
    # T
    "tchad": "Chad",
    "te betalen": "Netherlands",
    "thailand": "Thailand",
    "thai": "Thailand",
    "timbre poste": "Morocco",
    "toga": "Tonga",
    "touva": "Tannu-Touva",
    "transkei": "Transkei",
    "tunisie": "Tunisia",
    "tunis": "Tunisia",
    "turkiye": "Turkey",
    "turk postalari": "Turkey",
    "turkiye cumhuriyeti postalari": "Turkey",
    "tuvalu": "Tuvalu",
    # U
    "uar": "UAR",
    "union of south africa": "South-Africa",
    # V
    "valles d'andorre": "Andorra-FR",
    "vaticane": "Vatican-City",
    "venezolana": "Venezuela",
    "venezolano": "Venezuela",
    "viet-nam": "Vietnam",
    # W
    "western samoa": "Samoa",
    # Z
    "zaire": "Congo,-Dr",
    "zambia": "Zambia",
    "zimbabwe": "Zimbabwe",
    "zuidwest afrika": "South-West-Africa",
}


# Common denomination/currency terms found on stamps, mapped to country hints.
# These are weaker signals than country inscriptions but still useful.
# Values use StampWorld URL slugs.
CURRENCY_TO_COUNTRY = {
    "penny": "Great-Britain",
    "pence": "Great-Britain",
    "shilling": "Great-Britain",
    "shillings": "Great-Britain",
    "halfpenny": "Great-Britain",
    "centimes": "France",
    "centime": "France",
    "franc": "France",
    "francs": "France",
    "pfennig": "Germany",
    "pfg": "Germany",
    "mark": "Germany",
    "reichsmark": "Germany",
    "ore": "Sweden",
    "krona": "Sweden",
    "kronor": "Sweden",
    "krone": "Denmark",
    "kroner": "Denmark",
    "kopek": "Russia",
    "kopecks": "Russia",
    "rouble": "Russia",
    "ruble": "Russia",
    "lira": "Italy",
    "lire": "Italy",
    "centesimi": "Italy",
    "peseta": "Spain",
    "pesetas": "Spain",
    "centimos": "Spain",
    "reis": "Portugal",
    "escudo": "Portugal",
    "escudos": "Portugal",
    "gulden": "Netherlands",
    "cent": "Netherlands",
    "pengo": "Hungary",
    "filler": "Hungary",
    "forint": "Hungary",
    "zloty": "Poland",
    "groszy": "Poland",
    "koruna": "Czech-Republic",
    "haleru": "Czech-Republic",
    "dinar": "Yugoslavia",
    "para": "Yugoslavia",
    "lev": "Bulgaria",
    "leva": "Bulgaria",
    "stotinki": "Bulgaria",
    "lei": "Romania",
    "bani": "Romania",
    "markka": "Finland",
    "pennia": "Finland",
    "drachma": "Greece",
    "lepta": "Greece",
    "yen": "Japan",
    "sen": "Japan",
    "won": "South-Korea",
    "rupee": "India",
    "rupees": "India",
    "anna": "India",
    "annas": "India",
    "paise": "India",
    "paisa": "India",
    "baht": "Thailand",
    "satang": "Thailand",
    "dollar": "United-States",
    "dollars": "United-States",
    "cents": "United-States",
}


def find_country_from_text(ocr_text: str) -> list:
    """
    Given OCR text from a stamp, return a ranked list of
    (country, confidence) tuples based on text matches.

    Tries longest matches first for accuracy.
    """
    if not ocr_text:
        return []

    text_lower = ocr_text.lower()
    # Remove common noise characters
    for ch in ".,;:!?()[]{}\"'":
        text_lower = text_lower.replace(ch, " ")

    matches = {}

    # Check inscription matches (strongest signal)
    for inscription, country in STAMP_TEXT_TO_COUNTRY.items():
        if inscription in text_lower:
            # Longer matches are more specific/reliable
            weight = len(inscription) / 10.0
            matches[country] = max(matches.get(country, 0), weight)

    # Check currency matches (weaker signal)
    words = set(text_lower.split())
    for term, country in CURRENCY_TO_COUNTRY.items():
        if term in words:
            weight = 0.3  # weaker than inscription matches
            matches[country] = max(matches.get(country, 0), weight)

    # Sort by confidence descending
    ranked = sorted(matches.items(), key=lambda x: -x[1])
    return ranked

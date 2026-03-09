#!/usr/bin/env python3
"""
Build a complete territory catalog from StampWorld's sitemap.

Scrapes https://www.stampworld.com/en/sitemap/ to get every territory,
then visits each territory page to get the total stamp count.

Outputs territory_catalog.json with continent/territory hierarchy and counts.
"""

import json
import os
import re
import time
import logging
import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("catalog")

SITEMAP_URL = "https://www.stampworld.com/en/sitemap/"
BASE_URL = "https://www.stampworld.com"
CATALOG_FILE = "territory_catalog.json"
DELAY = 1.0  # seconds between requests

# ---------------------------------------------------------------------------
# Continent classification
# ---------------------------------------------------------------------------
# Territories are assigned to continents. If unknown, goes to "Unknown".

EUROPE = {
    "Aaland", "Albania", "Alderney", "Allenstein", "Andorra ES", "Andorra FR",
    "Austria", "Austrian Post Crete", "Austrian Post Turkish Empire",
    "Austro Hungary Military Post", "Azores", "Baden", "Bavaria", "Belarus",
    "Belgium", "Bergedorf", "Berlin", "Bohemia and Moravia", "Bosnia Herzegovina",
    "Braunschweig", "Bremen", "Bulgaria", "Bulgarian Occ. in Romania",
    "Castellorizo", "Conseil de LEurope", "Crete", "Croatia",
    "Croatian Post East Mostar", "Croatian Post Mostar", "Cyprus Greek",
    "Cyprus Turkish", "Czech Republic", "Czechoslovak Legion Post",
    "Czechoslovakia", "DDR", "Danzig", "Denmark", "Eastern Rumelia",
    "Eastern Silesia", "Epirus", "Estonia", "Faroe Islands", "Finland",
    "Fiume", "France", "French Baden", "French Committee of National Liberation",
    "French Rheinland Pfalz", "French Wurttemberg", "French Zone",
    "General Government", "German Colonies", "German Empire",
    "German Plebiscite Territories", "German Post Abroad",
    "German WWI Occ. in Belgium", "German WWI Occ. in Poland",
    "German WWI Occ. in Romania", "German WWII Occ. in Albania",
    "German WWII Occ. in Alsace", "German WWII Occ. in Estonia",
    "German WWII Occ. in France", "German WWII Occ. in Guernsey",
    "German WWII Occ. in Jersey", "German WWII Occ. in Kotor",
    "German WWII Occ. in Kurland", "German WWII Occ. in Laibach",
    "German WWII Occ. in Latvia", "German WWII Occ. in Lithuania",

"""
Centralized configuration for the stamp matching system.

All magic numbers, constants, and default values are defined here.
"""

# ---------------------------------------------------------------------------
# Scraper configuration
# ---------------------------------------------------------------------------
SCRAPER_DELAY = 1.5  # seconds between page requests
SCRAPER_MAX_RETRIES = 3  # maximum retry attempts for failed requests
BROWSER_RESTART_EVERY = 20  # restart browser every N pages to avoid memory/session rot
SCRAPER_TIMEOUT = 3600  # 1 hour timeout per country in batch scraping

# ---------------------------------------------------------------------------
# Matching configuration
# ---------------------------------------------------------------------------

# CNN matching parameters
CNN_FLOOR = 0.75  # minimum useful cosine similarity for CNN embeddings
CNN_CEIL = 0.95   # maximum useful cosine similarity for CNN embeddings
CNN_BATCH_SIZE = 32  # batch size for CNN embedding computation
CNN_INPUT_SIZE = 224  # ResNet input size (224x224)
CNN_EMBEDDING_DIM = 512  # ResNet-18 embedding dimension

# Signal weights for multi-signal re-ranking (must sum to 1.0)
SIGNAL_WEIGHTS = {
    "cnn": 0.55,    # CNN embedding similarity
    "color": 0.35,  # Color histogram similarity
    "aspect": 0.10, # Aspect ratio similarity
}

# Confidence sigmoid scaling
CONFIDENCE_SIGMOID_SCALE = 12
CONFIDENCE_SIGMOID_CENTER = 0.45

# Histogram configuration
HSV_BINS = (36, 12, 12)  # (hue, saturation, value) bins for color histograms
HSV_BINS_V2 = (18, 3, 3)  # v2 histogram index uses fewer bins

# Histogram matcher weights (must sum to 1.0)
HISTOGRAM_WEIGHTS = {
    "hist": 0.50,   # Color histogram similarity
    "ahash": 0.20,  # Average hash similarity
    "phash": 0.30,  # Perceptual hash similarity
}

# Image processing
IMAGE_RESIZE_TARGET = 512  # target size for preprocessing
IMAGE_RESIZE_V2 = 256  # v2 index resizes to 256
IMAGE_RESIZE_QUERY = 256  # query images resized to 256

# OCR text matching
TEXT_MATCH_WEIGHT = 0.3  # weight for currency/denomination text matches

# ---------------------------------------------------------------------------
# Path configuration
# ---------------------------------------------------------------------------
OUTPUT_DIR = "output"  # directory for scraped JSON files
IMAGES_DIR = "stamp_images"  # root directory for stamp images
INDEX_DIR = "descriptor_index"  # directory for index files
TOOLS_DIR = "tools"  # directory for utility scripts

# ---------------------------------------------------------------------------
# Web server configuration
# ---------------------------------------------------------------------------
FLASK_HOST = "0.0.0.0"
FLASK_PORT = 5000
FLASK_DEBUG = True

# ---------------------------------------------------------------------------
# StampWorld API configuration
# ---------------------------------------------------------------------------
STAMPWORLD_BASE_URL = "https://www.stampworld.com"
STAMPWORLD_USER_ID = "694157"  # default user ID for collection paths

# ---------------------------------------------------------------------------
# Validation and limits
# ---------------------------------------------------------------------------
MAX_IMAGE_SIZE_MB = 10  # maximum image size to process (MB)
MIN_IMAGE_DIMENSION = 50  # minimum image dimension (pixels)
MAX_QUERY_RESULTS = 100  # maximum number of results to return
DEFAULT_TOP_K = 10  # default number of matches to return
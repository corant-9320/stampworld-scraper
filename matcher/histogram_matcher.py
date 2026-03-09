"""
Histogram + perceptual-hash matcher for stamp images.

Uses colour histograms (HSV) and average-hash for robust
"same design" matching — much better than ORB for stamps
where the query is a photo of a catalogue-scan reference.
"""

import cv2
import numpy as np
from dataclasses import dataclass, field
from typing import List, Optional, Tuple


# ---------------------------------------------------------------------------
# Feature extraction
# ---------------------------------------------------------------------------

def compute_features(gray: np.ndarray, color_bgr: np.ndarray = None,
                     hist_bins: Tuple[int, int, int] = (18, 3, 3)) -> dict:
    """
    Compute a compact feature vector for one stamp image.

    Returns dict with:
      - "hist"   : normalised HSV histogram  (flattened float32)
      - "ahash"  : average-hash bits         (uint8 array, 64 bits)
      - "phash"  : perceptual DCT hash bits  (uint8 array, 64 bits)
    """
    features = {}

    # --- colour histogram (HSV) ---
    if color_bgr is not None:
        hsv = cv2.cvtColor(color_bgr, cv2.COLOR_BGR2HSV)
    else:
        # If only grayscale provided, fake a 1-channel "hue" hist
        hsv = cv2.merge([gray, gray, gray])

    hist = cv2.calcHist([hsv], [0, 1, 2], None, list(hist_bins),
                        [0, 180, 0, 256, 0, 256])
    cv2.normalize(hist, hist)
    features["hist"] = hist.flatten().astype(np.float32)

    # --- average hash (8×8) ---
    small = cv2.resize(gray, (8, 8), interpolation=cv2.INTER_AREA)
    mean_val = small.mean()
    features["ahash"] = (small > mean_val).flatten().astype(np.uint8)

    # --- perceptual hash (DCT-based, 8×8) ---
    img32 = cv2.resize(gray, (32, 32), interpolation=cv2.INTER_AREA).astype(np.float32)
    dct = cv2.dct(img32)
    dct_low = dct[:8, :8]
    med = np.median(dct_low)
    features["phash"] = (dct_low > med).flatten().astype(np.uint8)

    return features


def hamming(a: np.ndarray, b: np.ndarray) -> int:
    """Hamming distance between two bit-arrays."""
    return int(np.count_nonzero(a != b))


def hist_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """Correlation-based histogram similarity in [−1, 1], higher = more similar."""
    return float(cv2.compareHist(a, b, cv2.HISTCMP_CORREL))


# ---------------------------------------------------------------------------
# Index (in-memory, numpy-backed)
# ---------------------------------------------------------------------------

@dataclass
class IndexRecord:
    idx: int
    image_id: str
    country: str
    sw_id: str
    number: str
    group_title: str
    local_image: str
    detail_url: str


class HistogramIndex:
    """Pre-computed feature index for all reference images."""

    def __init__(self):
        self.records: List[IndexRecord] = []
        self.hists: Optional[np.ndarray] = None      # (N, hist_dim) float32
        self.ahashes: Optional[np.ndarray] = None     # (N, 64) uint8
        self.phashes: Optional[np.ndarray] = None     # (N, 64) uint8

    # ---- building --------------------------------------------------------

    def add(self, record: IndexRecord, features: dict) -> None:
        h = features["hist"].reshape(1, -1)
        a = features["ahash"].reshape(1, -1)
        p = features["phash"].reshape(1, -1)

        if self.hists is None:
            self.hists = h
            self.ahashes = a
            self.phashes = p
        else:
            self.hists = np.vstack([self.hists, h])
            self.ahashes = np.vstack([self.ahashes, a])
            self.phashes = np.vstack([self.phashes, p])

        self.records.append(record)

    # ---- persistence -----------------------------------------------------

    def save(self, directory: str) -> None:
        import json, os
        os.makedirs(directory, exist_ok=True)
        np.save(os.path.join(directory, "hists.npy"), self.hists)
        np.save(os.path.join(directory, "ahashes.npy"), self.ahashes)
        np.save(os.path.join(directory, "phashes.npy"), self.phashes)

        manifest = {
            "version": 2,
            "total_images": len(self.records),
            "hist_dim": int(self.hists.shape[1]),
            "images": [
                {
                    "image_id": r.image_id,
                    "country": r.country,
                    "sw_id": r.sw_id,
                    "number": r.number,
                    "group_title": r.group_title,
                    "local_image": r.local_image,
                    "detail_url": r.detail_url,
                }
                for r in self.records
            ],
        }
        with open(os.path.join(directory, "manifest_v2.json"), "w") as f:
            json.dump(manifest, f, indent=2)

    @classmethod
    def load(cls, directory: str) -> "HistogramIndex":
        import json, os
        idx = cls()
        idx.hists = np.load(os.path.join(directory, "hists.npy"))
        idx.ahashes = np.load(os.path.join(directory, "ahashes.npy"))
        idx.phashes = np.load(os.path.join(directory, "phashes.npy"))

        with open(os.path.join(directory, "manifest_v2.json")) as f:
            manifest = json.load(f)

        for img in manifest["images"]:
            idx.records.append(IndexRecord(
                idx=len(idx.records),
                image_id=img["image_id"],
                country=img["country"],
                sw_id=img["sw_id"],
                number=img["number"],
                group_title=img["group_title"],
                local_image=img["local_image"],
                detail_url=img["detail_url"],
            ))
        return idx

    def __len__(self):
        return len(self.records)

    # ---- querying --------------------------------------------------------

    def query(self, features: dict, top_k: int = 10,
              country: str = None,
              w_hist: float = 0.50, w_ahash: float = 0.20,
              w_phash: float = 0.30) -> List[dict]:
        """
        Find the top-K most similar reference images.

        Scores are a weighted combination of:
          - HSV histogram correlation  (higher = better, range −1..1)
          - Average-hash similarity    (1 − hamming/64, range 0..1)
          - Perceptual-hash similarity (1 − hamming/64, range 0..1)

        Returns list of dicts sorted by score descending.
        """
        q_hist = features["hist"].astype(np.float64)
        q_ahash = features["ahash"]
        q_phash = features["phash"]

        n = len(self.records)

        # --- Vectorised histogram correlation (Pearson) ---
        # corr(a, b) = sum((a-mean_a)*(b-mean_b)) / (std_a * std_b * N)
        ref = self.hists.astype(np.float64)          # (N, D)
        q = q_hist.reshape(1, -1)                     # (1, D)

        q_mean = q.mean()
        q_centered = q - q_mean
        q_std = np.sqrt(np.sum(q_centered ** 2))

        ref_mean = ref.mean(axis=1, keepdims=True)    # (N, 1)
        ref_centered = ref - ref_mean                  # (N, D)
        ref_std = np.sqrt(np.sum(ref_centered ** 2, axis=1))  # (N,)

        # Dot product of centered vectors
        dot = ref_centered @ q_centered.T              # (N, 1)
        dot = dot.ravel()                              # (N,)

        # Avoid division by zero
        denom = ref_std * q_std
        denom[denom == 0] = 1e-10

        hist_corr = dot / denom                        # (N,) in [-1, 1]

        scores = w_hist * hist_corr

        # --- Hash distances (already vectorised) ---
        ahash_dist = np.count_nonzero(self.ahashes != q_ahash, axis=1)
        phash_dist = np.count_nonzero(self.phashes != q_phash, axis=1)
        scores += w_ahash * (1.0 - ahash_dist / 64.0)
        scores += w_phash * (1.0 - phash_dist / 64.0)

        # --- Country filter ---
        if country:
            cl = country.lower()
            mask = np.array([r.country.lower() != cl for r in self.records])
            scores[mask] = -999.0

        # --- Top-K ---
        top_idx = np.argpartition(scores, -top_k)[-top_k:]
        top_idx = top_idx[np.argsort(scores[top_idx])[::-1]]

        results = []
        for i in top_idx:
            if scores[i] < -900:
                continue
            r = self.records[i]
            results.append({
                "image_id": r.image_id,
                "country": r.country,
                "sw_id": r.sw_id,
                "number": r.number,
                "group_title": r.group_title,
                "local_image": r.local_image,
                "detail_url": r.detail_url,
                "confidence": float(np.clip(scores[i], 0, 1)),
                "good_matches": 0,
            })
        return results

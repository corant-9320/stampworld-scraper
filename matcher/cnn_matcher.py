"""
CNN embedding matcher for stamp images (v3).

Uses a pretrained ResNet-18 to extract 512-dim feature vectors,
then cosine similarity for matching. Far more accurate than
histogram/hash approaches for visual similarity.
"""

import os
import json
import numpy as np
from dataclasses import dataclass
from typing import List, Optional

import torch
import torch.nn as nn
from torchvision import models, transforms
from PIL import Image


# ---------------------------------------------------------------------------
# Feature extraction
# ---------------------------------------------------------------------------

# ImageNet normalisation
_TRANSFORM = transforms.Compose([
    transforms.Resize((224, 224)),
    transforms.ToTensor(),
    transforms.Normalize(mean=[0.485, 0.456, 0.406],
                         std=[0.229, 0.224, 0.225]),
])

_model = None


def _get_model():
    """Lazy-load ResNet-18 with final FC layer removed."""
    global _model
    if _model is None:
        resnet = models.resnet18(weights=models.ResNet18_Weights.DEFAULT)
        # Remove the classification head — keep up to avgpool
        _model = nn.Sequential(*list(resnet.children())[:-1])
        _model.eval()
        if torch.cuda.is_available():
            _model = _model.cuda()
    return _model


def compute_embedding(img_path: str) -> Optional[np.ndarray]:
    """
    Compute a 512-dim L2-normalised embedding for one image.
    Returns None if the image can't be loaded.
    """
    try:
        img = Image.open(img_path).convert("RGB")
    except Exception:
        return None

    tensor = _TRANSFORM(img).unsqueeze(0)  # (1, 3, 224, 224)

    model = _get_model()
    device = next(model.parameters()).device
    tensor = tensor.to(device)

    with torch.no_grad():
        feat = model(tensor)  # (1, 512, 1, 1)

    vec = feat.squeeze().cpu().numpy().astype(np.float32)  # (512,)
    # L2 normalise so dot product = cosine similarity
    norm = np.linalg.norm(vec)
    if norm > 0:
        vec /= norm
    return vec


def compute_embeddings_batch(img_paths: list) -> list:
    """
    Compute embeddings for a batch of images at once (much faster on CPU).
    Returns list of (path, embedding) tuples; skips images that fail to load.
    """
    tensors = []
    valid_paths = []

    for p in img_paths:
        try:
            img = Image.open(p).convert("RGB")
            tensors.append(_TRANSFORM(img))
            valid_paths.append(p)
        except Exception:
            continue

    if not tensors:
        return []

    batch = torch.stack(tensors)  # (B, 3, 224, 224)
    model = _get_model()
    device = next(model.parameters()).device
    batch = batch.to(device)

    with torch.no_grad():
        feats = model(batch)  # (B, 512, 1, 1)

    feats = feats.squeeze(-1).squeeze(-1).cpu().numpy().astype(np.float32)  # (B, 512)
    # L2 normalise each row
    norms = np.linalg.norm(feats, axis=1, keepdims=True)
    norms[norms == 0] = 1e-10
    feats /= norms

    return list(zip(valid_paths, feats))


def compute_embedding_from_bytes(image_bytes: bytes) -> Optional[np.ndarray]:
    """Compute embedding from raw image bytes (for upload handling)."""
    import io
    try:
        img = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    except Exception:
        return None

    tensor = _TRANSFORM(img).unsqueeze(0)
    model = _get_model()
    device = next(model.parameters()).device
    tensor = tensor.to(device)

    with torch.no_grad():
        feat = model(tensor)

    vec = feat.squeeze().cpu().numpy().astype(np.float32)
    norm = np.linalg.norm(vec)
    if norm > 0:
        vec /= norm
    return vec


# ---------------------------------------------------------------------------
# Index
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


class CNNIndex:
    """Pre-computed CNN embedding index for all reference images."""

    def __init__(self):
        self.records: List[IndexRecord] = []
        self.embeddings: Optional[np.ndarray] = None  # (N, 512) float32

    def add(self, record: IndexRecord, embedding: np.ndarray) -> None:
        emb = embedding.reshape(1, -1)
        if self.embeddings is None:
            self.embeddings = emb
        else:
            self.embeddings = np.vstack([self.embeddings, emb])
        self.records.append(record)

    def save(self, directory: str) -> None:
        os.makedirs(directory, exist_ok=True)
        np.save(os.path.join(directory, "embeddings_v3.npy"), self.embeddings)

        manifest = {
            "version": 3,
            "total_images": len(self.records),
            "embedding_dim": int(self.embeddings.shape[1]),
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
        with open(os.path.join(directory, "manifest_v3.json"), "w") as f:
            json.dump(manifest, f, indent=2)

    @classmethod
    def load(cls, directory: str) -> "CNNIndex":
        idx = cls()
        idx.embeddings = np.load(os.path.join(directory, "embeddings_v3.npy"))

        with open(os.path.join(directory, "manifest_v3.json")) as f:
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

    def query(self, embedding: np.ndarray, top_k: int = 10,
              country: str = None,
              color_hist: np.ndarray = None,
              ocr_text: str = None,
              stamp_metadata: dict = None,
              w_cnn: float = 0.40, w_color: float = 0.35,
              w_text: float = 0.10, w_country: float = 0.15) -> List[dict]:
        """
        Find top-K most similar images, re-ranked with multiple signals:

        - CNN cosine similarity: design/structure matching (50%)
        - HSV histogram correlation: colour discrimination (25%)
        - OCR text vs scraped denomination/colour: text matching (10%)
        - OCR country identification: country boost (15%)
        """
        import cv2
        from matcher.stamp_text_countries import find_country_from_text

        q = embedding.reshape(1, -1)  # (1, 512)

        # Cosine similarity via dot product (all normalised)
        cnn_scores = (self.embeddings @ q.T).ravel()  # (N,)

        # Country filter (explicit user selection)
        if country:
            cl = country.lower()
            mask = np.array([r.country.lower() != cl for r in self.records])
            cnn_scores[mask] = -999.0

        # Get a wider shortlist for re-ranking (5x top_k)
        shortlist_k = min(top_k * 5, len(cnn_scores))
        if len(cnn_scores) > shortlist_k:
            shortlist_idx = np.argpartition(cnn_scores, -shortlist_k)[-shortlist_k:]
        else:
            shortlist_idx = np.arange(len(cnn_scores))

        # Prepare OCR tokens for denomination matching
        ocr_tokens = set()
        if ocr_text:
            ocr_tokens = set(ocr_text.lower().split())

        # Identify country from OCR text
        ocr_countries = {}
        if ocr_text:
            for c, conf in find_country_from_text(ocr_text):
                ocr_countries[c.lower()] = min(conf, 1.0)

        # Re-rank shortlist
        # CNN cosine sims for stamps cluster in ~0.75-0.95 because all stamps
        # share borders/frames/text. Rescale to the useful range so the CNN
        # weight actually discriminates between good and bad design matches.
        # floor=0.75 → 0%, ceiling=0.95 → 100%
        CNN_FLOOR = 0.75
        CNN_CEIL = 0.95

        final_scores = np.full(len(cnn_scores), -999.0)
        _breakdown = {}
        for i in shortlist_idx:
            if cnn_scores[i] < -900:
                continue

            # Rescale CNN to useful range
            cnn_rescaled = np.clip(
                (cnn_scores[i] - CNN_FLOOR) / (CNN_CEIL - CNN_FLOOR), 0.0, 1.0)
            score = w_cnn * cnn_rescaled

            # Track individual scores for display
            _hist_sim = 0.0
            _text_sim = 0.0
            _country_sim = 0.0

            # Colour histogram re-ranking
            # Use Bhattacharyya distance (0 = identical, 1 = no overlap)
            # converted to similarity. CORREL was wrong here — it measures
            # linear correlation of bin patterns, not actual colour overlap,
            # so brown and blue could score similarly if their S/V
            # distributions had similar shapes.
            if color_hist is not None:
                r = self.records[i]
                ref_hist = self._get_color_hist(r.local_image)
                if ref_hist is not None:
                    bhatt_dist = float(cv2.compareHist(
                        color_hist, ref_hist, cv2.HISTCMP_BHATTACHARYYA))
                    _hist_sim = 1.0 - bhatt_dist  # 1 = identical, 0 = no overlap
                score += w_color * _hist_sim

            # OCR text matching against scraped denomination/colour
            if ocr_tokens and stamp_metadata:
                img_key = self.records[i].local_image.replace("\\", "/")
                meta = stamp_metadata.get(img_key, {})
                denom = (meta.get("denomination") or "").lower()
                colour = (meta.get("colour") or "").lower()
                ref_text = f"{denom} {colour}"
                ref_tokens = set(ref_text.split())
                if ref_tokens:
                    overlap = len(ocr_tokens & ref_tokens)
                    _text_sim = overlap / max(len(ocr_tokens | ref_tokens), 1)
                score += w_text * _text_sim

            # Country identification boost from OCR
            if ocr_countries:
                rec_country = self.records[i].country.lower()
                _country_sim = ocr_countries.get(rec_country, 0.0)
                score += w_country * _country_sim

            final_scores[i] = score
            _breakdown[i] = (cnn_rescaled, _hist_sim, _text_sim, _country_sim)

        # Top-K from final scores
        if len(final_scores) > top_k:
            top_idx = np.argpartition(final_scores, -top_k)[-top_k:]
            top_idx = top_idx[np.argsort(final_scores[top_idx])[::-1]]
        else:
            top_idx = np.argsort(final_scores)[::-1][:top_k]

        valid_idx = [i for i in top_idx if final_scores[i] > -900]

        # Include detected country in results
        detected_country = list(ocr_countries.keys())[0] if ocr_countries else ""

        # Confidence: use the raw combined score directly.
        # The theoretical max is 1.0 (all signals perfect).
        # Scale so that a combined score of ~0.7 maps to ~90% confidence,
        # and scores below ~0.3 feel appropriately low.
        results = []
        for i in valid_idx:
            r = self.records[i]
            raw = final_scores[i]
            # Absolute confidence: raw score is already 0-1 range
            # (weighted sum of similarities each in [0,1]).
            # Apply a mild sigmoid to spread the useful range.
            confidence = 1.0 / (1.0 + np.exp(-12 * (raw - 0.45)))
            bd = _breakdown.get(i, (0, 0, 0, 0))
            results.append({
                "image_id": r.image_id,
                "country": r.country,
                "sw_id": r.sw_id,
                "number": r.number,
                "group_title": r.group_title,
                "local_image": r.local_image,
                "detail_url": r.detail_url,
                "confidence": float(np.clip(confidence, 0, 1)),
                "cosine_sim": float(cnn_scores[i]),
                "detected_country": detected_country,
                "score_cnn": float(bd[0]),
                "score_color": float(bd[1]),
                "score_text": float(bd[2]),
                "score_country": float(bd[3]),
            })
        return results


    @staticmethod
    def _get_color_hist(img_path: str,
                        bins: tuple = (36, 12, 12)) -> Optional[np.ndarray]:
        """Compute normalised HSV histogram for a reference image."""
        import cv2
        try:
            img = cv2.imread(img_path, cv2.IMREAD_COLOR)
            if img is None:
                return None
            h, w = img.shape[:2]
            if max(h, w) > 256:
                scale = 256 / max(h, w)
                img = cv2.resize(img, (int(w * scale), int(h * scale)),
                                 interpolation=cv2.INTER_AREA)
            hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
            hist = cv2.calcHist([hsv], [0, 1, 2], None, list(bins),
                                [0, 180, 0, 256, 0, 256])
            cv2.normalize(hist, hist)
            return hist.flatten().astype(np.float32)
        except Exception:
            return None

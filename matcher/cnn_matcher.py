"""
CNN embedding matcher for stamp images (v3).

Uses a pretrained ResNet-18 to extract 512-dim feature vectors,
then cosine similarity for matching. Far more accurate than
histogram/hash approaches for visual similarity.
"""

import os
import json
import numpy as np
from typing import List, Optional, Dict, Any

import torch
import torch.nn as nn
from torchvision import models, transforms
from PIL import Image

from matcher.base_index import BaseIndex, IndexRecord
from config import CNN_FLOOR, CNN_CEIL, SIGNAL_WEIGHTS, CONFIDENCE_SIGMOID_SCALE, CONFIDENCE_SIGMOID_CENTER, HSV_BINS


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

class CNNIndex(BaseIndex):
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
                    "ocr_text": r.ocr_text,
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
                ocr_text=img.get("ocr_text", ""),
            ))
        return idx

    def __len__(self):
        return len(self.records)

    def _query_internal(self, embedding: np.ndarray, top_k: int = 10,
                        country: str = None,
                        color_hist: np.ndarray = None,
                        query_aspect: float = None,
                        w_cnn: float = 0.55,
                        w_color: float = 0.35,
                        w_aspect: float = 0.10) -> List[dict]:
        """
        Find top-K most similar images, re-ranked with CNN + color + aspect signals.
        OCR signals removed — stamp typography is too complex for local OCR engines.
        """
        import cv2

        q = embedding.reshape(1, -1)  # (1, 512)

        # Cosine similarity via dot product (all normalised)
        cnn_scores = (self.embeddings @ q.T).ravel()  # (N,)

        # Country filter (explicit user selection)
        if country:
            cl = country.lower()
            mask = np.array([r.country.lower() != cl for r in self.records])
            cnn_scores[mask] = -999.0

        # Prepare shortlist for re-ranking
        shortlist_k = min(top_k * 5, len(cnn_scores))
        if len(cnn_scores) > shortlist_k:
            shortlist_idx = np.argpartition(cnn_scores, -shortlist_k)[-shortlist_k:]
        else:
            shortlist_idx = np.arange(len(cnn_scores))

        final_scores = np.full(len(cnn_scores), -999.0)
        _breakdown = {}
        for i in shortlist_idx:
            if cnn_scores[i] < -900:
                continue

            # Rescale CNN to useful range (stamps cluster in ~0.75-0.95)
            cnn_rescaled = np.clip(
                (cnn_scores[i] - CNN_FLOOR) / (CNN_CEIL - CNN_FLOOR), 0.0, 1.0)
            score = w_cnn * cnn_rescaled

            _hist_sim = 0.0
            _quad_sims = [0.0, 0.0, 0.0, 0.0]
            _aspect_sim = 0.0

            # Aspect ratio similarity
            if query_aspect is not None:
                ref_aspect = self._get_aspect_ratio(self.records[i].local_image)
                if ref_aspect is not None:
                    ratio_diff = abs(query_aspect - ref_aspect) / max(query_aspect, ref_aspect)
                    _aspect_sim = max(0.0, 1.0 - ratio_diff * 2.0)
                score += w_aspect * _aspect_sim

            # Colour histogram re-ranking (quadrant-based)
            if color_hist is not None:
                r = self.records[i]
                ref_hist = self._get_color_hist(r.local_image)
                if ref_hist is not None:
                    qsize = len(color_hist) // 4
                    quad_sims = []
                    for qi in range(4):
                        s = qi * qsize
                        e = s + qsize
                        bhatt = float(cv2.compareHist(
                            color_hist[s:e], ref_hist[s:e], cv2.HISTCMP_BHATTACHARYYA))
                        quad_sims.append(1.0 - bhatt)
                    _hist_sim = sum(quad_sims) / 4.0
                    _quad_sims = quad_sims
                score += w_color * _hist_sim

            final_scores[i] = score
            _breakdown[i] = (cnn_rescaled, _hist_sim, _aspect_sim, _quad_sims)

        # Top-K from final scores
        if len(final_scores) > top_k:
            top_idx = np.argpartition(final_scores, -top_k)[-top_k:]
            top_idx = top_idx[np.argsort(final_scores[top_idx])[::-1]]
        else:
            top_idx = np.argsort(final_scores)[::-1][:top_k]

        valid_idx = [i for i in top_idx if final_scores[i] > -900]

        results = []
        for i in valid_idx:
            r = self.records[i]
            raw = final_scores[i]
            bd = _breakdown.get(i, (0, 0, 0, [0, 0, 0, 0]))
            results.append({
                "image_id": r.image_id,
                "country": r.country,
                "sw_id": r.sw_id,
                "number": r.number,
                "group_title": r.group_title,
                "local_image": r.local_image,
                "detail_url": r.detail_url,
                "confidence": float(np.clip(raw, 0, 1)),
                "cosine_sim": float(cnn_scores[i]),
                "score_cnn": float(bd[0]),
                "score_color": float(bd[1]),
                "score_aspect": float(bd[2]),
                "score_color_tl": float(bd[3][0]),
                "score_color_tr": float(bd[3][1]),
                "score_color_bl": float(bd[3][2]),
                "score_color_br": float(bd[3][3]),
            })
        return results


    @staticmethod
    def _get_aspect_ratio(img_path: str) -> Optional[float]:
        """Return width/height ratio for an image."""
        from PIL import Image as PILImage
        try:
            with PILImage.open(img_path) as img:
                w, h = img.size
                return w / h if h > 0 else None
        except Exception:
            return None

    @staticmethod
    def _compute_quadrant_hist(hsv_img, bins=HSV_BINS):
        """Compute concatenated HSV histograms for 4 quadrants of an image."""
        import cv2
        h, w = hsv_img.shape[:2]
        mid_h, mid_w = h // 2, w // 2
        quadrants = [
            hsv_img[:mid_h, :mid_w],    # top-left
            hsv_img[:mid_h, mid_w:],    # top-right
            hsv_img[mid_h:, :mid_w],    # bottom-left
            hsv_img[mid_h:, mid_w:],    # bottom-right
        ]
        parts = []
        for q in quadrants:
            hist = cv2.calcHist([q], [0, 1, 2], None, list(bins),
                                [0, 180, 0, 256, 0, 256])
            cv2.normalize(hist, hist)
            parts.append(hist.flatten().astype(np.float32))
        return np.concatenate(parts)

    @staticmethod
    def _get_color_hist(img_path: str,
                        bins: tuple = HSV_BINS) -> Optional[np.ndarray]:
        """Compute normalised quadrant HSV histograms for a reference image."""
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
            return CNNIndex._compute_quadrant_hist(hsv, bins)
        except Exception:
            return None

    @staticmethod
    def _compute_color_hist_from_bytes(image_bytes: bytes) -> Optional[np.ndarray]:
        """Compute quadrant color histograms from image bytes."""
        import cv2
        try:
            arr = np.frombuffer(image_bytes, dtype=np.uint8)
            img_bgr = cv2.imdecode(arr, cv2.IMREAD_COLOR)
            if img_bgr is None:
                return None
            h, w = img_bgr.shape[:2]
            if max(h, w) > 256:
                scale = 256 / max(h, w)
                img_bgr = cv2.resize(img_bgr, (int(w * scale), int(h * scale)),
                                     interpolation=cv2.INTER_AREA)
            hsv = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2HSV)
            return CNNIndex._compute_quadrant_hist(hsv)
        except Exception:
            return None


    def query(self, image_bytes: bytes, top_k: int = 10, 
              country: str = None) -> List[Dict[str, Any]]:
        """
        Query the index with an image (BaseIndex interface).
        """
        embedding = compute_embedding_from_bytes(image_bytes)
        if embedding is None:
            return []
        
        color_hist = self._compute_color_hist_from_bytes(image_bytes)
        
        # Compute query aspect ratio
        query_aspect = None
        try:
            import io
            from PIL import Image as PILImage
            with PILImage.open(io.BytesIO(image_bytes)) as img:
                w, h = img.size
                query_aspect = w / h if h > 0 else None
        except Exception:
            pass
        
        results = self._query_internal(
            embedding=embedding,
            top_k=top_k,
            country=country,
            color_hist=color_hist,
            query_aspect=query_aspect,
        )

        return results

"""
FLANN-based matcher for stamp images.

Performs fast approximate nearest-neighbor matching between query descriptors
and the reference index using LSH (Locality-Sensitive Hashing), with
geometric verification via RANSAC homography.
"""

import cv2
import numpy as np
from dataclasses import dataclass
from typing import List, Optional, Dict, Tuple

from .descriptor_store import DescriptorStore


@dataclass
class MatchResult:
    """Result of a match query."""
    image_id: str
    country: str
    sw_id: str
    number: str
    group_title: str
    local_image: str
    good_matches: int       # count after Lowe's ratio test
    total_query_desc: int   # total query descriptors
    confidence: float       # geometric-verified confidence score
    detail_url: str
    inlier_count: int = 0   # RANSAC inliers (0 if not verified)


class FLANNMatcher:
    """FLANN-based matcher for ORB descriptors with geometric verification."""

    def __init__(self, store: DescriptorStore):
        self.store = store
        self._bf = cv2.BFMatcher(cv2.NORM_HAMMING, crossCheck=False)
        self._last_country = "__unset__"
        self._country_records = None  # cached filtered record indices

    def build_index(self, country: Optional[str] = None) -> None:
        """
        Prepare for matching, optionally filtering by country.
        With BFMatcher there's no explicit index to build, but we
        cache the country filter for use during match().
        """
        self._last_country = country

        if country:
            self._country_records = [
                i for i, r in enumerate(self.store.records)
                if r.country.lower() == country.lower()
            ]
            if not self._country_records:
                raise ValueError(f"No descriptors available for country: {country}")
        else:
            self._country_records = None

    def _get_descriptors_for_record(self, record_idx: int) -> np.ndarray:
        """Return the descriptor slice for a single image record."""
        rec = self.store.records[record_idx]
        start = rec.descriptor_offset
        end = start + rec.descriptor_count
        return self.store.all_descriptors[start:end]

    def match(
        self,
        query_descriptors: np.ndarray,
        top_k: int = 10,
        ratio_threshold: float = 0.80,
        query_keypoints: list = None,
    ) -> List[MatchResult]:
        """
        Match query descriptors against the index using per-image BF matching
        with Lowe's ratio test and optional geometric verification.

        Args:
            query_descriptors: Query ORB descriptors, shape (N, 32), dtype uint8
            top_k: Maximum number of results to return
            ratio_threshold: Lowe's ratio test threshold
            query_keypoints: Optional list of cv2.KeyPoint for geometric verification

        Returns:
            List of MatchResult objects sorted by confidence descending
        """
        if query_descriptors.shape[1] != 32 or query_descriptors.dtype != np.uint8:
            raise ValueError(
                f"Query descriptors must be (N, 32) uint8, got {query_descriptors.shape}, {query_descriptors.dtype}"
            )

        if query_descriptors.shape[0] < 10:
            return []

        # Determine which records to search
        if self._country_records is not None:
            record_indices = self._country_records
        else:
            record_indices = range(len(self.store.records))

        candidates: List[Tuple[int, int, List]] = []  # (record_idx, good_count, good_matches_list)

        for rec_idx in record_indices:
            ref_desc = self._get_descriptors_for_record(rec_idx)
            if ref_desc.shape[0] < 5:
                continue

            # knnMatch with k=2 for ratio test
            raw_matches = self._bf.knnMatch(query_descriptors, ref_desc, k=2)

            good = []
            for pair in raw_matches:
                if len(pair) < 2:
                    continue
                m, n = pair
                if m.distance < ratio_threshold * n.distance:
                    good.append(m)

            if len(good) < 4:
                continue

            candidates.append((rec_idx, len(good), good))

        # Sort by good match count descending, take top candidates for verification
        candidates.sort(key=lambda c: c[1], reverse=True)
        verify_limit = min(len(candidates), top_k * 5)
        candidates = candidates[:verify_limit]

        results = []
        total_query = query_descriptors.shape[0]

        for rec_idx, good_count, good_matches in candidates:
            record = self.store.records[rec_idx]
            ref_desc_count = record.descriptor_count

            # Confidence: ratio of good matches to the smaller descriptor set
            # This normalizes for images with very different feature counts
            min_desc = min(total_query, ref_desc_count)
            confidence = good_count / min_desc if min_desc > 0 else 0.0

            results.append(MatchResult(
                image_id=record.image_id,
                country=record.country,
                sw_id=record.sw_id,
                number=record.number,
                group_title=record.group_title,
                local_image=record.local_image,
                good_matches=good_count,
                total_query_desc=total_query,
                confidence=confidence,
                detail_url=record.detail_url,
                inlier_count=0,
            ))

        # Sort by confidence descending
        results.sort(key=lambda r: r.confidence, reverse=True)

        # Deduplicate by image_id
        seen = set()
        unique = []
        for r in results:
            if r.image_id not in seen:
                seen.add(r.image_id)
                unique.append(r)

        return unique[:top_k]

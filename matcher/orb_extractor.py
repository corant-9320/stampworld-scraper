"""
ORB feature extraction for stamp images.

Extracts ORB keypoints and descriptors from preprocessed images.
"""

import cv2
import numpy as np
from dataclasses import dataclass
from typing import Optional


@dataclass
class FeatureResult:
    """Container for ORB feature extraction results."""
    keypoints: list  # list of cv2.KeyPoint (not stored, used transiently)
    descriptors: np.ndarray  # shape (N, 32), dtype uint8
    num_features: int


def create_orb_detector(max_features: int = 500, n_levels: int = 8) -> cv2.ORB:
    """
    Create a configured ORB detector instance.
    
    Args:
        max_features: Maximum number of features to retain
        n_levels: Number of pyramid levels
    
    Returns:
        Configured ORB detector
    """
    return cv2.ORB_create(
        nfeatures=max_features,
        scaleFactor=1.2,
        nlevels=n_levels,
        edgeThreshold=31,
        firstLevel=0,
        WTA_K=2,
        scoreType=cv2.ORB_HARRIS_SCORE,
        patchSize=31,
        fastThreshold=20
    )


def extract_features(
    gray_image: np.ndarray,
    orb: Optional[cv2.ORB] = None,
    max_features: int = 500
) -> Optional[FeatureResult]:
    """
    Extract ORB features from a grayscale image.
    
    Args:
        gray_image: Preprocessed grayscale image (uint8)
        orb: Optional pre-configured ORB detector
        max_features: Maximum number of features to extract
    
    Returns:
        FeatureResult with descriptors, or None if fewer than 10 keypoints found
    """
    if orb is None:
        orb = create_orb_detector(max_features=max_features)
    
    # Detect keypoints and compute descriptors
    keypoints, descriptors = orb.detectAndCompute(gray_image, None)
    
    if descriptors is None or len(keypoints) < 10:
        return None
    
    # Ensure descriptors are uint8 and have correct shape
    if descriptors.dtype != np.uint8:
        descriptors = descriptors.astype(np.uint8)
    
    # ORB descriptors are 32 bytes each
    if descriptors.shape[1] != 32:
        # This shouldn't happen with ORB, but just in case
        return None
    
    return FeatureResult(
        keypoints=keypoints,
        descriptors=descriptors,
        num_features=len(keypoints)
    )
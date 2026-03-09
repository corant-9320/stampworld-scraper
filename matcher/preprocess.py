"""
Image preprocessing for stamp matching.

Normalizes uploaded photos and reference images into a consistent format
for ORB feature extraction. Handles varying lighting, rotation, scale,
and background noise.
"""

import cv2
import numpy as np
from typing import Optional


def preprocess_image(
    image_bytes: bytes,
    target_size: int = 512,
    apply_clahe: bool = True
) -> Optional[np.ndarray]:
    """
    Decode, resize, convert to grayscale, and enhance contrast.
    
    Args:
        image_bytes: Raw bytes of a JPEG or PNG image
        target_size: Longest edge size after resizing (preserves aspect ratio)
        apply_clahe: Whether to apply CLAHE contrast enhancement
    
    Returns:
        Single-channel uint8 numpy array ready for ORB, or None if decoding fails
    """
    # Decode image bytes
    arr = np.frombuffer(image_bytes, dtype=np.uint8)
    img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    if img is None:
        return None
    
    # Convert to grayscale
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    
    # Resize preserving aspect ratio (longest edge = target_size)
    h, w = gray.shape[:2]
    if max(h, w) > target_size:
        scale = target_size / max(h, w)
        new_w, new_h = int(w * scale), int(h * scale)
        gray = cv2.resize(gray, (new_w, new_h), interpolation=cv2.INTER_AREA)
    
    # CLAHE for contrast normalization
    if apply_clahe:
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
        gray = clahe.apply(gray)
    
    return gray


def remove_background(gray_image: np.ndarray, border_pct: float = 0.05) -> np.ndarray:
    """
    Attempt to crop out uniform borders / album page background
    using edge detection + contour finding.
    
    Args:
        gray_image: Grayscale image (uint8)
        border_pct: Percentage of image dimensions to consider as border
    
    Returns:
        Cropped grayscale image, or original if no clear stamp boundary found
    """
    h, w = gray_image.shape[:2]
    
    # Edge detection
    edges = cv2.Canny(gray_image, 50, 150)
    
    # Find contours
    contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    if not contours:
        return gray_image
    
    # Find largest contour by area
    largest_contour = max(contours, key=cv2.contourArea)
    x, y, cw, ch = cv2.boundingRect(largest_contour)
    
    # Only crop if contour is significantly smaller than image
    # and not too close to borders
    min_border = int(min(h, w) * border_pct)
    if (cw < w - 2 * min_border and ch < h - 2 * min_border and
        x > min_border and y > min_border and
        x + cw < w - min_border and y + ch < h - min_border):
        return gray_image[y:y+ch, x:x+cw]
    
    return gray_image
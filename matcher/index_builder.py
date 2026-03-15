"""
Shared index building logic for v2 and v3 index builders.

Extracts common stamp loading, filtering, and progress reporting.
"""
import glob
import json
import os
import time
from typing import List, Tuple, Dict, Any, Optional


def load_stamps(output_dir: str = "output", country_filter: Optional[str] = None) -> List[Tuple[Dict, str]]:
    """
    Load stamp records, filter by country, validate image paths.
    
    Returns list of (stamp_dict, image_path) tuples for stamps with valid images.
    """
    all_stamps = []
    for jp in sorted(glob.glob(os.path.join(output_dir, "stamps_*.json"))):
        with open(jp, encoding="utf-8") as f:
            data = json.load(f)
        stamps = data.get("stamps", [])
        all_stamps.extend(stamps)
    
    if country_filter:
        # Match on country field (case-insensitive, handle hyphens/spaces)
        target = country_filter.lower().replace("-", " ").replace("_", " ")
        all_stamps = [s for s in all_stamps
                      if s.get("country", "").lower().replace("-", " ").replace("_", " ") == target]
    
    # Pre-filter to stamps with valid image paths
    valid_stamps = []
    for stamp in all_stamps:
        local_image = stamp.get("local_image", "")
        if not local_image:
            continue
        img_path = local_image.replace("\\", "/")
        if os.path.exists(img_path):
            valid_stamps.append((stamp, img_path))
    
    return valid_stamps


def build_with_progress(stamps: List[Tuple[Dict, str]], 
                       extract_fn, 
                       save_fn,
                       batch_size: int = 1,
                       progress_interval: int = 100) -> None:
    """
    Generic build loop with progress reporting.
    
    Args:
        stamps: List of (stamp_dict, image_path) tuples
        extract_fn: Function that takes image path and returns features/embeddings
        save_fn: Function that takes (stamp_dict, features) and saves to index
        batch_size: Number of images to process at once (for batch processing)
        progress_interval: How often to print progress updates
    """
    total = len(stamps)
    if total == 0:
        print("No stamps with valid images found!")
        return
    
    print(f"Processing {total} stamps...")
    t0 = time.time()
    processed = 0
    
    if batch_size > 1:
        # Batch processing
        for batch_start in range(0, total, batch_size):
            batch = stamps[batch_start:batch_start + batch_size]
            paths = [p for _, p in batch]
            stamp_map = {p: s for s, p in batch}
            
            results = extract_fn(paths)
            
            for path, features in results:
                stamp = stamp_map[path]
                save_fn(stamp, features)
                processed += 1
            
            if (batch_start + batch_size) % progress_interval == 0 or batch_start + batch_size >= total:
                elapsed = time.time() - t0
                rate = processed / elapsed if elapsed > 0 else 0
                eta = (total - processed) / rate if rate > 0 else 0
                print(f"  {processed}/{total} ({processed/total*100:.1f}%) "
                      f"| {rate:.1f} img/s | ETA: {eta:.0f}s")
    else:
        # Single image processing
        for i, (stamp, img_path) in enumerate(stamps):
            try:
                features = extract_fn(img_path)
                save_fn(stamp, features)
                processed += 1
                
                if (i + 1) % progress_interval == 0 or i + 1 == total:
                    elapsed = time.time() - t0
                    rate = processed / elapsed if elapsed > 0 else 0
                    eta = (total - processed) / rate if rate > 0 else 0
                    print(f"  {processed}/{total} ({processed/total*100:.1f}%) "
                          f"| {rate:.1f} img/s | ETA: {eta:.0f}s")
            except Exception as e:
                print(f"  Error processing {img_path}: {e}")
                continue
    
    elapsed_total = time.time() - t0
    print(f"Done! Processed {processed} stamps in {elapsed_total:.1f}s "
          f"({processed/elapsed_total:.1f} img/s)")
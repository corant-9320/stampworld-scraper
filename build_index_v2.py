#!/usr/bin/env python3
"""
Build histogram + perceptual-hash index for stamp matching (v2).

Much more accurate than ORB for matching stamp photos against
catalogue reference images.

Usage:
    python build_index_v2.py
    python build_index_v2.py --country Great-Britain
"""

import argparse
import os
import sys

import cv2
import numpy as np

from matcher.histogram_matcher import (
    compute_features, HistogramIndex, IndexRecord,
)
from matcher.index_builder import load_stamps


def main():
    parser = argparse.ArgumentParser(description="Build v2 histogram index")
    parser.add_argument("--output-dir", default="output")
    parser.add_argument("--index-dir", default="descriptor_index")
    parser.add_argument("--country", default=None)
    args = parser.parse_args()

    # Load stamp records using shared function
    valid_stamps = load_stamps(args.output_dir, country_filter=args.country)
    
    if not valid_stamps:
        print("No stamps found!")
        return

    index = HistogramIndex()
    processed = 0
    skipped = 0

    for i, (stamp, img_path) in enumerate(valid_stamps):
        if (i + 1) % 200 == 0:
            print(f"  {i+1}/{len(valid_stamps)} ...")

        try:
            img_bgr = cv2.imread(img_path, cv2.IMREAD_COLOR)
            if img_bgr is None:
                skipped += 1
                continue

            # Resize longest edge to 256 for speed
            h, w = img_bgr.shape[:2]
            if max(h, w) > 256:
                scale = 256 / max(h, w)
                img_bgr = cv2.resize(img_bgr, (int(w * scale), int(h * scale)),
                                     interpolation=cv2.INTER_AREA)

            gray = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2GRAY)
            features = compute_features(gray, color_bgr=img_bgr)

            image_id = os.path.splitext(os.path.basename(img_path))[0]
            rec = IndexRecord(
                idx=len(index.records),
                image_id=image_id,
                country=stamp.get("country", ""),
                sw_id=stamp.get("sw_id", ""),
                number=stamp.get("number", ""),
                group_title=stamp.get("group_title", ""),
                local_image=img_path,
                detail_url=stamp.get("detail_url", ""),
            )
            index.add(rec, features)
            processed += 1

        except Exception as e:
            print(f"  WARN: {img_path}: {e}")
            skipped += 1

    print(f"\nDone: {processed} indexed, {skipped} skipped")
    print(f"Saving to {args.index_dir} ...")
    index.save(args.index_dir)
    print("Saved.")


if __name__ == "__main__":
    main()

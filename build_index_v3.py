#!/usr/bin/env python3
"""
Build CNN embedding index for stamp matching (v3).

Uses ResNet-18 to extract 512-dim feature vectors per image.
Much more accurate than histogram/hash approaches.

Usage:
    python build_index_v3.py
    python build_index_v3.py --country Great-Britain
"""

import argparse
import glob
import json
import os
import sys
import time

import numpy as np

from matcher.cnn_matcher import compute_embedding, compute_embeddings_batch, CNNIndex, IndexRecord


BATCH_SIZE = 32


def main():
    parser = argparse.ArgumentParser(description="Build v3 CNN embedding index")
    parser.add_argument("--output-dir", default="output")
    parser.add_argument("--index-dir", default="descriptor_index")
    parser.add_argument("--country", default=None,
                        help="Filter to a single country (e.g. Great-Britain)")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    args = parser.parse_args()

    # Load stamp records
    all_stamps = []
    for jp in sorted(glob.glob(os.path.join(args.output_dir, "stamps_*.json"))):
        with open(jp, encoding="utf-8") as f:
            data = json.load(f)
        stamps = data.get("stamps", [])
        print(f"  {os.path.basename(jp)}: {len(stamps)} stamps")
        all_stamps.extend(stamps)

    if args.country:
        # Match on country field (case-insensitive, handle hyphens/spaces)
        target = args.country.lower().replace("-", " ").replace("_", " ")
        all_stamps = [s for s in all_stamps
                      if s.get("country", "").lower().replace("-", " ").replace("_", " ") == target]
        print(f"Filtered to {len(all_stamps)} stamps for {args.country}")

    if not all_stamps:
        print("No stamps found!")
        return

    # Pre-filter to stamps with valid image paths
    valid_stamps = []
    for stamp in all_stamps:
        local_image = stamp.get("local_image", "")
        if not local_image:
            continue
        img_path = local_image.replace("\\", "/")
        if os.path.exists(img_path):
            valid_stamps.append((stamp, img_path))

    print(f"{len(valid_stamps)} stamps with images (of {len(all_stamps)} total)")

    index = CNNIndex()
    processed = 0
    t0 = time.time()
    bs = args.batch_size

    for batch_start in range(0, len(valid_stamps), bs):
        batch = valid_stamps[batch_start:batch_start + bs]
        paths = [p for _, p in batch]
        stamp_map = {p: s for s, p in batch}

        results = compute_embeddings_batch(paths)

        for path, embedding in results:
            stamp = stamp_map[path]
            image_id = os.path.splitext(os.path.basename(path))[0]
            rec = IndexRecord(
                idx=len(index.records),
                image_id=image_id,
                country=stamp.get("country", ""),
                sw_id=stamp.get("sw_id", ""),
                number=stamp.get("number", ""),
                group_title=stamp.get("group_title", ""),
                local_image=path,
                detail_url=stamp.get("detail_url", ""),
            )
            index.add(rec, embedding)
            processed += 1

        if (batch_start + bs) % (bs * 5) == 0 or batch_start + bs >= len(valid_stamps):
            elapsed = time.time() - t0
            rate = processed / elapsed if elapsed > 0 else 0
            eta = (len(valid_stamps) - processed) / rate if rate > 0 else 0
            print(f"  {processed}/{len(valid_stamps)} ... ({rate:.0f} img/s, ETA {eta:.0f}s)")

    elapsed = time.time() - t0
    print(f"\nDone: {processed} indexed in {elapsed:.0f}s ({processed/elapsed:.0f} img/s)")
    print(f"Saving to {args.index_dir} ...")
    index.save(args.index_dir)
    print("Saved.")


if __name__ == "__main__":
    main()

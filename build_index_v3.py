#!/usr/bin/env python3
"""
Build CNN embedding index for stamp matching (v3).

Uses ResNet-18 to extract 512-dim feature vectors per image.

Usage:
    python build_index_v3.py
    python build_index_v3.py --country Great-Britain
"""

import argparse
import json
import os
import time

import numpy as np

from matcher.cnn_matcher import compute_embeddings_batch, CNNIndex, IndexRecord
from matcher.index_builder import load_stamps
from config import CNN_BATCH_SIZE


def main():
    parser = argparse.ArgumentParser(description="Build v3 CNN embedding index")
    parser.add_argument("--output-dir", default="output")
    parser.add_argument("--index-dir", default="descriptor_index")
    parser.add_argument("--country", default=None,
                        help="Filter to a single country (e.g. Great-Britain)")
    parser.add_argument("--batch-size", type=int, default=CNN_BATCH_SIZE)
    parser.add_argument("--sample", type=int, default=None,
                        help="Only process first N stamps (for testing)")
    args = parser.parse_args()

    valid_stamps = load_stamps(args.output_dir, country_filter=args.country)

    if not valid_stamps:
        print("No stamps found!")
        return

    print(f"{len(valid_stamps)} stamps with images")
    if args.sample:
        valid_stamps = valid_stamps[:args.sample]
        print(f"Sampling first {len(valid_stamps)} stamps")

    index = CNNIndex()
    processed = 0
    t0 = time.time()
    bs = args.batch_size or CNN_BATCH_SIZE

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
                ocr_text="",
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

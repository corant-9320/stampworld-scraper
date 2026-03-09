#!/usr/bin/env python3
"""
Build descriptor index for stamp image matching.

Scans all stamp JSON files, extracts ORB descriptors for each image,
and writes the consolidated index to disk.
"""

import argparse
import glob
import json
import os
import sys
from pathlib import Path
from typing import List, Dict, Any

import numpy as np

from matcher.preprocess import preprocess_image
from matcher.orb_extractor import create_orb_detector, extract_features
from matcher.descriptor_store import DescriptorStore, ImageRecord


def load_stamp_records(output_dir: str = "output") -> List[Dict[str, Any]]:
    """
    Load all stamp records from JSON files.
    
    Args:
        output_dir: Directory containing stamps_*.json files
    
    Returns:
        List of stamp records
    """
    records = []
    json_pattern = os.path.join(output_dir, "stamps_*.json")
    
    for json_path in glob.glob(json_pattern):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            stamps = data.get("stamps", [])
            print(f"  Loaded {len(stamps)} stamps from {os.path.basename(json_path)}")
            records.extend(stamps)
        except Exception as e:
            print(f"  Warning: Failed to load {json_path}: {e}")
    
    return records


def build_index(
    output_dir: str = "output",
    images_dir: str = "stamp_images",
    index_dir: str = "descriptor_index",
    country: str = None,
    max_features: int = 500,
    skip_existing: bool = False
) -> None:
    """
    Build descriptor index from stamp images.
    
    Args:
        output_dir: Directory containing stamps_*.json files
        images_dir: Root directory for stamp images
        index_dir: Directory to save index files
        country: If specified, only index images from this country
        max_features: Maximum ORB features per image
        skip_existing: Skip images already in existing index
    """
    # Check if index already exists
    if skip_existing and os.path.exists(os.path.join(index_dir, "manifest.json")):
        print(f"Index already exists at {index_dir}, skipping...")
        return
    
    # Load stamp records
    print("Loading stamp records...")
    all_stamps = load_stamp_records(output_dir)
    
    if not all_stamps:
        print("No stamp records found!")
        return
    
    # Filter by country if specified
    if country:
        all_stamps = [s for s in all_stamps if s.get("country", "").lower() == country.lower()]
        if not all_stamps:
            print(f"No stamps found for country: {country}")
            return
        print(f"Filtered to {len(all_stamps)} stamps from {country}")
    
    # Initialize components
    orb = create_orb_detector(max_features=max_features)
    store = DescriptorStore()
    
    # Process each stamp
    print(f"\nProcessing {len(all_stamps)} stamps...")
    processed = 0
    skipped = 0
    failed = 0
    
    for i, stamp in enumerate(all_stamps):
        if (i + 1) % 100 == 0:
            print(f"  Processed {i+1}/{len(all_stamps)} stamps...")
        
        local_image = stamp.get("local_image", "")
        if not local_image:
            skipped += 1
            continue
        
        # Normalize path separators
        img_path = local_image.replace("\\", "/")
        
        # Check if image exists
        if not os.path.exists(img_path):
            # Try relative to images_dir
            rel_path = os.path.join(images_dir, img_path)
            if os.path.exists(rel_path):
                img_path = rel_path
            else:
                skipped += 1
                continue
        
        try:
            # Read and preprocess image
            with open(img_path, "rb") as f:
                image_bytes = f.read()
            
            gray = preprocess_image(image_bytes, target_size=512, apply_clahe=True)
            if gray is None:
                failed += 1
                continue
            
            # Extract features
            result = extract_features(gray, orb=orb, max_features=max_features)
            if result is None:
                failed += 1
                continue
            
            # Derive image_id from filename (without extension)
            image_id = os.path.splitext(os.path.basename(img_path))[0]
            
            # Create record
            record = ImageRecord(
                image_id=image_id,
                country=stamp.get("country", ""),
                sw_id=stamp.get("sw_id", ""),
                number=stamp.get("number", ""),
                group_title=stamp.get("group_title", ""),
                local_image=img_path,
                detail_url=stamp.get("detail_url", ""),
                descriptor_offset=0,  # will be set by store.add_image
                descriptor_count=result.num_features
            )
            
            # Add to store
            store.add_image(record, result.descriptors)
            processed += 1
            
        except Exception as e:
            print(f"  Warning: Failed to process {img_path}: {e}")
            failed += 1
            continue
    
    # Save index
    if len(store) == 0:
        print("\nNo images were successfully indexed!")
        return
    
    print(f"\nIndexing complete:")
    print(f"  Processed: {processed}")
    print(f"  Skipped (no image): {skipped}")
    print(f"  Failed (no features): {failed}")
    print(f"  Total indexed: {len(store)} images")
    print(f"  Total descriptors: {store.total_descriptors()}")
    
    print(f"\nSaving index to {index_dir}...")
    store.save(index_dir)
    print("Done!")


def main():
    parser = argparse.ArgumentParser(
        description="Build ORB descriptor index for stamp image matching"
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Directory containing stamps_*.json files (default: output)"
    )
    parser.add_argument(
        "--images-dir",
        default="stamp_images",
        help="Root directory for stamp images (default: stamp_images)"
    )
    parser.add_argument(
        "--index-dir",
        default="descriptor_index",
        help="Directory to save index files (default: descriptor_index)"
    )
    parser.add_argument(
        "--country",
        help="Only index images from this country"
    )
    parser.add_argument(
        "--max-features",
        type=int,
        default=500,
        help="Maximum ORB features per image (default: 500)"
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip building if index already exists"
    )
    
    args = parser.parse_args()
    
    try:
        build_index(
            output_dir=args.output_dir,
            images_dir=args.images_dir,
            index_dir=args.index_dir,
            country=args.country,
            max_features=args.max_features,
            skip_existing=args.skip_existing
        )
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
"""
Descriptor store for ORB feature indexing.

Persists and loads pre-computed descriptor index for all reference images.
"""

import json
import numpy as np
import os
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import List, Optional


@dataclass
class ImageRecord:
    """Metadata for an indexed image."""
    image_id: str           # e.g. "great-britain_sw0001_A"
    country: str            # e.g. "Great-Britain"
    sw_id: str              # e.g. "great-britain_1"
    number: str             # stamp number
    group_title: str
    local_image: str        # relative path in stamp_images/
    detail_url: str
    descriptor_offset: int  # start row in the global descriptor matrix
    descriptor_count: int   # number of descriptors for this image


class DescriptorStore:
    """Stores ORB descriptors for all reference images."""
    
    def __init__(self):
        self.records: List[ImageRecord] = []
        self.all_descriptors: Optional[np.ndarray] = None  # shape (total_N, 32), uint8
        self.image_id_for_row: Optional[np.ndarray] = None  # shape (total_N,), maps row -> record index
    
    def add_image(self, record: ImageRecord, descriptors: np.ndarray) -> None:
        """
        Append an image's descriptors to the store.
        
        Args:
            record: Image metadata
            descriptors: ORB descriptors, shape (N, 32), dtype uint8
        
        Raises:
            ValueError: If descriptors have wrong shape or dtype
        """
        if descriptors.shape[1] != 32 or descriptors.dtype != np.uint8:
            raise ValueError(f"Descriptors must have shape (N, 32) and dtype uint8, got {descriptors.shape}, {descriptors.dtype}")
        
        if descriptors.shape[0] < 10:
            raise ValueError(f"Too few descriptors: {descriptors.shape[0]}, need at least 10")
        
        # Set offset to current total
        record.descriptor_offset = 0 if self.all_descriptors is None else self.all_descriptors.shape[0]
        record.descriptor_count = descriptors.shape[0]
        
        # Append descriptors
        if self.all_descriptors is None:
            self.all_descriptors = descriptors
            self.image_id_for_row = np.full(descriptors.shape[0], len(self.records), dtype=np.int32)
        else:
            self.all_descriptors = np.vstack([self.all_descriptors, descriptors])
            new_rows = np.full(descriptors.shape[0], len(self.records), dtype=np.int32)
            self.image_id_for_row = np.concatenate([self.image_id_for_row, new_rows])
        
        self.records.append(record)
    
    def save(self, directory: str) -> None:
        """
        Write descriptor store to disk.
        
        Args:
            directory: Directory to save index files
        
        Raises:
            ValueError: If store is empty
        """
        if not self.records or self.all_descriptors is None:
            raise ValueError("Cannot save empty descriptor store")
        
        os.makedirs(directory, exist_ok=True)
        
        # Save descriptor matrix
        descriptors_path = os.path.join(directory, "descriptors.npy")
        np.save(descriptors_path, self.all_descriptors)
        
        # Build manifest
        manifest = {
            "version": 1,
            "built_at": datetime.utcnow().isoformat() + "Z",
            "total_images": len(self.records),
            "total_descriptors": self.all_descriptors.shape[0],
            "orb_max_features": 500,  # Fixed for now, could be configurable
            "countries": sorted(set(r.country for r in self.records)),
            "images": [asdict(r) for r in self.records]
        }
        
        # Save manifest
        manifest_path = os.path.join(directory, "manifest.json")
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2)
    
    @classmethod
    def load(cls, directory: str) -> "DescriptorStore":
        """
        Load a previously saved descriptor store from disk.
        
        Args:
            directory: Directory containing index files
        
        Returns:
            Loaded DescriptorStore
        
        Raises:
            FileNotFoundError: If index files don't exist
            ValueError: If manifest validation fails
        """
        descriptors_path = os.path.join(directory, "descriptors.npy")
        manifest_path = os.path.join(directory, "manifest.json")
        
        if not os.path.exists(descriptors_path) or not os.path.exists(manifest_path):
            raise FileNotFoundError(f"Index files not found in {directory}")
        
        # Load manifest
        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest = json.load(f)
        
        # Validate manifest
        if manifest.get("version") != 1:
            raise ValueError(f"Unsupported manifest version: {manifest.get('version')}")
        
        # Load descriptors
        all_descriptors = np.load(descriptors_path)
        
        # Validate descriptor count matches manifest
        if all_descriptors.shape[0] != manifest["total_descriptors"]:
            raise ValueError(
                f"Descriptor count mismatch: manifest says {manifest['total_descriptors']}, "
                f"file has {all_descriptors.shape[0]}"
            )
        
        # Reconstruct store
        store = cls()
        store.all_descriptors = all_descriptors
        
        # Reconstruct records
        store.records = []
        for img_dict in manifest["images"]:
            record = ImageRecord(
                image_id=img_dict["image_id"],
                country=img_dict["country"],
                sw_id=img_dict["sw_id"],
                number=img_dict["number"],
                group_title=img_dict["group_title"],
                local_image=img_dict["local_image"],
                detail_url=img_dict["detail_url"],
                descriptor_offset=img_dict["descriptor_offset"],
                descriptor_count=img_dict["descriptor_count"]
            )
            store.records.append(record)
        
        # Reconstruct image_id_for_row mapping
        total_rows = all_descriptors.shape[0]
        store.image_id_for_row = np.zeros(total_rows, dtype=np.int32)
        
        for i, record in enumerate(store.records):
            start = record.descriptor_offset
            end = start + record.descriptor_count
            store.image_id_for_row[start:end] = i
        
        return store
    
    def get_country_mask(self, country: str) -> np.ndarray:
        """
        Return boolean mask over all_descriptors rows for a given country.
        
        Args:
            country: Country name to filter
        
        Returns:
            Boolean array where True indicates descriptor belongs to the country
        """
        if self.image_id_for_row is None:
            return np.array([], dtype=bool)
        
        mask = np.zeros(len(self.image_id_for_row), dtype=bool)
        for i, record in enumerate(self.records):
            if record.country == country:
                mask[self.image_id_for_row == i] = True
        
        return mask
    
    def __len__(self) -> int:
        """Number of indexed images."""
        return len(self.records)
    
    def total_descriptors(self) -> int:
        """Total number of descriptors in store."""
        return 0 if self.all_descriptors is None else self.all_descriptors.shape[0]
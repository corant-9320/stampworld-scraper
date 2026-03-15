"""
Base interface for all matcher indices.

Provides a common API for v2 (histogram) and v3 (CNN) indices
so the viewer can use them interchangeably.
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import numpy as np


class BaseIndex(ABC):
    """Abstract base class for all matcher indices."""
    
    @abstractmethod
    def query(self, image_bytes: bytes, top_k: int = 10, 
              country: str = None) -> List[Dict[str, Any]]:
        """
        Query the index with an image.
        
        Args:
            image_bytes: Raw image bytes (JPEG/PNG)
            top_k: Number of matches to return
            country: Optional country filter
            
        Returns:
            List of match dictionaries with keys:
            - image_id, country, sw_id, number, group_title, local_image, detail_url
            - score: confidence score (0-1)
            - distance: raw distance metric
        """
        pass
    
    @abstractmethod
    def save(self, directory: str) -> None:
        """Save index to directory."""
        pass
    
    @classmethod
    @abstractmethod
    def load(cls, directory: str) -> "BaseIndex":
        """Load index from directory."""
        pass
    
    @abstractmethod
    def __len__(self) -> int:
        """Return number of indexed images."""
        pass
    
    def get_stats(self) -> Dict[str, Any]:
        """Get index statistics."""
        return {
            "num_images": len(self),
            "index_type": self.__class__.__name__,
        }


class IndexRecord:
    """Common record format for all indices."""
    
    def __init__(self, idx: int, image_id: str, country: str, sw_id: str,
                 number: str, group_title: str, local_image: str, detail_url: str,
                 ocr_text: str = ""):
        self.idx = idx
        self.image_id = image_id
        self.country = country
        self.sw_id = sw_id
        self.number = number
        self.group_title = group_title
        self.local_image = local_image
        self.detail_url = detail_url
        self.ocr_text = ocr_text
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "idx": self.idx,
            "image_id": self.image_id,
            "country": self.country,
            "sw_id": self.sw_id,
            "number": self.number,
            "group_title": self.group_title,
            "local_image": self.local_image,
            "detail_url": self.detail_url,
            "ocr_text": self.ocr_text,
        }
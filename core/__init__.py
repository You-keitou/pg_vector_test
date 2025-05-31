"""
Core modules for text processing, embedding, and data handling.
"""

from .data_processor import DataProcessor
from .embedding import EmbeddingManager
from .logger import DualLogger
from .text_processing import TextProcessor

__all__ = ["TextProcessor", "EmbeddingManager", "DataProcessor", "DualLogger"]

"""
Document extraction module for DataBridge AI Librarian.

Provides capabilities to extract text and tabular data from:
- PDF documents
- Images (via OCR)
- Unstructured text
"""

from .pdf_extractor import PDFExtractor
from .ocr_extractor import OCRExtractor
from .table_parser import TableParser

__all__ = [
    "PDFExtractor",
    "OCRExtractor",
    "TableParser",
]

"""
PDF text extraction module.

Extracts text content from PDF files using pypdf library.
"""

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


@dataclass
class PageContent:
    """Content extracted from a single PDF page."""

    page_number: int
    text: str
    char_count: int
    word_count: int
    has_images: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PDFExtractionResult:
    """Result of PDF text extraction."""

    success: bool
    file_path: str
    total_pages: int
    pages_extracted: int
    pages: List[PageContent]
    metadata: Dict[str, Any]
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "success": self.success,
            "file_path": self.file_path,
            "total_pages": self.total_pages,
            "pages_extracted": self.pages_extracted,
            "pages": [
                {
                    "page_number": p.page_number,
                    "text": p.text,
                    "char_count": p.char_count,
                    "word_count": p.word_count,
                    "has_images": p.has_images,
                }
                for p in self.pages
            ],
            "metadata": self.metadata,
            "error": self.error,
        }

    @property
    def full_text(self) -> str:
        """Get all text from all pages combined."""
        return "\n\n".join(p.text for p in self.pages)


class PDFExtractor:
    """
    Extract text content from PDF documents.

    Uses pypdf library for text extraction. For scanned PDFs
    (image-based), use OCRExtractor instead.

    Example:
        ```python
        extractor = PDFExtractor()
        result = extractor.extract("report.pdf")

        if result.success:
            print(f"Extracted {result.pages_extracted} pages")
            print(result.full_text)
        ```
    """

    def __init__(self, max_pages: Optional[int] = None):
        """
        Initialize PDF extractor.

        Args:
            max_pages: Maximum pages to extract (None for all).
        """
        self.max_pages = max_pages

    def extract(
        self,
        file_path: Union[str, Path],
        pages: Optional[str] = "all",
    ) -> PDFExtractionResult:
        """
        Extract text from a PDF file.

        Args:
            file_path: Path to the PDF file.
            pages: Pages to extract:
                - "all": Extract all pages
                - "1,2,3": Specific page numbers (1-indexed)
                - "1-5": Page range (inclusive)

        Returns:
            PDFExtractionResult with extracted text and metadata.
        """
        try:
            import pypdf
        except ImportError:
            return PDFExtractionResult(
                success=False,
                file_path=str(file_path),
                total_pages=0,
                pages_extracted=0,
                pages=[],
                metadata={},
                error="pypdf is required. Install with: pip install 'databridge-librarian[ocr]'",
            )

        path = Path(file_path)
        if not path.exists():
            return PDFExtractionResult(
                success=False,
                file_path=str(file_path),
                total_pages=0,
                pages_extracted=0,
                pages=[],
                metadata={},
                error=f"File not found: {file_path}",
            )

        try:
            reader = pypdf.PdfReader(str(path))
            total_pages = len(reader.pages)

            # Parse page specification
            page_indices = self._parse_pages(pages, total_pages)

            # Apply max_pages limit
            if self.max_pages:
                page_indices = page_indices[: self.max_pages]

            # Extract metadata
            metadata = {}
            if reader.metadata:
                for key, value in reader.metadata.items():
                    # Remove leading slash from key names
                    clean_key = key.lstrip("/") if key.startswith("/") else key
                    metadata[clean_key] = str(value) if value else None

            # Extract text from each page
            extracted_pages: List[PageContent] = []
            for idx in page_indices:
                if 0 <= idx < total_pages:
                    page = reader.pages[idx]
                    text = page.extract_text() or ""

                    # Count words
                    words = text.split()
                    word_count = len(words)

                    # Check for images
                    has_images = False
                    if hasattr(page, "images") and page.images:
                        has_images = len(page.images) > 0

                    extracted_pages.append(
                        PageContent(
                            page_number=idx + 1,  # 1-indexed for users
                            text=text,
                            char_count=len(text),
                            word_count=word_count,
                            has_images=has_images,
                        )
                    )

            return PDFExtractionResult(
                success=True,
                file_path=str(file_path),
                total_pages=total_pages,
                pages_extracted=len(extracted_pages),
                pages=extracted_pages,
                metadata=metadata,
            )

        except Exception as e:
            return PDFExtractionResult(
                success=False,
                file_path=str(file_path),
                total_pages=0,
                pages_extracted=0,
                pages=[],
                metadata={},
                error=f"Failed to extract PDF: {e}",
            )

    def _parse_pages(self, pages: Optional[str], total_pages: int) -> List[int]:
        """
        Parse page specification into list of 0-indexed page numbers.

        Args:
            pages: Page specification string.
            total_pages: Total number of pages in document.

        Returns:
            List of 0-indexed page numbers.
        """
        if not pages or pages.lower() == "all":
            return list(range(total_pages))

        result = []

        # Split by comma
        parts = pages.replace(" ", "").split(",")

        for part in parts:
            if "-" in part:
                # Handle range (e.g., "1-5")
                start, end = part.split("-", 1)
                try:
                    start_idx = int(start) - 1  # Convert to 0-indexed
                    end_idx = int(end)  # End is inclusive, so don't subtract 1
                    result.extend(range(start_idx, end_idx))
                except ValueError:
                    continue
            else:
                # Handle single page number
                try:
                    page_num = int(part) - 1  # Convert to 0-indexed
                    result.append(page_num)
                except ValueError:
                    continue

        # Remove duplicates and sort
        return sorted(set(result))

    def extract_tables(
        self,
        file_path: Union[str, Path],
        pages: Optional[str] = "all",
    ) -> List[Dict[str, Any]]:
        """
        Extract tables from a PDF file.

        This is a basic implementation that attempts to detect
        tabular structures in the extracted text. For better
        table extraction, consider using specialized libraries
        like tabula-py or camelot.

        Args:
            file_path: Path to the PDF file.
            pages: Pages to extract tables from.

        Returns:
            List of dictionaries with detected table data.
        """
        result = self.extract(file_path, pages)

        if not result.success:
            return []

        tables = []
        for page in result.pages:
            # Simple table detection based on consistent spacing
            lines = page.text.split("\n")
            potential_table: List[List[str]] = []

            for line in lines:
                # Split by multiple spaces (potential column separator)
                cells = re.split(r"\s{2,}", line.strip())
                if len(cells) > 1:
                    potential_table.append(cells)
                elif potential_table:
                    # End of potential table
                    if len(potential_table) > 1:
                        tables.append(
                            {
                                "page": page.page_number,
                                "rows": potential_table,
                                "columns": len(potential_table[0])
                                if potential_table
                                else 0,
                            }
                        )
                    potential_table = []

            # Don't forget last table
            if len(potential_table) > 1:
                tables.append(
                    {
                        "page": page.page_number,
                        "rows": potential_table,
                        "columns": len(potential_table[0]) if potential_table else 0,
                    }
                )

        return tables

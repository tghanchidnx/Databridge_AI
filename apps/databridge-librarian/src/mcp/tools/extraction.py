"""
MCP Tools for Document Extraction in DataBridge AI Librarian.

Provides tools for extracting text and tabular data from:
- PDF documents
- Images (via OCR)
- Unstructured text
"""

from typing import Any, Dict, List, Optional

from fastmcp import FastMCP


def register_extraction_tools(mcp: FastMCP) -> None:
    """Register all document extraction MCP tools."""

    # ==================== PDF Extraction Tools ====================

    @mcp.tool()
    def extract_text_from_pdf(
        file_path: str,
        pages: str = "all",
    ) -> Dict[str, Any]:
        """
        Extract text content from a PDF file.

        Uses pypdf library for text extraction. For scanned PDFs
        (image-based), use ocr_image instead.

        Args:
            file_path: Path to the PDF file.
            pages: Pages to extract:
                - "all": Extract all pages
                - "1,2,3": Specific page numbers (1-indexed)
                - "1-5": Page range (inclusive)

        Returns:
            Dictionary with:
            - success: Whether extraction succeeded
            - file_path: Path to the processed file
            - total_pages: Total pages in document
            - pages_extracted: Number of pages extracted
            - pages: List of page content with text and metadata
            - metadata: Document metadata (author, title, etc.)
            - error: Error message if failed

        Example:
            # Extract all pages
            result = extract_text_from_pdf("report.pdf")

            # Extract specific pages
            result = extract_text_from_pdf("report.pdf", pages="1,3,5")

            # Extract page range
            result = extract_text_from_pdf("report.pdf", pages="1-10")
        """
        from ...extraction import PDFExtractor

        extractor = PDFExtractor()
        result = extractor.extract(file_path, pages=pages)
        return result.to_dict()

    @mcp.tool()
    def extract_tables_from_pdf(
        file_path: str,
        pages: str = "all",
    ) -> Dict[str, Any]:
        """
        Extract tables from a PDF file.

        Attempts to detect and parse tabular structures from
        the extracted text. For complex tables, consider using
        specialized table extraction tools.

        Args:
            file_path: Path to the PDF file.
            pages: Pages to extract tables from (same format as extract_text_from_pdf).

        Returns:
            Dictionary with:
            - success: Whether extraction succeeded
            - file_path: Path to the processed file
            - tables: List of detected tables, each with:
                - page: Page number where table was found
                - rows: Table data as list of rows
                - columns: Number of columns detected
            - error: Error message if failed
        """
        from ...extraction import PDFExtractor

        extractor = PDFExtractor()
        tables = extractor.extract_tables(file_path, pages=pages)

        return {
            "success": len(tables) > 0,
            "file_path": file_path,
            "tables": tables,
            "table_count": len(tables),
        }

    # ==================== OCR Extraction Tools ====================

    @mcp.tool()
    def ocr_image(
        file_path: str,
        language: str = "eng",
        preprocess: bool = False,
    ) -> Dict[str, Any]:
        """
        Extract text from an image using OCR (Tesseract).

        Supports common image formats: PNG, JPG, TIFF, BMP, GIF, WebP.
        Requires Tesseract to be installed on the system.

        Args:
            file_path: Path to the image file.
            language: Tesseract language code (e.g., 'eng', 'fra', 'deu').
                     Multiple languages can be combined: 'eng+fra'.
            preprocess: Apply preprocessing (grayscale, contrast) for better OCR.

        Returns:
            Dictionary with:
            - success: Whether extraction succeeded
            - file_path: Path to the processed file
            - text: Extracted text content
            - confidence: Average OCR confidence (0-100)
            - language: Language used for OCR
            - word_count: Number of words extracted
            - char_count: Number of characters extracted
            - error: Error message if failed

        Example:
            # Basic OCR
            result = ocr_image("document.png")

            # OCR with preprocessing for better quality
            result = ocr_image("scanned_doc.jpg", preprocess=True)

            # OCR for French text
            result = ocr_image("french_doc.png", language="fra")
        """
        from ...extraction import OCRExtractor

        extractor = OCRExtractor(language=language)
        result = extractor.extract(file_path, preprocess=preprocess)
        return result.to_dict()

    @mcp.tool()
    def ocr_image_with_boxes(
        file_path: str,
        language: str = "eng",
    ) -> Dict[str, Any]:
        """
        Extract text from an image with bounding box information.

        Returns word-level data including position coordinates,
        useful for understanding document layout.

        Args:
            file_path: Path to the image file.
            language: Tesseract language code.

        Returns:
            Dictionary with:
            - success: Whether extraction succeeded
            - text: Full extracted text
            - confidence: Average confidence score
            - bounding_boxes_count: Number of detected words
            - bounding_boxes: List of word data (limited to first 50):
                - text: The word
                - left, top, width, height: Position
                - confidence: Word-level confidence
                - block, line, word: Position in document structure
        """
        from ...extraction import OCRExtractor

        extractor = OCRExtractor(language=language)
        result = extractor.extract_with_boxes(file_path, output_type="data")

        response = result.to_dict()
        # Limit bounding boxes to avoid context overflow
        if result.bounding_boxes:
            response["bounding_boxes"] = result.bounding_boxes[:50]
            response["bounding_boxes_truncated"] = len(result.bounding_boxes) > 50

        return response

    @mcp.tool()
    def get_available_ocr_languages() -> Dict[str, Any]:
        """
        Get list of available Tesseract OCR languages.

        Returns:
            Dictionary with:
            - success: Whether query succeeded
            - languages: List of available language codes
            - count: Number of available languages
        """
        from ...extraction import OCRExtractor

        languages = OCRExtractor.get_available_languages()

        return {
            "success": len(languages) > 0,
            "languages": languages,
            "count": len(languages),
        }

    # ==================== Table Parsing Tools ====================

    @mcp.tool()
    def parse_table_from_text(
        text: str,
        delimiter: str = "auto",
        has_header: bool = True,
    ) -> Dict[str, Any]:
        """
        Parse tabular data from text.

        Detects and parses table structures from raw text,
        including text extracted from PDFs or OCR.

        Args:
            text: Raw text containing tabular data.
            delimiter: Column delimiter:
                - "auto": Auto-detect delimiter
                - "tab": Tab character
                - "space": Multiple spaces (fixed-width)
                - "pipe": Pipe character |
                - "comma": Comma
                - Or any custom character
            has_header: Treat first row as header.

        Returns:
            Dictionary with:
            - success: Whether parsing found valid tables
            - tables: List of parsed tables, each with:
                - headers: Column names (if has_header)
                - rows: Table data as list of rows
                - row_count: Number of data rows
                - column_count: Number of columns
                - delimiter_detected: Delimiter used
                - confidence: Parse confidence (0-1)
            - total_tables: Number of tables found
            - raw_text_length: Length of input text

        Example:
            # Auto-detect delimiter
            result = parse_table_from_text(extracted_text)

            # Tab-separated data
            result = parse_table_from_text(tsv_text, delimiter="tab")

            # Fixed-width columns
            result = parse_table_from_text(report_text, delimiter="space")
        """
        from ...extraction import TableParser

        parser = TableParser()
        result = parser.parse(text, delimiter=delimiter, has_header=has_header)
        return result.to_dict()

    @mcp.tool()
    def parse_markdown_table(text: str) -> Dict[str, Any]:
        """
        Parse a Markdown-formatted table.

        Specifically handles Markdown table syntax with pipe
        delimiters and header separator rows.

        Args:
            text: Text containing a Markdown table.

        Returns:
            Dictionary with:
            - success: Whether a valid table was found
            - table: Parsed table data (if found):
                - headers: Column names
                - rows: Table data
                - row_count: Number of rows
                - column_count: Number of columns
            - error: Error message if failed

        Example:
            markdown = '''
            | Name  | Age | City     |
            |-------|-----|----------|
            | Alice | 30  | New York |
            | Bob   | 25  | London   |
            '''
            result = parse_markdown_table(markdown)
        """
        from ...extraction import TableParser

        parser = TableParser()
        table = parser.parse_markdown_table(text)

        if table:
            return {
                "success": True,
                "table": table.to_dict(),
            }
        else:
            return {
                "success": False,
                "table": None,
                "error": "No valid Markdown table found",
            }

    @mcp.tool()
    def table_to_records(
        text: str,
        delimiter: str = "auto",
    ) -> Dict[str, Any]:
        """
        Parse table from text and convert to records format.

        Useful for converting extracted tables directly into
        a list of dictionaries suitable for further processing.

        Args:
            text: Raw text containing tabular data.
            delimiter: Column delimiter ("auto", "tab", "pipe", etc.)

        Returns:
            Dictionary with:
            - success: Whether parsing succeeded
            - records: List of dictionaries (one per row)
            - record_count: Number of records
            - columns: List of column names
        """
        from ...extraction import TableParser

        parser = TableParser()
        result = parser.parse(text, delimiter=delimiter, has_header=True)

        if not result.success or not result.tables:
            return {
                "success": False,
                "records": [],
                "record_count": 0,
                "columns": [],
                "error": result.error or "No tables found",
            }

        # Use first table
        table = result.tables[0]
        records = table.to_records()

        return {
            "success": True,
            "records": records[:100],  # Limit to 100 records
            "record_count": len(records),
            "columns": table.headers,
            "truncated": len(records) > 100,
        }

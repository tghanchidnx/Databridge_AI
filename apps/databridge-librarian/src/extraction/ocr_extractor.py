"""
OCR (Optical Character Recognition) module.

Extracts text from images using pytesseract (Tesseract OCR).
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union


@dataclass
class OCRResult:
    """Result of OCR text extraction."""

    success: bool
    file_path: str
    text: str
    confidence: Optional[float] = None
    language: str = "eng"
    word_count: int = 0
    char_count: int = 0
    bounding_boxes: List[Dict[str, Any]] = field(default_factory=list)
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "success": self.success,
            "file_path": self.file_path,
            "text": self.text,
            "confidence": self.confidence,
            "language": self.language,
            "word_count": self.word_count,
            "char_count": self.char_count,
            "bounding_boxes_count": len(self.bounding_boxes),
            "error": self.error,
        }


class OCRExtractor:
    """
    Extract text from images using Tesseract OCR.

    Supports common image formats: PNG, JPG, TIFF, BMP, GIF.
    Requires Tesseract to be installed on the system.

    Example:
        ```python
        extractor = OCRExtractor(language="eng")
        result = extractor.extract("document.png")

        if result.success:
            print(result.text)
            print(f"Confidence: {result.confidence}%")
        ```
    """

    SUPPORTED_FORMATS = {".png", ".jpg", ".jpeg", ".tiff", ".tif", ".bmp", ".gif", ".webp"}

    def __init__(
        self,
        language: str = "eng",
        tesseract_cmd: Optional[str] = None,
    ):
        """
        Initialize OCR extractor.

        Args:
            language: Tesseract language code (e.g., 'eng', 'fra', 'deu').
                     Multiple languages can be combined: 'eng+fra'.
            tesseract_cmd: Path to tesseract executable (auto-detected if None).
        """
        self.language = language
        self.tesseract_cmd = tesseract_cmd

    def extract(
        self,
        file_path: Union[str, Path],
        preprocess: bool = False,
        config: Optional[str] = None,
    ) -> OCRResult:
        """
        Extract text from an image using OCR.

        Args:
            file_path: Path to the image file.
            preprocess: Apply preprocessing (grayscale, threshold) for better OCR.
            config: Additional Tesseract configuration string.

        Returns:
            OCRResult with extracted text and metadata.
        """
        try:
            import pytesseract
            from PIL import Image
        except ImportError:
            return OCRResult(
                success=False,
                file_path=str(file_path),
                text="",
                error="pytesseract and Pillow are required. "
                "Install with: pip install 'databridge-librarian[ocr]'",
            )

        path = Path(file_path)

        # Validate file exists
        if not path.exists():
            return OCRResult(
                success=False,
                file_path=str(file_path),
                text="",
                error=f"File not found: {file_path}",
            )

        # Validate file format
        if path.suffix.lower() not in self.SUPPORTED_FORMATS:
            return OCRResult(
                success=False,
                file_path=str(file_path),
                text="",
                error=f"Unsupported format: {path.suffix}. "
                f"Supported: {', '.join(self.SUPPORTED_FORMATS)}",
            )

        try:
            # Set custom tesseract command if provided
            if self.tesseract_cmd:
                pytesseract.pytesseract.tesseract_cmd = self.tesseract_cmd

            # Load image
            image = Image.open(path)

            # Preprocess if requested
            if preprocess:
                image = self._preprocess_image(image)

            # Build config string
            ocr_config = config or ""

            # Extract text
            text = pytesseract.image_to_string(
                image, lang=self.language, config=ocr_config
            )

            # Get confidence data
            confidence = self._get_confidence(image)

            # Count words and characters
            words = text.split()
            word_count = len(words)
            char_count = len(text)

            return OCRResult(
                success=True,
                file_path=str(file_path),
                text=text,
                confidence=confidence,
                language=self.language,
                word_count=word_count,
                char_count=char_count,
            )

        except pytesseract.TesseractNotFoundError:
            return OCRResult(
                success=False,
                file_path=str(file_path),
                text="",
                error="Tesseract is not installed or not in PATH. "
                "Install from: https://github.com/tesseract-ocr/tesseract",
            )
        except Exception as e:
            return OCRResult(
                success=False,
                file_path=str(file_path),
                text="",
                error=f"OCR extraction failed: {e}",
            )

    def extract_with_boxes(
        self,
        file_path: Union[str, Path],
        output_type: str = "data",
    ) -> OCRResult:
        """
        Extract text with bounding box information.

        Args:
            file_path: Path to the image file.
            output_type: Type of output data:
                - "data": Return detailed word-level data
                - "boxes": Return character-level boxes

        Returns:
            OCRResult with text and bounding box information.
        """
        try:
            import pytesseract
            from PIL import Image
        except ImportError:
            return OCRResult(
                success=False,
                file_path=str(file_path),
                text="",
                error="pytesseract and Pillow are required.",
            )

        path = Path(file_path)
        if not path.exists():
            return OCRResult(
                success=False,
                file_path=str(file_path),
                text="",
                error=f"File not found: {file_path}",
            )

        try:
            if self.tesseract_cmd:
                pytesseract.pytesseract.tesseract_cmd = self.tesseract_cmd

            image = Image.open(path)

            if output_type == "data":
                # Get detailed data with bounding boxes
                data = pytesseract.image_to_data(
                    image, lang=self.language, output_type=pytesseract.Output.DICT
                )

                # Build bounding boxes list
                boxes = []
                for i in range(len(data["text"])):
                    if data["text"][i].strip():
                        boxes.append(
                            {
                                "text": data["text"][i],
                                "left": data["left"][i],
                                "top": data["top"][i],
                                "width": data["width"][i],
                                "height": data["height"][i],
                                "confidence": data["conf"][i],
                                "block": data["block_num"][i],
                                "line": data["line_num"][i],
                                "word": data["word_num"][i],
                            }
                        )

                # Extract full text
                text = " ".join(
                    word for word in data["text"] if word.strip()
                )

                # Calculate average confidence
                confidences = [c for c in data["conf"] if c > 0]
                avg_confidence = (
                    sum(confidences) / len(confidences) if confidences else None
                )

            else:
                # Get character-level boxes
                boxes_str = pytesseract.image_to_boxes(image, lang=self.language)
                boxes = []
                for line in boxes_str.split("\n"):
                    if line:
                        parts = line.split()
                        if len(parts) >= 5:
                            boxes.append(
                                {
                                    "char": parts[0],
                                    "left": int(parts[1]),
                                    "bottom": int(parts[2]),
                                    "right": int(parts[3]),
                                    "top": int(parts[4]),
                                }
                            )

                text = pytesseract.image_to_string(image, lang=self.language)
                avg_confidence = self._get_confidence(image)

            return OCRResult(
                success=True,
                file_path=str(file_path),
                text=text,
                confidence=avg_confidence,
                language=self.language,
                word_count=len(text.split()),
                char_count=len(text),
                bounding_boxes=boxes,
            )

        except Exception as e:
            return OCRResult(
                success=False,
                file_path=str(file_path),
                text="",
                error=f"OCR extraction failed: {e}",
            )

    def _preprocess_image(self, image):
        """
        Apply preprocessing to improve OCR quality.

        Args:
            image: PIL Image object.

        Returns:
            Preprocessed PIL Image.
        """
        from PIL import Image, ImageFilter, ImageOps

        # Convert to grayscale
        if image.mode != "L":
            image = image.convert("L")

        # Apply slight sharpening
        image = image.filter(ImageFilter.SHARPEN)

        # Increase contrast using autocontrast
        image = ImageOps.autocontrast(image)

        return image

    def _get_confidence(self, image) -> Optional[float]:
        """
        Calculate average OCR confidence for an image.

        Args:
            image: PIL Image object.

        Returns:
            Average confidence percentage or None.
        """
        try:
            import pytesseract

            data = pytesseract.image_to_data(
                image, lang=self.language, output_type=pytesseract.Output.DICT
            )
            confidences = [c for c in data["conf"] if c > 0]
            if confidences:
                return round(sum(confidences) / len(confidences), 2)
        except Exception:
            pass
        return None

    @staticmethod
    def get_available_languages() -> List[str]:
        """
        Get list of available Tesseract languages.

        Returns:
            List of language codes, or empty list if Tesseract not available.
        """
        try:
            import pytesseract

            return pytesseract.get_languages()
        except Exception:
            return []

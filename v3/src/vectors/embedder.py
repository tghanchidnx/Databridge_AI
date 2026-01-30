"""
Embedding generation for DataBridge AI V3.

Provides embedding generation using sentence-transformers or other providers.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Union
from enum import Enum
import logging

try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMERS_AVAILABLE = False


logger = logging.getLogger(__name__)


class EmbeddingProvider(str, Enum):
    """Supported embedding providers."""
    SENTENCE_TRANSFORMERS = "sentence-transformers"
    OPENAI = "openai"


@dataclass
class EmbeddingResult:
    """Result from embedding generation."""

    success: bool
    embeddings: List[List[float]] = field(default_factory=list)
    dimension: int = 0
    model: str = ""
    count: int = 0
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "dimension": self.dimension,
            "model": self.model,
            "count": self.count,
            "errors": self.errors,
        }


class Embedder:
    """
    Embedding generator using sentence-transformers.

    Provides:
    - Local embedding generation (offline capable)
    - Batch processing for efficiency
    - Multiple model support
    """

    # Default model for embeddings
    DEFAULT_MODEL = "all-MiniLM-L6-v2"

    # Model dimensions (known models)
    MODEL_DIMENSIONS = {
        "all-MiniLM-L6-v2": 384,
        "all-MiniLM-L12-v2": 384,
        "all-mpnet-base-v2": 768,
        "paraphrase-MiniLM-L6-v2": 384,
        "paraphrase-mpnet-base-v2": 768,
        "multi-qa-MiniLM-L6-cos-v1": 384,
        "multi-qa-mpnet-base-dot-v1": 768,
    }

    def __init__(
        self,
        model_name: str = DEFAULT_MODEL,
        device: Optional[str] = None,
        cache_folder: Optional[str] = None,
    ):
        """
        Initialize the embedder.

        Args:
            model_name: Name of the sentence-transformers model.
            device: Device to use ('cpu', 'cuda', etc.).
            cache_folder: Folder to cache models.
        """
        if not SENTENCE_TRANSFORMERS_AVAILABLE:
            raise ImportError(
                "sentence-transformers is required for Embedder. "
                "Install with: pip install sentence-transformers"
            )

        self.model_name = model_name
        self.device = device
        self._model: Optional[SentenceTransformer] = None
        self._cache_folder = cache_folder

    def _load_model(self) -> SentenceTransformer:
        """Load the sentence-transformers model."""
        if self._model is None:
            logger.info(f"Loading embedding model: {self.model_name}")
            self._model = SentenceTransformer(
                self.model_name,
                device=self.device,
                cache_folder=self._cache_folder,
            )
        return self._model

    @property
    def dimension(self) -> int:
        """Get the embedding dimension for the current model."""
        if self.model_name in self.MODEL_DIMENSIONS:
            return self.MODEL_DIMENSIONS[self.model_name]
        # Load model to get dimension
        model = self._load_model()
        return model.get_sentence_embedding_dimension()

    def embed(
        self,
        texts: Union[str, List[str]],
        batch_size: int = 32,
        show_progress: bool = False,
        normalize: bool = True,
    ) -> EmbeddingResult:
        """
        Generate embeddings for texts.

        Args:
            texts: Single text or list of texts to embed.
            batch_size: Batch size for processing.
            show_progress: Show progress bar.
            normalize: Normalize embeddings to unit length.

        Returns:
            EmbeddingResult with embeddings.
        """
        try:
            if isinstance(texts, str):
                texts = [texts]

            if not texts:
                return EmbeddingResult(
                    success=True,
                    embeddings=[],
                    dimension=self.dimension,
                    model=self.model_name,
                    count=0,
                )

            model = self._load_model()

            embeddings = model.encode(
                texts,
                batch_size=batch_size,
                show_progress_bar=show_progress,
                normalize_embeddings=normalize,
            )

            # Convert to list of lists
            embeddings_list = embeddings.tolist()

            return EmbeddingResult(
                success=True,
                embeddings=embeddings_list,
                dimension=len(embeddings_list[0]) if embeddings_list else 0,
                model=self.model_name,
                count=len(embeddings_list),
            )

        except Exception as e:
            logger.error(f"Embedding generation failed: {e}")
            return EmbeddingResult(
                success=False,
                errors=[str(e)],
            )

    def similarity(
        self,
        text1: str,
        text2: str,
    ) -> float:
        """
        Calculate cosine similarity between two texts.

        Args:
            text1: First text.
            text2: Second text.

        Returns:
            Cosine similarity score (0-1).
        """
        result = self.embed([text1, text2])
        if not result.success or len(result.embeddings) < 2:
            return 0.0

        # Calculate cosine similarity
        import numpy as np
        e1 = np.array(result.embeddings[0])
        e2 = np.array(result.embeddings[1])

        # Already normalized, so dot product equals cosine similarity
        return float(np.dot(e1, e2))


class HierarchyEmbedder:
    """
    Specialized embedder for hierarchies.

    Converts hierarchical structures into semantic text representations
    and generates embeddings for similarity search and RAG.
    """

    def __init__(
        self,
        embedder: Optional[Embedder] = None,
        model_name: str = Embedder.DEFAULT_MODEL,
    ):
        """
        Initialize the hierarchy embedder.

        Args:
            embedder: Existing Embedder instance to use.
            model_name: Model name if creating new embedder.
        """
        self.embedder = embedder or Embedder(model_name=model_name)

    def hierarchy_to_text(
        self,
        hierarchy: Dict[str, Any],
        include_path: bool = True,
        include_description: bool = True,
        include_mappings: bool = True,
        separator: str = " > ",
    ) -> str:
        """
        Convert a hierarchy node to semantic text.

        Args:
            hierarchy: Hierarchy data dictionary.
            include_path: Include the hierarchy path.
            include_description: Include description.
            include_mappings: Include source mappings.
            separator: Path separator.

        Returns:
            Semantic text representation.
        """
        parts = []

        # Add name
        name = hierarchy.get("name") or hierarchy.get("hierarchy_name", "")
        if name:
            parts.append(f"Name: {name}")

        # Add path (levels)
        if include_path:
            levels = []
            for i in range(1, 11):
                level_key = f"level_{i}"
                level_value = hierarchy.get(level_key, "")
                if level_value:
                    levels.append(level_value)
            if levels:
                parts.append(f"Path: {separator.join(levels)}")

        # Add description
        if include_description:
            desc = hierarchy.get("description", "")
            if desc:
                parts.append(f"Description: {desc}")

        # Add formula group
        formula_group = hierarchy.get("formula_group", "")
        if formula_group:
            parts.append(f"Formula: {formula_group}")

        # Add mappings summary
        if include_mappings:
            mappings = hierarchy.get("mappings", [])
            if mappings:
                mapping_texts = []
                for m in mappings[:5]:  # Limit to first 5 mappings
                    source = f"{m.get('source_database', '')}.{m.get('source_schema', '')}.{m.get('source_table', '')}"
                    if source.strip("."):
                        mapping_texts.append(source)
                if mapping_texts:
                    parts.append(f"Sources: {', '.join(mapping_texts)}")

        return ". ".join(parts) if parts else name

    def embed_hierarchy(
        self,
        hierarchy: Dict[str, Any],
        include_path: bool = True,
        include_description: bool = True,
        include_mappings: bool = True,
    ) -> EmbeddingResult:
        """
        Generate embedding for a single hierarchy.

        Args:
            hierarchy: Hierarchy data dictionary.
            include_path: Include path in text representation.
            include_description: Include description.
            include_mappings: Include source mappings.

        Returns:
            EmbeddingResult with single embedding.
        """
        text = self.hierarchy_to_text(
            hierarchy,
            include_path=include_path,
            include_description=include_description,
            include_mappings=include_mappings,
        )
        return self.embedder.embed(text)

    def embed_hierarchies(
        self,
        hierarchies: List[Dict[str, Any]],
        include_path: bool = True,
        include_description: bool = True,
        include_mappings: bool = True,
        batch_size: int = 32,
        show_progress: bool = False,
    ) -> EmbeddingResult:
        """
        Generate embeddings for multiple hierarchies.

        Args:
            hierarchies: List of hierarchy data dictionaries.
            include_path: Include path in text representation.
            include_description: Include description.
            include_mappings: Include source mappings.
            batch_size: Batch size for embedding.
            show_progress: Show progress bar.

        Returns:
            EmbeddingResult with embeddings for all hierarchies.
        """
        texts = [
            self.hierarchy_to_text(h, include_path, include_description, include_mappings)
            for h in hierarchies
        ]
        return self.embedder.embed(
            texts,
            batch_size=batch_size,
            show_progress=show_progress,
        )

    def tree_to_text(
        self,
        tree: Dict[str, Any],
        depth: int = 0,
        max_depth: int = 5,
    ) -> str:
        """
        Convert a hierarchy tree to text representation.

        Args:
            tree: Tree structure with children.
            depth: Current depth level.
            max_depth: Maximum depth to traverse.

        Returns:
            Text representation of the tree.
        """
        if depth >= max_depth:
            return ""

        parts = []
        indent = "  " * depth

        # Add current node
        name = tree.get("name") or tree.get("hierarchy_name", "Unknown")
        parts.append(f"{indent}- {name}")

        # Add description if available
        desc = tree.get("description", "")
        if desc:
            parts.append(f"{indent}  ({desc})")

        # Process children
        children = tree.get("children", [])
        for child in children:
            child_text = self.tree_to_text(child, depth + 1, max_depth)
            if child_text:
                parts.append(child_text)

        return "\n".join(parts)

    def embed_tree(
        self,
        tree: Dict[str, Any],
        max_depth: int = 5,
    ) -> EmbeddingResult:
        """
        Generate embedding for a hierarchy tree.

        Args:
            tree: Tree structure with children.
            max_depth: Maximum depth to include.

        Returns:
            EmbeddingResult for the tree.
        """
        text = self.tree_to_text(tree, max_depth=max_depth)
        return self.embedder.embed(text)


class ConceptEmbedder:
    """
    Embedder for whitepaper concepts and industry patterns.
    """

    def __init__(
        self,
        embedder: Optional[Embedder] = None,
        model_name: str = Embedder.DEFAULT_MODEL,
    ):
        """
        Initialize the concept embedder.

        Args:
            embedder: Existing Embedder instance to use.
            model_name: Model name if creating new embedder.
        """
        self.embedder = embedder or Embedder(model_name=model_name)

    def concept_to_text(
        self,
        concept: Dict[str, Any],
    ) -> str:
        """
        Convert a concept to semantic text.

        Args:
            concept: Concept data dictionary.

        Returns:
            Semantic text representation.
        """
        parts = []

        # Add title
        title = concept.get("title") or concept.get("name", "")
        if title:
            parts.append(f"Concept: {title}")

        # Add category
        category = concept.get("category", "")
        if category:
            parts.append(f"Category: {category}")

        # Add definition
        definition = concept.get("definition") or concept.get("description", "")
        if definition:
            parts.append(f"Definition: {definition}")

        # Add examples
        examples = concept.get("examples", [])
        if examples:
            examples_text = ", ".join(examples[:5])
            parts.append(f"Examples: {examples_text}")

        # Add related concepts
        related = concept.get("related", [])
        if related:
            related_text = ", ".join(related[:5])
            parts.append(f"Related: {related_text}")

        return ". ".join(parts)

    def embed_concept(
        self,
        concept: Dict[str, Any],
    ) -> EmbeddingResult:
        """
        Generate embedding for a concept.

        Args:
            concept: Concept data dictionary.

        Returns:
            EmbeddingResult with embedding.
        """
        text = self.concept_to_text(concept)
        return self.embedder.embed(text)

    def embed_concepts(
        self,
        concepts: List[Dict[str, Any]],
        batch_size: int = 32,
        show_progress: bool = False,
    ) -> EmbeddingResult:
        """
        Generate embeddings for multiple concepts.

        Args:
            concepts: List of concept dictionaries.
            batch_size: Batch size for embedding.
            show_progress: Show progress bar.

        Returns:
            EmbeddingResult with embeddings for all concepts.
        """
        texts = [self.concept_to_text(c) for c in concepts]
        return self.embedder.embed(
            texts,
            batch_size=batch_size,
            show_progress=show_progress,
        )

    def industry_pattern_to_text(
        self,
        pattern: Dict[str, Any],
    ) -> str:
        """
        Convert an industry pattern to semantic text.

        Args:
            pattern: Industry pattern data dictionary.

        Returns:
            Semantic text representation.
        """
        parts = []

        # Add industry
        industry = pattern.get("industry", "")
        if industry:
            parts.append(f"Industry: {industry}")

        # Add pattern name
        name = pattern.get("name") or pattern.get("pattern_name", "")
        if name:
            parts.append(f"Pattern: {name}")

        # Add description
        desc = pattern.get("description", "")
        if desc:
            parts.append(f"Description: {desc}")

        # Add hierarchy type
        hierarchy_type = pattern.get("hierarchy_type", "")
        if hierarchy_type:
            parts.append(f"Type: {hierarchy_type}")

        # Add typical levels
        levels = pattern.get("typical_levels", [])
        if levels:
            levels_text = " > ".join(levels)
            parts.append(f"Levels: {levels_text}")

        # Add key metrics
        metrics = pattern.get("key_metrics", [])
        if metrics:
            metrics_text = ", ".join(metrics[:5])
            parts.append(f"Metrics: {metrics_text}")

        return ". ".join(parts)

    def embed_industry_pattern(
        self,
        pattern: Dict[str, Any],
    ) -> EmbeddingResult:
        """
        Generate embedding for an industry pattern.

        Args:
            pattern: Industry pattern data dictionary.

        Returns:
            EmbeddingResult with embedding.
        """
        text = self.industry_pattern_to_text(pattern)
        return self.embedder.embed(text)

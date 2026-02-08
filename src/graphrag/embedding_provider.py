"""
Embedding Provider - Unified interface for text embeddings.

Supports:
- OpenAI (text-embedding-3-small/large)
- HuggingFace (sentence-transformers)
- Local placeholder for future Ollama support

Includes caching to avoid redundant API calls.
"""
import hashlib
import json
import logging
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional, Any

from .types import EmbeddingProvider

logger = logging.getLogger(__name__)


class EmbeddingCache:
    """
    Cache embeddings to disk to avoid recomputation.

    Uses content hash as key to handle identical content efficiently.
    """

    def __init__(self, cache_dir: str = "data/graphrag/embedding_cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._memory_cache: Dict[str, List[float]] = {}
        self._stats = {"hits": 0, "misses": 0}

    def _hash_key(self, text: str, model: str) -> str:
        """Generate cache key from text and model."""
        content = f"{model}:{text}"
        return hashlib.sha256(content.encode()).hexdigest()[:32]

    def get(self, text: str, model: str) -> Optional[List[float]]:
        """Get embedding from cache if exists."""
        key = self._hash_key(text, model)

        # Check memory cache first
        if key in self._memory_cache:
            self._stats["hits"] += 1
            return self._memory_cache[key]

        # Check disk cache
        cache_file = self.cache_dir / f"{key}.json"
        if cache_file.exists():
            try:
                with open(cache_file, "r") as f:
                    embedding = json.load(f)
                self._memory_cache[key] = embedding
                self._stats["hits"] += 1
                return embedding
            except Exception as e:
                logger.debug(f"Cache read error: {e}")

        self._stats["misses"] += 1
        return None

    def set(self, text: str, model: str, embedding: List[float]) -> None:
        """Store embedding in cache."""
        key = self._hash_key(text, model)
        self._memory_cache[key] = embedding

        # Write to disk
        cache_file = self.cache_dir / f"{key}.json"
        try:
            with open(cache_file, "w") as f:
                json.dump(embedding, f)
        except Exception as e:
            logger.debug(f"Cache write error: {e}")

    def clear(self) -> int:
        """Clear all cached embeddings. Returns count cleared."""
        count = 0
        self._memory_cache.clear()
        for f in self.cache_dir.glob("*.json"):
            try:
                f.unlink()
                count += 1
            except Exception:
                pass
        return count

    def stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        disk_count = len(list(self.cache_dir.glob("*.json")))
        return {
            "memory_count": len(self._memory_cache),
            "disk_count": disk_count,
            "hits": self._stats["hits"],
            "misses": self._stats["misses"],
            "hit_rate": self._stats["hits"] / max(1, self._stats["hits"] + self._stats["misses"]),
        }


class BaseEmbeddingProvider(ABC):
    """Abstract base class for embedding providers."""

    @abstractmethod
    def embed(self, text: str) -> List[float]:
        """Generate embedding for a single text."""
        pass

    @abstractmethod
    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for multiple texts."""
        pass

    @property
    @abstractmethod
    def dimension(self) -> int:
        """Return embedding dimension."""
        pass

    @property
    @abstractmethod
    def model_name(self) -> str:
        """Return model name."""
        pass


class OpenAIEmbeddings(BaseEmbeddingProvider):
    """OpenAI embedding provider using text-embedding-3 models."""

    def __init__(
        self,
        model: str = "text-embedding-3-small",
        api_key: Optional[str] = None,
        cache: Optional[EmbeddingCache] = None,
    ):
        self.model = model
        self.api_key = api_key or os.getenv("OPENAI_API_KEY", "")
        self.cache = cache or EmbeddingCache()

        # Set dimension based on model
        if "large" in model:
            self._dimension = 3072
        else:
            self._dimension = 1536

        self._client = None

    def _get_client(self):
        """Lazy load OpenAI client."""
        if self._client is None:
            try:
                import openai
                self._client = openai.OpenAI(api_key=self.api_key)
            except ImportError:
                raise ImportError("openai package not installed. Run: pip install openai")
        return self._client

    @property
    def dimension(self) -> int:
        return self._dimension

    @property
    def model_name(self) -> str:
        return self.model

    def embed(self, text: str) -> List[float]:
        """Generate embedding for single text."""
        # Check cache
        cached = self.cache.get(text, self.model)
        if cached is not None:
            return cached

        try:
            client = self._get_client()
            response = client.embeddings.create(
                model=self.model,
                input=text,
            )
            embedding = response.data[0].embedding
            self.cache.set(text, self.model, embedding)
            return embedding

        except Exception as e:
            logger.error(f"OpenAI embedding failed: {e}")
            # Return zero vector on failure
            return [0.0] * self.dimension

    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for multiple texts."""
        results: List[Optional[List[float]]] = [None] * len(texts)
        uncached_texts = []
        uncached_indices = []

        # Check cache for each text
        for i, text in enumerate(texts):
            cached = self.cache.get(text, self.model)
            if cached is not None:
                results[i] = cached
            else:
                uncached_texts.append(text)
                uncached_indices.append(i)

        # Batch embed uncached texts
        if uncached_texts:
            try:
                client = self._get_client()
                response = client.embeddings.create(
                    model=self.model,
                    input=uncached_texts,
                )

                for j, emb_data in enumerate(response.data):
                    idx = uncached_indices[j]
                    embedding = emb_data.embedding
                    results[idx] = embedding
                    self.cache.set(uncached_texts[j], self.model, embedding)

            except Exception as e:
                logger.error(f"OpenAI batch embedding failed: {e}")
                # Fill failures with zero vectors
                for idx in uncached_indices:
                    if results[idx] is None:
                        results[idx] = [0.0] * self.dimension

        return [r if r is not None else [0.0] * self.dimension for r in results]


class HuggingFaceEmbeddings(BaseEmbeddingProvider):
    """HuggingFace sentence-transformers embedding provider."""

    def __init__(
        self,
        model: str = "all-MiniLM-L6-v2",
        cache: Optional[EmbeddingCache] = None,
    ):
        self._model_name = model
        self.cache = cache or EmbeddingCache()
        self._model = None
        self._dimension = 384  # Default for MiniLM

    def _load_model(self):
        """Lazy load the model."""
        if self._model is None:
            try:
                from sentence_transformers import SentenceTransformer
                self._model = SentenceTransformer(self._model_name)
                self._dimension = self._model.get_sentence_embedding_dimension()
                logger.info(f"Loaded HuggingFace model: {self._model_name} (dim={self._dimension})")
            except ImportError:
                raise ImportError(
                    "sentence-transformers not installed. Run: pip install sentence-transformers"
                )

    @property
    def dimension(self) -> int:
        return self._dimension

    @property
    def model_name(self) -> str:
        return self._model_name

    def embed(self, text: str) -> List[float]:
        """Generate embedding for single text."""
        cached = self.cache.get(text, self._model_name)
        if cached is not None:
            return cached

        self._load_model()
        try:
            embedding = self._model.encode(text, convert_to_numpy=True).tolist()
            self.cache.set(text, self._model_name, embedding)
            return embedding
        except Exception as e:
            logger.error(f"HuggingFace embedding failed: {e}")
            return [0.0] * self.dimension

    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for multiple texts."""
        self._load_model()

        results: List[Optional[List[float]]] = [None] * len(texts)
        uncached_texts = []
        uncached_indices = []

        for i, text in enumerate(texts):
            cached = self.cache.get(text, self._model_name)
            if cached is not None:
                results[i] = cached
            else:
                uncached_texts.append(text)
                uncached_indices.append(i)

        if uncached_texts:
            try:
                embeddings = self._model.encode(uncached_texts, convert_to_numpy=True)
                for j, emb in enumerate(embeddings):
                    idx = uncached_indices[j]
                    emb_list = emb.tolist()
                    results[idx] = emb_list
                    self.cache.set(uncached_texts[j], self._model_name, emb_list)
            except Exception as e:
                logger.error(f"HuggingFace batch embedding failed: {e}")
                for idx in uncached_indices:
                    if results[idx] is None:
                        results[idx] = [0.0] * self.dimension

        return [r if r is not None else [0.0] * self.dimension for r in results]


class MockEmbeddings(BaseEmbeddingProvider):
    """
    Mock embedding provider for testing without API calls.

    Generates deterministic embeddings based on text hash.
    """

    def __init__(self, dimension: int = 384):
        self._dimension = dimension

    @property
    def dimension(self) -> int:
        return self._dimension

    @property
    def model_name(self) -> str:
        return "mock-embeddings"

    def _hash_to_embedding(self, text: str) -> List[float]:
        """Generate deterministic embedding from text hash."""
        import hashlib
        hash_bytes = hashlib.sha256(text.encode()).digest()

        # Convert hash bytes to floats
        embedding = []
        for i in range(0, min(len(hash_bytes), self._dimension), 1):
            # Normalize to [-1, 1]
            val = (hash_bytes[i % len(hash_bytes)] / 127.5) - 1
            embedding.append(val)

        # Pad if needed
        while len(embedding) < self._dimension:
            embedding.append(0.0)

        # Normalize to unit vector
        import math
        norm = math.sqrt(sum(x * x for x in embedding))
        if norm > 0:
            embedding = [x / norm for x in embedding]

        return embedding[:self._dimension]

    def embed(self, text: str) -> List[float]:
        return self._hash_to_embedding(text)

    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        return [self._hash_to_embedding(t) for t in texts]


def get_embedding_provider(
    provider: EmbeddingProvider,
    model: Optional[str] = None,
    cache_dir: Optional[str] = None,
    **kwargs,
) -> BaseEmbeddingProvider:
    """
    Factory function to get an embedding provider.

    Args:
        provider: The embedding provider type
        model: Optional model name override
        cache_dir: Optional cache directory
        **kwargs: Additional provider-specific arguments

    Returns:
        Configured embedding provider instance
    """
    cache = EmbeddingCache(cache_dir) if cache_dir else None

    if provider == EmbeddingProvider.OPENAI:
        return OpenAIEmbeddings(
            model=model or "text-embedding-3-small",
            cache=cache,
            **kwargs,
        )

    elif provider == EmbeddingProvider.HUGGINGFACE:
        return HuggingFaceEmbeddings(
            model=model or "all-MiniLM-L6-v2",
            cache=cache,
        )

    elif provider == EmbeddingProvider.LOCAL:
        # For now, use mock. Future: Ollama integration
        logger.warning("LOCAL provider using mock embeddings. Future: Ollama support.")
        return MockEmbeddings(dimension=384)

    else:
        raise ValueError(f"Unsupported embedding provider: {provider}")

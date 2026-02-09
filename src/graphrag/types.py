"""
GraphRAG Types - Pydantic models for the RAG engine.

Phase 31: GraphRAG Engine for anti-hallucination.
"""
from enum import Enum
from datetime import datetime
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field
import uuid


class EmbeddingProvider(str, Enum):
    """Supported embedding providers."""
    OPENAI = "openai"           # text-embedding-3-small/large
    ANTHROPIC = "anthropic"     # Claude embeddings (future)
    HUGGINGFACE = "huggingface" # sentence-transformers
    LOCAL = "local"             # Local models via Ollama


class VectorStoreType(str, Enum):
    """Supported vector databases."""
    CHROMA = "chroma"           # Local, easy setup
    PINECONE = "pinecone"       # Cloud, production-ready
    WEAVIATE = "weaviate"       # Hybrid search native
    SQLITE = "sqlite"           # SQLite with numpy (no deps)


class RetrievalSource(str, Enum):
    """Sources for hybrid retrieval."""
    VECTOR = "vector"           # Embedding similarity
    GRAPH = "graph"             # Lineage traversal
    LEXICAL = "lexical"         # Catalog keyword search
    TEMPLATE = "template"       # Template matching
    KNOWLEDGE = "knowledge"     # Knowledge base
    SKILL = "skill"             # Skill prompts


class EntityType(str, Enum):
    """Types of entities extracted from queries."""
    TABLE = "table"
    COLUMN = "column"
    HIERARCHY = "hierarchy"
    HIERARCHY_PROJECT = "hierarchy_project"
    GLOSSARY_TERM = "glossary_term"
    SKILL = "skill"
    TEMPLATE = "template"
    DATABASE = "database"
    SCHEMA = "schema"
    DOMAIN = "domain"
    INDUSTRY = "industry"
    FORMULA = "formula"
    MAPPING = "mapping"


class ExtractedEntity(BaseModel):
    """An entity extracted from a query."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    text: str
    entity_type: EntityType
    confidence: float = 1.0
    linked_id: Optional[str] = None  # ID in catalog/hierarchy
    start_pos: Optional[int] = None  # Position in query
    end_pos: Optional[int] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

    model_config = {"extra": "allow"}


class RAGQuery(BaseModel):
    """A query for the RAG system."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    query: str
    entities: List[ExtractedEntity] = Field(default_factory=list)
    intent: Optional[str] = None
    domain: Optional[str] = None
    industry: Optional[str] = None
    connection_id: Optional[str] = None  # For live validation

    # Retrieval options
    include_lineage: bool = True
    include_templates: bool = True
    include_knowledge: bool = True
    include_skills: bool = True
    max_results: int = 10

    # Weights for hybrid retrieval
    vector_weight: float = 0.4
    graph_weight: float = 0.3
    lexical_weight: float = 0.2
    template_weight: float = 0.1

    model_config = {"extra": "allow"}


class RetrievedItem(BaseModel):
    """A single item retrieved by the RAG system."""
    id: str
    source: RetrievalSource
    content: str
    score: float
    rank: int = 0
    metadata: Dict[str, Any] = Field(default_factory=dict)
    embedding: Optional[List[float]] = None

    model_config = {"extra": "allow"}


class RAGContext(BaseModel):
    """Assembled context for LLM generation."""
    query: RAGQuery
    retrieved_items: List[RetrievedItem] = Field(default_factory=list)

    # Structured context by type
    templates: List[Dict[str, Any]] = Field(default_factory=list)
    skills: List[Dict[str, Any]] = Field(default_factory=list)
    knowledge: List[Dict[str, Any]] = Field(default_factory=list)
    lineage_paths: List[Dict[str, Any]] = Field(default_factory=list)
    glossary_terms: List[Dict[str, Any]] = Field(default_factory=list)
    catalog_assets: List[Dict[str, Any]] = Field(default_factory=list)

    # Hierarchy context
    hierarchy_structures: List[Dict[str, Any]] = Field(default_factory=list)
    hierarchy_projects: List[Dict[str, Any]] = Field(default_factory=list)

    # For Proof of Graph validation
    available_tables: List[str] = Field(default_factory=list)
    available_columns: Dict[str, List[str]] = Field(default_factory=dict)
    available_hierarchies: List[str] = Field(default_factory=list)

    # Context window management
    total_tokens: int = 0
    max_tokens: int = 8000

    # Timing
    retrieval_time_ms: int = 0

    model_config = {"extra": "allow"}

    def to_prompt_context(self) -> str:
        """Format context for LLM prompt injection."""
        sections = []

        if self.templates:
            sections.append("## Available Templates\n" +
                "\n".join(f"- {t.get('name', 'Unknown')}: {t.get('description', '')}"
                          for t in self.templates[:5]))

        if self.skills:
            sections.append("## Relevant Skills\n" +
                "\n".join(f"- {s.get('name', 'Unknown')}: {s.get('description', '')}"
                          for s in self.skills[:3]))

        if self.lineage_paths:
            sections.append("## Data Lineage\n" +
                "\n".join(f"- {lp.get('path', '')}" for lp in self.lineage_paths[:5]))

        if self.glossary_terms:
            sections.append("## Business Terms\n" +
                "\n".join(f"- **{t.get('name', '')}**: {t.get('definition', '')}"
                          for t in self.glossary_terms[:5]))

        if self.hierarchy_structures:
            hier_lines = []
            for hs in self.hierarchy_structures[:10]:
                name = hs.get("name", hs.get("hierarchy_name", ""))
                levels = hs.get("level_depth", "")
                mappings = hs.get("mapping_count", 0)
                hier_lines.append(f"- **{name}** (levels: {levels}, mappings: {mappings})")
            sections.append("## Hierarchy Structures\n" + "\n".join(hier_lines))

        if self.hierarchy_projects:
            proj_lines = []
            for hp in self.hierarchy_projects[:5]:
                pname = hp.get("name", "")
                hcount = hp.get("hierarchy_count", 0)
                proj_lines.append(f"- **{pname}** ({hcount} hierarchies)")
            sections.append("## Hierarchy Projects\n" + "\n".join(proj_lines))

        if self.available_tables:
            sections.append("## Available Tables\n" +
                ", ".join(self.available_tables[:20]))

        return "\n\n".join(sections)


class ValidationSeverity(str, Enum):
    """Severity levels for validation issues."""
    ERROR = "error"       # Must fix before proceeding
    WARNING = "warning"   # Should review but can proceed
    INFO = "info"         # Informational only


class ValidationIssue(BaseModel):
    """A single validation issue."""
    severity: ValidationSeverity
    message: str
    entity: Optional[str] = None
    suggestion: Optional[str] = None
    line_number: Optional[int] = None

    model_config = {"extra": "allow"}


class ValidationResult(BaseModel):
    """Result of Proof of Graph validation."""
    valid: bool
    issues: List[ValidationIssue] = Field(default_factory=list)

    # Convenience accessors
    @property
    def errors(self) -> List[str]:
        return [i.message for i in self.issues if i.severity == ValidationSeverity.ERROR]

    @property
    def warnings(self) -> List[str]:
        return [i.message for i in self.issues if i.severity == ValidationSeverity.WARNING]

    # Entity tracking
    referenced_entities: List[str] = Field(default_factory=list)
    verified_entities: List[str] = Field(default_factory=list)
    missing_entities: List[str] = Field(default_factory=list)
    suggested_fixes: List[str] = Field(default_factory=list)

    model_config = {"extra": "allow"}


class RAGResult(BaseModel):
    """Final result from the RAG pipeline."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    query: str
    context: RAGContext
    response: Optional[str] = None
    validation: Optional[ValidationResult] = None

    # Attribution
    sources: List[str] = Field(default_factory=list)
    confidence: float = 0.0

    # Performance
    processing_time_ms: int = 0
    created_at: datetime = Field(default_factory=datetime.utcnow)

    model_config = {"extra": "allow"}


class RAGConfig(BaseModel):
    """Configuration for the RAG engine."""
    # Embedding
    embedding_provider: EmbeddingProvider = EmbeddingProvider.OPENAI
    embedding_model: str = "text-embedding-3-small"
    embedding_dimension: int = 1536

    # Vector store
    vector_store_type: VectorStoreType = VectorStoreType.SQLITE
    vector_store_path: str = "data/graphrag/vectors"

    # Retrieval
    default_top_k: int = 10
    similarity_threshold: float = 0.5

    # Fusion weights
    vector_weight: float = 0.4
    graph_weight: float = 0.3
    lexical_weight: float = 0.2
    template_weight: float = 0.1

    # Validation
    enable_proof_of_graph: bool = True
    strict_validation: bool = False  # Fail on warnings if True

    # Caching
    enable_cache: bool = True
    cache_ttl_seconds: int = 3600

    model_config = {"extra": "allow"}


class IndexedDocument(BaseModel):
    """A document indexed in the vector store."""
    id: str
    content: str
    embedding: Optional[List[float]] = None
    source_type: str  # catalog, hierarchy, template, skill, knowledge
    source_id: str    # Original ID in source system
    metadata: Dict[str, Any] = Field(default_factory=dict)
    indexed_at: datetime = Field(default_factory=datetime.utcnow)

    model_config = {"extra": "allow"}


class IndexStats(BaseModel):
    """Statistics about the RAG index."""
    total_documents: int = 0
    by_source: Dict[str, int] = Field(default_factory=dict)
    last_indexed: Optional[datetime] = None
    embedding_model: Optional[str] = None
    vector_dimension: int = 0

    model_config = {"extra": "allow"}

# Vector Embeddings Enhancement for Headless DataBridge AI

## Purpose: AI-Native Financial Hierarchy Understanding

This enhancement enables AI systems to deeply understand:
1. **Hierarchical relationships** - Parent-child structures, inheritance patterns
2. **Cross-perspective reconciliation** - How the same transaction maps to multiple valid views
3. **Industry patterns** - O&G, Manufacturing, Healthcare, PE, Retail, Construction
4. **Financial concepts** - COA structures, cost allocation, reconciliation logic

---

## Architecture: Vector-Enhanced Hierarchy System

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         AI Intelligence Layer                           │
├─────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────────┐  │
│  │  RAG Pipeline    │  │ Semantic Search  │  │ Concept Retrieval    │  │
│  │  (Hierarchy +    │  │ (Find similar    │  │ (Industry patterns,  │  │
│  │   Whitepaper)    │  │  hierarchies)    │  │  reconciliation)     │  │
│  └──────────────────┘  └──────────────────┘  └──────────────────────┘  │
├─────────────────────────────────────────────────────────────────────────┤
│                         Vector Store Layer                              │
├─────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │                     ChromaDB / Qdrant                             │  │
│  │  Collections:                                                     │  │
│  │  ├── hierarchies          (hierarchy node embeddings)            │  │
│  │  ├── mappings             (source mapping embeddings)            │  │
│  │  ├── perspectives         (cross-perspective relationships)      │  │
│  │  ├── industry_patterns    (O&G, Manufacturing, Healthcare, etc.) │  │
│  │  ├── reconciliation_rules (how views reconcile)                  │  │
│  │  └── whitepaper_concepts  (core methodology concepts)            │  │
│  └──────────────────────────────────────────────────────────────────┘  │
├─────────────────────────────────────────────────────────────────────────┤
│                         Embedding Generation                            │
├─────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────────┐  │
│  │ OpenAI           │  │ Sentence         │  │ Local Models         │  │
│  │ text-embedding-  │  │ Transformers     │  │ (all-MiniLM-L6-v2)   │  │
│  │ 3-small          │  │ (offline)        │  │ (air-gapped)         │  │
│  └──────────────────┘  └──────────────────┘  └──────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## New Module: `src/vectors/`

### Directory Structure

```
src/vectors/
├── __init__.py
├── config.py              # Vector store configuration
├── embedder.py            # Embedding generation (OpenAI/local)
├── store.py               # ChromaDB/Qdrant wrapper
├── indexer.py             # Index hierarchies, mappings, concepts
├── retriever.py           # Semantic search and RAG
├── perspectives.py        # Cross-perspective relationship modeling
├── industry_patterns.py   # Industry-specific pattern library
└── whitepaper_loader.py   # Load whitepaper concepts into vectors
```

### Core Components

#### 1. Embedding Configuration (`config.py`)

```python
from pydantic_settings import BaseSettings
from typing import Literal

class VectorSettings(BaseSettings):
    # Embedding provider
    embedding_provider: Literal["openai", "sentence-transformers", "local"] = "sentence-transformers"
    embedding_model: str = "all-MiniLM-L6-v2"  # 384 dimensions, fast
    openai_embedding_model: str = "text-embedding-3-small"  # 1536 dimensions

    # Vector store
    vector_store: Literal["chromadb", "qdrant", "faiss"] = "chromadb"
    vector_db_path: str = "data/vectors"

    # Collections
    collection_hierarchies: str = "hierarchies"
    collection_mappings: str = "mappings"
    collection_perspectives: str = "perspectives"
    collection_industry: str = "industry_patterns"
    collection_concepts: str = "whitepaper_concepts"

    # Search settings
    default_top_k: int = 10
    similarity_threshold: float = 0.7

    class Config:
        env_prefix = "DATABRIDGE_VECTOR_"
```

#### 2. Hierarchy Embedder (`embedder.py`)

```python
from typing import List, Dict, Any
import json

class HierarchyEmbedder:
    """Generate rich embeddings for hierarchy nodes."""

    def embed_hierarchy(self, hierarchy: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create embedding-ready text from hierarchy node.
        Captures: structure, purpose, mappings, relationships.
        """
        # Build rich text representation
        text_parts = [
            f"Hierarchy: {hierarchy['hierarchy_name']}",
            f"Description: {hierarchy.get('description', '')}",
        ]

        # Add level context (path in tree)
        for i in range(1, 16):
            level = hierarchy.get(f'level_{i}')
            if level:
                text_parts.append(f"Level {i}: {level}")

        # Add parent context
        if hierarchy.get('parent_name'):
            text_parts.append(f"Parent: {hierarchy['parent_name']}")

        # Add mapping context (what data sources feed this)
        for mapping in hierarchy.get('source_mappings', []):
            text_parts.append(
                f"Source: {mapping['database']}.{mapping['schema']}."
                f"{mapping['table']}.{mapping['column']}"
            )

        # Add flags context
        flags = []
        if hierarchy.get('include_flag'): flags.append("included")
        if hierarchy.get('exclude_flag'): flags.append("excluded")
        if hierarchy.get('calculation_flag'): flags.append("calculated")
        if flags:
            text_parts.append(f"Flags: {', '.join(flags)}")

        # Add formula context if present
        if hierarchy.get('formula_config'):
            formula = hierarchy['formula_config']
            text_parts.append(f"Formula: {formula.get('operation', '')} operation")

        return {
            "id": hierarchy['hierarchy_id'],
            "text": " | ".join(text_parts),
            "metadata": {
                "project_id": hierarchy.get('project_id'),
                "hierarchy_id": hierarchy['hierarchy_id'],
                "hierarchy_name": hierarchy['hierarchy_name'],
                "parent_id": hierarchy.get('parent_id'),
                "is_leaf": hierarchy.get('is_leaf_node', False),
                "has_mappings": len(hierarchy.get('source_mappings', [])) > 0,
                "level_depth": self._get_level_depth(hierarchy),
            }
        }

    def embed_perspective(self, perspective: Dict[str, Any]) -> Dict[str, Any]:
        """
        Embed a cross-perspective relationship.
        Example: Same asset viewed by Operations vs Finance vs Tax.
        """
        text_parts = [
            f"Perspective: {perspective['name']}",
            f"Grouping Logic: {perspective['grouping_logic']}",
            f"Financial Use Case: {perspective['use_case']}",
            f"Industry: {perspective.get('industry', 'General')}",
        ]

        # Add the reconciliation context
        if perspective.get('reconciliation_note'):
            text_parts.append(f"Reconciliation: {perspective['reconciliation_note']}")

        return {
            "id": perspective['id'],
            "text": " | ".join(text_parts),
            "metadata": perspective
        }

    def embed_reconciliation_problem(self, problem: Dict[str, Any]) -> Dict[str, Any]:
        """
        Embed a reconciliation problem pattern.
        Captures: multiple valid views, why they differ, how to reconcile.
        """
        text = f"""
        Industry: {problem['industry']}
        Scenario: {problem['scenario']}
        Amount: {problem['amount']}
        Perspectives: {', '.join([p['name'] for p in problem['perspectives']])}
        Reconciliation Challenge: {problem['challenge']}
        Solution: {problem['solution']}
        """

        return {
            "id": problem['id'],
            "text": text.strip(),
            "metadata": {
                "industry": problem['industry'],
                "perspective_count": len(problem['perspectives']),
                "amount": problem['amount'],
            }
        }
```

#### 3. Vector Store Wrapper (`store.py`)

```python
import chromadb
from chromadb.config import Settings
from typing import List, Dict, Any, Optional

class VectorStore:
    """Unified interface for vector operations."""

    def __init__(self, settings: VectorSettings):
        self.settings = settings
        self.client = chromadb.PersistentClient(
            path=settings.vector_db_path,
            settings=Settings(anonymized_telemetry=False)
        )
        self._init_collections()

    def _init_collections(self):
        """Initialize all collections."""
        self.hierarchies = self.client.get_or_create_collection(
            name=self.settings.collection_hierarchies,
            metadata={"description": "Hierarchy node embeddings"}
        )
        self.mappings = self.client.get_or_create_collection(
            name=self.settings.collection_mappings,
            metadata={"description": "Source mapping embeddings"}
        )
        self.perspectives = self.client.get_or_create_collection(
            name=self.settings.collection_perspectives,
            metadata={"description": "Cross-perspective relationships"}
        )
        self.industry_patterns = self.client.get_or_create_collection(
            name=self.settings.collection_industry,
            metadata={"description": "Industry-specific patterns"}
        )
        self.concepts = self.client.get_or_create_collection(
            name=self.settings.collection_concepts,
            metadata={"description": "Whitepaper methodology concepts"}
        )

    def index_hierarchy(self, hierarchy_data: Dict[str, Any], embedding: List[float]):
        """Add or update a hierarchy in the vector store."""
        self.hierarchies.upsert(
            ids=[hierarchy_data['id']],
            embeddings=[embedding],
            documents=[hierarchy_data['text']],
            metadatas=[hierarchy_data['metadata']]
        )

    def search_hierarchies(
        self,
        query_embedding: List[float],
        top_k: int = 10,
        filters: Optional[Dict] = None
    ) -> List[Dict]:
        """Find similar hierarchies."""
        results = self.hierarchies.query(
            query_embeddings=[query_embedding],
            n_results=top_k,
            where=filters
        )
        return self._format_results(results)

    def search_by_text(
        self,
        query: str,
        collection: str = "hierarchies",
        top_k: int = 10
    ) -> List[Dict]:
        """Search using text query (requires embedding)."""
        # Get embedding for query
        embedding = self.embedder.embed_text(query)

        # Search appropriate collection
        coll = getattr(self, collection)
        results = coll.query(
            query_embeddings=[embedding],
            n_results=top_k
        )
        return self._format_results(results)

    def get_reconciliation_context(
        self,
        hierarchy_id: str,
        industry: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get full reconciliation context for a hierarchy.
        Returns: related perspectives, industry patterns, similar hierarchies.
        """
        # Get the hierarchy
        hierarchy = self.hierarchies.get(ids=[hierarchy_id])

        # Find similar hierarchies
        similar = self.search_hierarchies(
            hierarchy['embeddings'][0],
            top_k=5
        )

        # Find relevant perspectives
        perspectives = self.perspectives.query(
            query_embeddings=hierarchy['embeddings'],
            n_results=5,
            where={"industry": industry} if industry else None
        )

        # Find industry patterns
        patterns = self.industry_patterns.query(
            query_embeddings=hierarchy['embeddings'],
            n_results=3,
            where={"industry": industry} if industry else None
        )

        return {
            "hierarchy": hierarchy,
            "similar_hierarchies": similar,
            "perspectives": self._format_results(perspectives),
            "industry_patterns": self._format_results(patterns),
        }
```

#### 4. Industry Pattern Library (`industry_patterns.py`)

```python
"""
Pre-loaded industry patterns from the whitepaper.
These help AI understand reconciliation problems across industries.
"""

INDUSTRY_PATTERNS = {
    "oil_and_gas": {
        "name": "Oil & Gas Operations",
        "perspectives": [
            {
                "name": "Geological",
                "grouping_logic": "Basin → Play → Formation → Well",
                "use_case": "Reserve estimation, decline curves, type curves"
            },
            {
                "name": "Production",
                "grouping_logic": "Field → Battery → Well → Completion",
                "use_case": "Volumetric reporting, allocation, run tickets"
            },
            {
                "name": "Finance",
                "grouping_logic": "Business Unit → AFE → Cost Center → GL Account",
                "use_case": "P&L, lease operating statements, JIB"
            },
            {
                "name": "Land",
                "grouping_logic": "Lease → Spacing Unit → Division Order",
                "use_case": "Working interest, NRI, royalty burdens"
            },
            {
                "name": "Facilities",
                "grouping_logic": "System → Gathering → Compression → Processing",
                "use_case": "Midstream cost allocation, throughput fees"
            }
        ],
        "reconciliation_problem": """
            The same well appears in 5+ systems with different identifiers.
            ARIES shows reserves value. ComboCurve shows production forecast.
            ProCount shows actual volumes and JIB. GL shows booked revenue.
            Reconciling these takes 2-3 weeks per reporting cycle.
        """,
        "unified_hierarchy_solution": """
            Bidirectional mappings from each source system to a unified
            financial hierarchy. Well API-14 serves as the atomic key.
            Each perspective maintains its valid grouping while reconciling
            to the same transaction-verified GL foundation.
        """
    },

    "manufacturing": {
        "name": "Manufacturing Operations",
        "perspectives": [
            {
                "name": "Operations",
                "grouping_logic": "Production Cell → Work Center → Machine",
                "use_case": "OEE, efficiency metrics, downtime analysis"
            },
            {
                "name": "Product Costing",
                "grouping_logic": "Product Line → SKU → BOM Component",
                "use_case": "Gross margin, standard cost variance, pricing"
            },
            {
                "name": "Project Accounting",
                "grouping_logic": "Initiative → Project → Work Package",
                "use_case": "Capital justification, ROI tracking"
            },
            {
                "name": "Maintenance",
                "grouping_logic": "Asset Class → Equipment Type → Asset ID",
                "use_case": "Maintenance budget, CMMS integration"
            },
            {
                "name": "Plant Controller",
                "grouping_logic": "Cost Center → Department → GL Account",
                "use_case": "P&L accountability, budget variance"
            }
        ],
        "reconciliation_problem": """
            $2.4M monthly operating costs must satisfy 5 valid structures.
            Product Costing shows 12% over standard. Operations shows 94% OEE.
            Project Accounting shows $180K savings. All are correct.
            CFO spends days reconciling before answering margin questions.
        """,
        "unified_hierarchy_solution": """
            Unified hierarchy maintains all 5 perspectives simultaneously.
            Same transaction flows through Product Line AND Cost Center AND
            Project. Bidirectional mappings ensure totals always tie.
        """
    },

    "healthcare": {
        "name": "Healthcare System",
        "perspectives": [
            {
                "name": "Clinical Operations",
                "grouping_logic": "Service Line → Department → Provider",
                "use_case": "Contribution margin by specialty"
            },
            {
                "name": "Facilities",
                "grouping_logic": "Campus → Building → Floor → Unit",
                "use_case": "Space cost allocation, lease analysis"
            },
            {
                "name": "Finance",
                "grouping_logic": "Cost Center → GL Account",
                "use_case": "Budget variance, financial close"
            },
            {
                "name": "Research Administration",
                "grouping_logic": "Grant → Project → Task",
                "use_case": "Grant compliance, indirect cost recovery"
            },
            {
                "name": "Supply Chain",
                "grouping_logic": "Vendor → Category → Item",
                "use_case": "Spend analytics, contract negotiation"
            }
        ],
        "reconciliation_problem": """
            $8.2M monthly supply expense viewed 5 different ways.
            Research says grant is on budget. Finance shows cost center over.
            Clinical reports margin declining. Is it volume, cost, or allocation?
            Takes 2 weeks to reconcile—by then the variance is baked in.
        """,
        "unified_hierarchy_solution": """
            Same supply dollar maps to Service Line AND Grant AND Cost Center.
            Hierarchy preserves granularity—never aggregates destructively.
            AI can answer: 'What drove the Cardiology margin decline?' instantly.
        """
    },

    "private_equity": {
        "name": "Private Equity Portfolio",
        "perspectives": [
            {
                "name": "Operating Company",
                "grouping_logic": "Business Unit → Product Line → Region",
                "use_case": "Operational accountability, management bonuses"
            },
            {
                "name": "Fund Reporting",
                "grouping_logic": "Consolidated with add-backs",
                "use_case": "LP reporting, fund performance"
            },
            {
                "name": "Lender/Covenants",
                "grouping_logic": "Adjusted per credit agreement",
                "use_case": "Debt compliance, covenant calculations"
            },
            {
                "name": "Tax",
                "grouping_logic": "Legal entity structure",
                "use_case": "Tax planning, transfer pricing"
            },
            {
                "name": "Exit/M&A",
                "grouping_logic": "Carve-out adjusted",
                "use_case": "Quality of earnings, buyer diligence"
            }
        ],
        "reconciliation_problem": """
            $47M EBITDA looks different to everyone: $44M for covenants,
            $51M for exit. Operating team celebrates hitting target.
            CFO worries about covenant headroom. Deal team prepares QofE.
            40+ hours per portfolio company per quarter reconciling.
        """,
        "unified_hierarchy_solution": """
            Unified hierarchy maintains all 5 EBITDA definitions.
            Same add-back applied differently per perspective.
            Audit trail shows exactly why each EBITDA differs.
        """
    },

    "retail": {
        "name": "Retail / Multi-Location",
        "perspectives": [
            {
                "name": "Merchandising",
                "grouping_logic": "Category → Subcategory → SKU",
                "use_case": "Margin analysis, assortment planning"
            },
            {
                "name": "Real Estate",
                "grouping_logic": "Lease Agreement → Property → Location",
                "use_case": "Occupancy cost, ASC 842 compliance"
            },
            {
                "name": "Operations",
                "grouping_logic": "Region → District → Store",
                "use_case": "Labor productivity, store P&L"
            },
            {
                "name": "Marketing",
                "grouping_logic": "Campaign → Channel → Promotion",
                "use_case": "Marketing ROI, attribution"
            },
            {
                "name": "Finance",
                "grouping_logic": "Legal Entity → Cost Center → GL",
                "use_case": "Statutory reporting, financial close"
            }
        ],
        "reconciliation_problem": """
            $120M quarterly revenue across 200 stores. Merchandising shows
            Apparel margin up 200bps. Operations shows District 7 comps down.
            Real Estate shows occupancy improving. Marketing claims $4M lift.
            CFO needs coherent story—takes 2 days of spreadsheet wrangling.
        """,
        "unified_hierarchy_solution": """
            Same transaction tagged to Category AND Store AND Campaign.
            AI answers: 'Is Apparel margin real or just a mix shift?'
            without manual crosswalks or spreadsheet reconciliation.
        """
    },

    "construction": {
        "name": "Construction / Capital Projects",
        "perspectives": [
            {
                "name": "Project Management",
                "grouping_logic": "Phase → Deliverable → Activity",
                "use_case": "Earned value, schedule variance"
            },
            {
                "name": "Procurement",
                "grouping_logic": "Subcontractor → Contract → Change Order",
                "use_case": "Commitment tracking, exposure analysis"
            },
            {
                "name": "Job Costing",
                "grouping_logic": "Cost Code → Cost Type → Resource",
                "use_case": "Bid vs actual, estimating feedback"
            },
            {
                "name": "Finance",
                "grouping_logic": "Job → WBS → GL Account",
                "use_case": "WIP schedule, revenue recognition"
            },
            {
                "name": "Owner Billing",
                "grouping_logic": "Pay Application → Schedule of Values",
                "use_case": "Cash flow, retention tracking"
            }
        ],
        "reconciliation_problem": """
            $50M project tracked 5 ways. PM says 94% complete, under budget.
            Procurement shows $1.2M pending change orders. Job costing shows
            concrete 18% over estimate. Finance needs to book revenue.
            Project controller spends a week monthly reconciling.
        """,
        "unified_hierarchy_solution": """
            Phase completion maps to Cost Code maps to Job maps to Billing.
            Change orders propagate to all perspectives automatically.
            Margin at risk visible in real-time across all views.
        """
    }
}


def load_industry_patterns(vector_store: VectorStore, embedder):
    """Load all industry patterns into the vector store."""
    for industry_key, pattern in INDUSTRY_PATTERNS.items():
        # Embed the main pattern
        pattern_text = f"""
        Industry: {pattern['name']}
        Perspectives: {', '.join([p['name'] for p in pattern['perspectives']])}
        Problem: {pattern['reconciliation_problem']}
        Solution: {pattern['unified_hierarchy_solution']}
        """

        embedding = embedder.embed_text(pattern_text)
        vector_store.industry_patterns.upsert(
            ids=[industry_key],
            embeddings=[embedding],
            documents=[pattern_text],
            metadatas=[{
                "industry": industry_key,
                "perspective_count": len(pattern['perspectives']),
            }]
        )

        # Embed each perspective
        for i, perspective in enumerate(pattern['perspectives']):
            perspective_id = f"{industry_key}_perspective_{i}"
            perspective_text = f"""
            Industry: {pattern['name']}
            Perspective: {perspective['name']}
            Grouping Logic: {perspective['grouping_logic']}
            Use Case: {perspective['use_case']}
            """

            embedding = embedder.embed_text(perspective_text)
            vector_store.perspectives.upsert(
                ids=[perspective_id],
                embeddings=[embedding],
                documents=[perspective_text],
                metadatas=[{
                    "industry": industry_key,
                    "perspective_name": perspective['name'],
                    "grouping_logic": perspective['grouping_logic'],
                }]
            )
```

#### 5. Whitepaper Concept Loader (`whitepaper_loader.py`)

```python
"""
Load core whitepaper concepts into the vector store.
Enables RAG for AI to understand the methodology.
"""

WHITEPAPER_CONCEPTS = [
    {
        "id": "core_problem",
        "title": "The Fragmentation Problem",
        "content": """
            Organizations manage financial data across 5+ specialized systems,
            each with its own categorization schema. This fragmentation causes
            2-3 weeks of manual reconciliation per reporting cycle before
            meaningful analysis can begin. Different departments require
            different but equally valid perspectives on the same assets.
        """
    },
    {
        "id": "unified_hierarchy",
        "title": "Unified Financial Hierarchy",
        "content": """
            A universal translation layer across heterogeneous systems.
            Functions as bidirectional mappings between source systems and
            a unified financial hierarchy. Never aggregates destructively—
            preserves source granularity while enabling reconciliation.
        """
    },
    {
        "id": "six_essential_elements",
        "title": "Six Essential Elements",
        "content": """
            1. Accounting system (GL) as verification backbone
            2. Bidirectional mappings between source systems and hierarchy
            3. Preserved source granularity (aggregation never destroys detail)
            4. Temporal alignment (production months, accounting periods, forecast vintages)
            5. Ownership interest precision (WI%, NRI, Division Orders)
            6. Date logic standardization (production vs. accounting vs. reporting periods)
        """
    },
    {
        "id": "gl_backbone",
        "title": "GL as Verification Backbone",
        "content": """
            The General Ledger serves as the verified truth. All other systems
            must reconcile back to GL. This doesn't mean GL is the master—
            it means GL provides the verification checkpoint. Operational systems
            have richer granularity, but their totals must tie to GL.
        """
    },
    {
        "id": "bidirectional_mappings",
        "title": "Bidirectional Mappings",
        "content": """
            Source system → Hierarchy (aggregation path)
            Hierarchy → Source system (drill-down path)

            Both directions must be maintained. Users can start from any view
            and navigate to any other view without losing traceability.
            The mapping table is the integration point—not data duplication.
        """
    },
    {
        "id": "preserved_granularity",
        "title": "Preserved Source Granularity",
        "content": """
            Never aggregate destructively. The hierarchy doesn't replace
            source systems—it maps to them. A summary report pulls from
            the hierarchy, but drill-down always reaches source-level detail.
            This is why bidirectional mappings are essential.
        """
    },
    {
        "id": "temporal_alignment",
        "title": "Temporal Alignment",
        "content": """
            Production months ≠ Accounting periods ≠ Forecast vintages.
            January production may book in February, be restated in March,
            and reforecasted in April. The hierarchy must track all four dates:
            - Production date (when volumes occurred)
            - Accounting date (when booked to GL)
            - Reporting date (when reported externally)
            - Forecast vintage (which forecast version predicted it)
        """
    },
    {
        "id": "ownership_precision",
        "title": "Ownership Interest Precision",
        "content": """
            Working Interest (WI%) ≠ Net Revenue Interest (NRI) ≠ Division Order interest.
            The same 100 barrels of oil becomes different amounts depending on
            ownership structure. The hierarchy must preserve all interest types
            and apply them correctly per perspective.
        """
    },
    {
        "id": "functional_hierarchy",
        "title": "One Dimension, Every Perspective",
        "content": """
            Each department groups the same assets differently—and they're all right.
            Operations groups by physical location (production cell, well pad).
            Finance groups by cost allocation (cost center, GL account).
            Tax groups by legal entity. M&A groups by carve-out scope.

            The unified hierarchy doesn't pick a winner. It maintains all
            perspectives and ensures they reconcile to the same foundation.
        """
    },
    {
        "id": "reconciliation_value",
        "title": "Reconciliation Value Proposition",
        "content": """
            75-90% reduction in COA implementation timelines
            100+ hours/month eliminated from reconciliation
            $765,000+ annual value per mid-sized operator
            8-10 week implementation timeline

            The value isn't the hierarchy itself—it's the elimination of
            manual reconciliation work that currently blocks analysis.
        """
    },
    {
        "id": "ai_enablement",
        "title": "AI as First-Class Consumer",
        "content": """
            The hierarchy serves as foundational infrastructure for AI/ML.
            Cortex natural language queries, anomaly detection, and predictive
            analytics only work with consistent, well-mapped data structures.

            Without the hierarchy, AI answers are unreliable because the AI
            can't reconcile conflicting source systems. With the hierarchy,
            AI has a single verified truth to query.
        """
    }
]


def load_whitepaper_concepts(vector_store: VectorStore, embedder):
    """Load all whitepaper concepts into the vector store."""
    for concept in WHITEPAPER_CONCEPTS:
        full_text = f"{concept['title']}: {concept['content']}"
        embedding = embedder.embed_text(full_text)

        vector_store.concepts.upsert(
            ids=[concept['id']],
            embeddings=[embedding],
            documents=[full_text],
            metadatas=[{
                "concept_id": concept['id'],
                "title": concept['title'],
            }]
        )
```

#### 6. RAG Pipeline (`retriever.py`)

```python
"""
RAG (Retrieval Augmented Generation) pipeline for AI context.
"""

from typing import List, Dict, Any, Optional

class HierarchyRAG:
    """Retrieve relevant context for AI queries about hierarchies."""

    def __init__(self, vector_store: VectorStore, embedder):
        self.vector_store = vector_store
        self.embedder = embedder

    def get_context_for_query(
        self,
        query: str,
        project_id: Optional[str] = None,
        industry: Optional[str] = None,
        include_concepts: bool = True,
        include_patterns: bool = True,
        top_k: int = 5
    ) -> Dict[str, Any]:
        """
        Retrieve comprehensive context for an AI query.

        Returns context from:
        - Relevant hierarchies in the project
        - Applicable industry patterns
        - Core whitepaper concepts
        - Similar past queries/examples
        """
        query_embedding = self.embedder.embed_text(query)

        context = {
            "query": query,
            "hierarchies": [],
            "concepts": [],
            "industry_patterns": [],
            "perspectives": [],
        }

        # Search relevant hierarchies
        hierarchy_filters = {}
        if project_id:
            hierarchy_filters["project_id"] = project_id

        context["hierarchies"] = self.vector_store.search_hierarchies(
            query_embedding,
            top_k=top_k,
            filters=hierarchy_filters if hierarchy_filters else None
        )

        # Search whitepaper concepts
        if include_concepts:
            concepts_results = self.vector_store.concepts.query(
                query_embeddings=[query_embedding],
                n_results=3
            )
            context["concepts"] = self._format_results(concepts_results)

        # Search industry patterns
        if include_patterns:
            pattern_filters = {"industry": industry} if industry else None
            patterns_results = self.vector_store.industry_patterns.query(
                query_embeddings=[query_embedding],
                n_results=3,
                where=pattern_filters
            )
            context["industry_patterns"] = self._format_results(patterns_results)

            # Also get relevant perspectives
            perspectives_results = self.vector_store.perspectives.query(
                query_embeddings=[query_embedding],
                n_results=5,
                where=pattern_filters
            )
            context["perspectives"] = self._format_results(perspectives_results)

        return context

    def format_context_for_llm(self, context: Dict[str, Any]) -> str:
        """Format retrieved context into a prompt-friendly string."""
        sections = []

        if context["hierarchies"]:
            sections.append("## Relevant Hierarchies")
            for h in context["hierarchies"]:
                sections.append(f"- {h['document']}")

        if context["concepts"]:
            sections.append("\n## Core Concepts (from methodology)")
            for c in context["concepts"]:
                sections.append(f"- {c['document']}")

        if context["industry_patterns"]:
            sections.append("\n## Industry Patterns")
            for p in context["industry_patterns"]:
                sections.append(f"- {p['document']}")

        if context["perspectives"]:
            sections.append("\n## Relevant Perspectives")
            for p in context["perspectives"]:
                sections.append(f"- {p['document']}")

        return "\n".join(sections)

    def answer_with_context(
        self,
        query: str,
        project_id: Optional[str] = None,
        industry: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get context and format it for LLM consumption.
        Returns both raw context and formatted prompt augmentation.
        """
        context = self.get_context_for_query(
            query,
            project_id=project_id,
            industry=industry
        )

        formatted = self.format_context_for_llm(context)

        return {
            "raw_context": context,
            "formatted_context": formatted,
            "augmented_prompt": f"""
Based on the following context about financial hierarchies and reconciliation:

{formatted}

User Query: {query}

Please provide a response that leverages this context about unified financial hierarchies,
cross-perspective reconciliation, and the specific hierarchies/patterns retrieved.
"""
        }
```

---

## New MCP Tools for Vector Operations

Add these tools to the MCP server:

```python
# mcp/vector_tools.py

@mcp.tool()
def search_similar_hierarchies(
    query: str,
    project_id: str = None,
    top_k: int = 5
) -> str:
    """
    Find hierarchies similar to the query using semantic search.

    Args:
        query: Natural language description of what you're looking for
        project_id: Optional project to search within
        top_k: Number of results to return

    Returns:
        JSON array of similar hierarchies with similarity scores
    """
    results = rag.vector_store.search_by_text(
        query,
        collection="hierarchies",
        top_k=top_k
    )
    return json.dumps(results, indent=2)


@mcp.tool()
def get_reconciliation_context(
    query: str,
    industry: str = None
) -> str:
    """
    Get full context for a reconciliation question.
    Includes relevant concepts, patterns, and perspectives.

    Args:
        query: The question about reconciliation or hierarchies
        industry: Optional industry filter (oil_and_gas, manufacturing, etc.)

    Returns:
        Comprehensive context for AI to answer the question
    """
    context = rag.answer_with_context(query, industry=industry)
    return json.dumps({
        "augmented_prompt": context["augmented_prompt"],
        "sources": {
            "hierarchies": len(context["raw_context"]["hierarchies"]),
            "concepts": len(context["raw_context"]["concepts"]),
            "patterns": len(context["raw_context"]["industry_patterns"]),
        }
    }, indent=2)


@mcp.tool()
def get_industry_pattern(industry: str) -> str:
    """
    Get the reconciliation pattern for a specific industry.

    Args:
        industry: One of: oil_and_gas, manufacturing, healthcare,
                  private_equity, retail, construction

    Returns:
        Industry pattern including perspectives, problem, and solution
    """
    pattern = INDUSTRY_PATTERNS.get(industry)
    if not pattern:
        return json.dumps({"error": f"Unknown industry: {industry}"})
    return json.dumps(pattern, indent=2)


@mcp.tool()
def explain_perspective_difference(
    hierarchy_id: str,
    perspective_a: str,
    perspective_b: str
) -> str:
    """
    Explain why the same hierarchy looks different from two perspectives.

    Args:
        hierarchy_id: The hierarchy to analyze
        perspective_a: First perspective (e.g., "Operations")
        perspective_b: Second perspective (e.g., "Finance")

    Returns:
        Explanation of the differences and how they reconcile
    """
    # Retrieve context about both perspectives
    context = rag.get_context_for_query(
        f"Compare {perspective_a} and {perspective_b} views of hierarchy",
        include_patterns=True
    )

    return json.dumps({
        "hierarchy_id": hierarchy_id,
        "perspectives_compared": [perspective_a, perspective_b],
        "reconciliation_explanation": context["formatted_context"],
    }, indent=2)


@mcp.tool()
def index_project_for_ai(project_id: str) -> str:
    """
    Index all hierarchies in a project for AI semantic search.
    Run this after creating or importing hierarchies.

    Args:
        project_id: Project to index

    Returns:
        Indexing statistics
    """
    hierarchies = hierarchy_service.list_hierarchies(project_id)
    indexed = 0

    for h in hierarchies:
        embed_data = embedder.embed_hierarchy(h.to_dict())
        embedding = embedder.embed_text(embed_data["text"])
        vector_store.index_hierarchy(embed_data, embedding)
        indexed += 1

    return json.dumps({
        "success": True,
        "project_id": project_id,
        "hierarchies_indexed": indexed
    }, indent=2)
```

---

## Additional Python Libraries for Vectors

Add to `requirements.txt`:

```
# Vector Store & Embeddings
chromadb>=0.4.22              # Vector database
sentence-transformers>=2.5.0  # Local embeddings (offline capable)

# Optional: OpenAI embeddings (requires API key)
# openai>=1.12.0

# Optional: Alternative vector stores
# qdrant-client>=1.7.0
# faiss-cpu>=1.7.4
```

Add to `requirements-optional.txt`:

```
# Cloud Embeddings
openai>=1.12.0                # OpenAI embeddings API

# Alternative Vector Stores
qdrant-client>=1.7.0          # Qdrant vector database
faiss-cpu>=1.7.4              # Facebook AI Similarity Search

# Advanced Embeddings
instructor-embedding>=1.2.0    # Instruction-tuned embeddings
cohere>=4.47                   # Cohere embeddings
```

---

## CLI Commands for Vector Operations

```bash
# Index a project for AI search
databridge vector index <project-id>

# Search for similar hierarchies
databridge vector search "revenue recognition for oil wells"

# Get industry pattern
databridge vector pattern oil_and_gas

# Initialize with whitepaper concepts
databridge vector init-concepts

# Get reconciliation context
databridge vector context "Why does EBITDA differ between reports?"
```

---

## Integration with Existing MCP Server

The vector capabilities integrate seamlessly:

1. **On hierarchy create/update**: Automatically index to vector store
2. **On query**: Augment with retrieved context
3. **On template instantiation**: Index new hierarchies
4. **On import**: Bulk index imported hierarchies

This enables Claude to:
- Find similar hierarchies semantically
- Understand reconciliation patterns by industry
- Explain why perspectives differ
- Provide contextual answers using RAG


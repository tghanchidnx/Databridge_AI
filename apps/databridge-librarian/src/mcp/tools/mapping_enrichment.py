"""
Mapping Enrichment Module - Configurable COA/Reference Data Expansion.

This module provides:
1. Configurable detail columns for mapping exports
2. Reference data loading from CSV or database connections
3. AI-driven post-hierarchy enrichment workflow
4. Pattern matching expansion (ILIKE, IN, =, BETWEEN)
"""

import csv
import os
import re
import json
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field, asdict
from datetime import datetime


@dataclass
class DataSourceConfig:
    """Configuration for a reference data source."""
    source_id: str
    source_type: str  # 'csv' or 'database'
    source_path: str  # File path for CSV, connection string for DB
    table_name: str
    key_column: str  # Column used for matching (e.g., ACCOUNT_CODE)
    detail_columns: List[str] = field(default_factory=list)
    display_name: str = ""
    description: str = ""

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> 'DataSourceConfig':
        return cls(**data)


@dataclass
class EnrichmentConfig:
    """Configuration for mapping enrichment."""
    project_id: str
    client_id: str = ""
    data_sources: List[DataSourceConfig] = field(default_factory=list)
    default_detail_columns: Dict[str, List[str]] = field(default_factory=dict)
    created_at: str = ""
    updated_at: str = ""

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now().isoformat()
        self.updated_at = datetime.now().isoformat()

    def to_dict(self) -> dict:
        return {
            'project_id': self.project_id,
            'client_id': self.client_id,
            'data_sources': [ds.to_dict() for ds in self.data_sources],
            'default_detail_columns': self.default_detail_columns,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'EnrichmentConfig':
        data_sources = [DataSourceConfig.from_dict(ds) for ds in data.get('data_sources', [])]
        return cls(
            project_id=data['project_id'],
            client_id=data.get('client_id', ''),
            data_sources=data_sources,
            default_detail_columns=data.get('default_detail_columns', {}),
            created_at=data.get('created_at', ''),
            updated_at=data.get('updated_at', ''),
        )


class ReferenceDataLoader:
    """Loads reference data from CSV or database sources."""

    def __init__(self):
        self.cache: Dict[str, Dict[str, dict]] = {}

    def load_csv(self, file_path: str, key_column: str) -> Dict[str, dict]:
        """Load CSV file into dictionary keyed by specified column."""
        cache_key = f"csv:{file_path}:{key_column}"
        if cache_key in self.cache:
            return self.cache[cache_key]

        data = {}
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            self.columns = reader.fieldnames or []
            for row in reader:
                key = row.get(key_column, '').strip()
                if key:
                    data[key] = {k: v for k, v in row.items()}

        self.cache[cache_key] = data
        return data

    def load_database(self, connection_string: str, table_name: str,
                      key_column: str, columns: List[str] = None) -> Dict[str, dict]:
        """Load data from database table."""
        try:
            from sqlalchemy import create_engine, text

            cache_key = f"db:{connection_string}:{table_name}:{key_column}"
            if cache_key in self.cache:
                return self.cache[cache_key]

            engine = create_engine(connection_string)

            col_list = ', '.join(columns) if columns else '*'
            query = f"SELECT {col_list} FROM {table_name}"

            data = {}
            with engine.connect() as conn:
                result = conn.execute(text(query))
                for row in result:
                    row_dict = dict(row._mapping)
                    key = str(row_dict.get(key_column, '')).strip()
                    if key:
                        data[key] = row_dict

            self.cache[cache_key] = data
            return data
        except Exception as e:
            raise ValueError(f"Database load failed: {e}")

    def get_available_columns(self, source_config: DataSourceConfig) -> List[str]:
        """Get available columns from a data source."""
        if source_config.source_type == 'csv':
            with open(source_config.source_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                return reader.fieldnames or []
        elif source_config.source_type == 'database':
            try:
                from sqlalchemy import create_engine, inspect
                engine = create_engine(source_config.source_path)
                inspector = inspect(engine)
                columns = inspector.get_columns(source_config.table_name)
                return [col['name'] for col in columns]
            except Exception as e:
                return []
        return []


class PatternMatcher:
    """Matches SQL patterns to reference data keys."""

    @staticmethod
    def match_ilike(pattern: str, keys: List[str]) -> List[str]:
        """Match SQL ILIKE pattern to keys."""
        pattern = pattern.strip()
        matches = []

        if pattern.endswith('%') and not pattern.startswith('%'):
            # Prefix match: '501%'
            prefix = pattern[:-1].lower()
            matches = [k for k in keys if k.lower().startswith(prefix)]
        elif pattern.startswith('%') and not pattern.endswith('%'):
            # Suffix match: '%990'
            suffix = pattern[1:].lower()
            matches = [k for k in keys if k.lower().endswith(suffix)]
        elif pattern.startswith('%') and pattern.endswith('%'):
            # Contains match: '%gas%'
            contains = pattern[1:-1].lower()
            matches = [k for k in keys if contains in k.lower()]
        else:
            # Exact match (case-insensitive)
            matches = [k for k in keys if k.lower() == pattern.lower()]

        return matches

    @staticmethod
    def match_in(values: List[str], keys: List[str]) -> List[str]:
        """Match IN clause values to keys."""
        return [k for k in keys if k in values]

    @staticmethod
    def match_exact(value: str, keys: List[str]) -> List[str]:
        """Match exact value to keys."""
        return [k for k in keys if k == value]

    @staticmethod
    def match_between(start: str, end: str, keys: List[str]) -> List[str]:
        """Match BETWEEN range to keys."""
        return [k for k in keys if start <= k <= end]


class MappingEnricher:
    """Enriches mapping files with reference data details."""

    def __init__(self, config: EnrichmentConfig = None):
        self.config = config
        self.loader = ReferenceDataLoader()
        self.matcher = PatternMatcher()
        self.reference_data: Dict[str, Dict[str, dict]] = {}

    def load_reference_data(self, source_config: DataSourceConfig) -> int:
        """Load reference data from configured source."""
        if source_config.source_type == 'csv':
            data = self.loader.load_csv(
                source_config.source_path,
                source_config.key_column
            )
        elif source_config.source_type == 'database':
            data = self.loader.load_database(
                source_config.source_path,
                source_config.table_name,
                source_config.key_column,
                source_config.detail_columns
            )
        else:
            raise ValueError(f"Unknown source type: {source_config.source_type}")

        self.reference_data[source_config.source_id] = data
        return len(data)

    def expand_mapping_row(self, row: dict, source_config: DataSourceConfig) -> List[dict]:
        """Expand a single mapping row with reference data matches."""
        condition_type = row.get('CONDITION_TYPE', '').upper()
        condition_value = row.get('CONDITION_VALUE', '')
        source_column = row.get('SOURCE_COLUMN', '').lower()

        # Check if this mapping relates to the reference data
        ref_data = self.reference_data.get(source_config.source_id, {})
        if not ref_data:
            return [row]

        keys = list(ref_data.keys())
        matches = []

        # Match based on condition type
        if condition_type == 'ILIKE' or condition_type == 'LIKE':
            matches = self.matcher.match_ilike(condition_value, keys)
        elif condition_type == 'IN':
            # Parse IN values
            values = [v.strip().strip("'\"") for v in condition_value.split(',')]
            matches = self.matcher.match_in(values, keys)
        elif condition_type == '=':
            matches = self.matcher.match_exact(condition_value, keys)
        elif condition_type == 'BETWEEN':
            parts = condition_value.split(' AND ')
            if len(parts) == 2:
                matches = self.matcher.match_between(parts[0].strip(), parts[1].strip(), keys)

        if not matches:
            # No matches - return original row with empty detail columns
            expanded_row = row.copy()
            for col in source_config.detail_columns:
                expanded_row[f'EXPANDED_{col}'] = ''
            expanded_row['MATCH_TYPE'] = 'NO_MATCH'
            return [expanded_row]

        # Expand to one row per match
        expanded_rows = []
        for match_key in matches:
            expanded_row = row.copy()
            ref_record = ref_data[match_key]

            for col in source_config.detail_columns:
                value = ref_record.get(col, '')
                expanded_row[f'EXPANDED_{col}'] = value if value and str(value) != 'nan' else ''

            expanded_row['MATCH_TYPE'] = 'REFERENCE_MATCH'
            expanded_rows.append(expanded_row)

        return expanded_rows

    def enrich_mapping_file(self, mapping_path: str, source_config: DataSourceConfig,
                           output_path: str = None) -> dict:
        """Enrich a mapping CSV file with reference data."""
        stats = {
            'original_rows': 0,
            'expanded_rows': 0,
            'matched_rows': 0,
            'unmatched_rows': 0,
        }

        # Load reference data if not already loaded
        if source_config.source_id not in self.reference_data:
            self.load_reference_data(source_config)

        expanded_rows = []
        original_fieldnames = []

        with open(mapping_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            original_fieldnames = list(reader.fieldnames or [])

            for row in reader:
                stats['original_rows'] += 1
                expanded = self.expand_mapping_row(row, source_config)

                for exp_row in expanded:
                    if exp_row.get('MATCH_TYPE') == 'REFERENCE_MATCH':
                        stats['matched_rows'] += 1
                    else:
                        stats['unmatched_rows'] += 1

                expanded_rows.extend(expanded)

        stats['expanded_rows'] = len(expanded_rows)

        # Determine output path
        if not output_path:
            base_name = Path(mapping_path).stem
            output_dir = Path(mapping_path).parent / 'enriched'
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = str(output_dir / f"{base_name}_ENRICHED.csv")

        # Build output fieldnames
        output_fieldnames = original_fieldnames.copy()
        for col in source_config.detail_columns:
            exp_col = f'EXPANDED_{col}'
            if exp_col not in output_fieldnames:
                output_fieldnames.append(exp_col)
        if 'MATCH_TYPE' not in output_fieldnames:
            output_fieldnames.append('MATCH_TYPE')

        # Write enriched file
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=output_fieldnames)
            writer.writeheader()
            writer.writerows(expanded_rows)

        stats['output_path'] = output_path
        return stats


class EnrichmentConfigStore:
    """Persists enrichment configurations."""

    def __init__(self, storage_path: str = None):
        if storage_path is None:
            storage_path = os.path.join(
                os.path.dirname(__file__),
                '..', '..', '..', '..', 'data', 'enrichment_configs.json'
            )
        self.storage_path = storage_path
        self._ensure_storage()

    def _ensure_storage(self):
        """Ensure storage file exists."""
        os.makedirs(os.path.dirname(self.storage_path), exist_ok=True)
        if not os.path.exists(self.storage_path):
            with open(self.storage_path, 'w') as f:
                json.dump({'configs': {}}, f)

    def save_config(self, config: EnrichmentConfig) -> None:
        """Save an enrichment configuration."""
        with open(self.storage_path, 'r') as f:
            data = json.load(f)

        config.updated_at = datetime.now().isoformat()
        data['configs'][config.project_id] = config.to_dict()

        with open(self.storage_path, 'w') as f:
            json.dump(data, f, indent=2)

    def get_config(self, project_id: str) -> Optional[EnrichmentConfig]:
        """Get enrichment configuration for a project."""
        with open(self.storage_path, 'r') as f:
            data = json.load(f)

        config_data = data['configs'].get(project_id)
        if config_data:
            return EnrichmentConfig.from_dict(config_data)
        return None

    def list_configs(self) -> List[EnrichmentConfig]:
        """List all enrichment configurations."""
        with open(self.storage_path, 'r') as f:
            data = json.load(f)

        return [EnrichmentConfig.from_dict(c) for c in data['configs'].values()]

    def delete_config(self, project_id: str) -> bool:
        """Delete an enrichment configuration."""
        with open(self.storage_path, 'r') as f:
            data = json.load(f)

        if project_id in data['configs']:
            del data['configs'][project_id]
            with open(self.storage_path, 'w') as f:
                json.dump(data, f, indent=2)
            return True
        return False


# ============================================================================
# AI Enrichment Agent - Post-Hierarchy Creation Workflow
# ============================================================================

class EnrichmentAgent:
    """
    AI Agent that guides users through post-hierarchy data enrichment.

    After hierarchy creation, this agent:
    1. Detects which source tables were used in mappings
    2. Asks if user wants to add reference data for enrichment
    3. For each table, asks which detail columns to include
    4. Configures and executes the enrichment
    """

    def __init__(self, config_store: EnrichmentConfigStore = None):
        self.config_store = config_store or EnrichmentConfigStore()
        self.enricher = MappingEnricher()
        self.detected_tables: List[dict] = []
        self.user_selections: Dict[str, Any] = {}

    def detect_source_tables(self, mapping_data: List[dict]) -> List[dict]:
        """
        Detect unique source tables used in mapping conditions.

        Returns list of detected tables with metadata.
        """
        tables = {}

        for row in mapping_data:
            source_column = row.get('SOURCE_COLUMN', '')
            condition_type = row.get('CONDITION_TYPE', '')

            # Infer table from column patterns
            table_info = self._infer_table_from_column(source_column, condition_type)
            if table_info:
                table_key = table_info['table_name']
                if table_key not in tables:
                    tables[table_key] = {
                        'table_name': table_info['table_name'],
                        'key_column': table_info['key_column'],
                        'display_name': table_info['display_name'],
                        'condition_count': 0,
                        'sample_conditions': [],
                    }
                tables[table_key]['condition_count'] += 1
                if len(tables[table_key]['sample_conditions']) < 3:
                    tables[table_key]['sample_conditions'].append(
                        row.get('CONDITION_VALUE', '')
                    )

        self.detected_tables = list(tables.values())
        return self.detected_tables

    def _infer_table_from_column(self, column: str, condition_type: str) -> Optional[dict]:
        """Infer the reference table from column name patterns."""
        column_lower = column.lower()

        # Account-related columns -> DIM_ACCOUNT
        if any(kw in column_lower for kw in ['account', 'acct', 'gl_code', 'account_code']):
            return {
                'table_name': 'DIM_ACCOUNT',
                'key_column': 'ACCOUNT_CODE',
                'display_name': 'Chart of Accounts',
            }

        # Cost center columns -> DIM_COST_CENTER
        if any(kw in column_lower for kw in ['cost_center', 'cc', 'costcenter']):
            return {
                'table_name': 'DIM_COST_CENTER',
                'key_column': 'COST_CENTER_CODE',
                'display_name': 'Cost Centers',
            }

        # Entity/Company columns -> DIM_ENTITY
        if any(kw in column_lower for kw in ['entity', 'company', 'corp', 'fund']):
            return {
                'table_name': 'DIM_ENTITY',
                'key_column': 'ENTITY_CODE',
                'display_name': 'Entities',
            }

        # Product columns -> DIM_PRODUCT
        if any(kw in column_lower for kw in ['product', 'item', 'sku']):
            return {
                'table_name': 'DIM_PRODUCT',
                'key_column': 'PRODUCT_CODE',
                'display_name': 'Products',
            }

        # Project columns -> DIM_PROJECT
        if any(kw in column_lower for kw in ['project', 'job', 'work_order']):
            return {
                'table_name': 'DIM_PROJECT',
                'key_column': 'PROJECT_CODE',
                'display_name': 'Projects',
            }

        # AFE columns (Oil & Gas) -> DIM_AFE
        if any(kw in column_lower for kw in ['afe', 'authorization']):
            return {
                'table_name': 'DIM_AFE',
                'key_column': 'AFE_CODE',
                'display_name': 'AFE (Authorizations)',
            }

        # Well columns (Oil & Gas) -> DIM_WELL
        if any(kw in column_lower for kw in ['well', 'wellbore', 'api']):
            return {
                'table_name': 'DIM_WELL',
                'key_column': 'WELL_CODE',
                'display_name': 'Wells',
            }

        return None

    def generate_enrichment_prompt(self, project_id: str, hierarchy_name: str) -> dict:
        """
        Generate the prompt structure for post-hierarchy enrichment.

        Returns a structured prompt for the AI to present to the user.
        """
        if not self.detected_tables:
            return {
                'has_tables': False,
                'message': 'No reference tables detected in the mapping conditions.',
            }

        prompt = {
            'has_tables': True,
            'project_id': project_id,
            'hierarchy_name': hierarchy_name,
            'intro_message': f"""
Hierarchy '{hierarchy_name}' has been created successfully!

I detected {len(self.detected_tables)} reference table(s) used in your mappings:
""",
            'tables': [],
            'questions': [],
        }

        for i, table in enumerate(self.detected_tables, 1):
            prompt['tables'].append({
                'index': i,
                'table_name': table['table_name'],
                'display_name': table['display_name'],
                'key_column': table['key_column'],
                'condition_count': table['condition_count'],
                'sample_conditions': table['sample_conditions'],
            })

        # Generate questions for each table
        prompt['questions'] = [
            {
                'question_id': 'add_reference_data',
                'question': 'Would you like to add reference data to enrich your mapping exports?',
                'options': ['Yes', 'No'],
                'default': 'Yes',
            }
        ]

        for table in self.detected_tables:
            prompt['questions'].append({
                'question_id': f"source_{table['table_name']}",
                'question': f"For {table['display_name']} ({table['table_name']}), how would you like to provide the data?",
                'options': ['CSV File', 'Database Connection', 'Skip this table'],
                'default': 'CSV File',
                'depends_on': {'add_reference_data': 'Yes'},
            })
            prompt['questions'].append({
                'question_id': f"columns_{table['table_name']}",
                'question': f"Which detail columns would you like to include from {table['display_name']}?",
                'type': 'multi_select',
                'options': [],  # Will be populated after data source is selected
                'depends_on': {f"source_{table['table_name']}": ['CSV File', 'Database Connection']},
            })

        return prompt

    def process_user_response(self, response: dict) -> dict:
        """
        Process user's responses to enrichment questions.

        Returns configuration or next question.
        """
        self.user_selections.update(response)

        # Check if user wants enrichment
        if self.user_selections.get('add_reference_data') == 'No':
            return {
                'status': 'skipped',
                'message': 'Enrichment skipped. Your hierarchy is ready to use.',
            }

        # Build configuration from responses
        config = EnrichmentConfig(
            project_id=self.user_selections.get('project_id', ''),
            client_id=self.user_selections.get('client_id', ''),
        )

        for table in self.detected_tables:
            table_name = table['table_name']
            source_type = self.user_selections.get(f"source_{table_name}")

            if source_type == 'Skip this table':
                continue

            source_path = self.user_selections.get(f"path_{table_name}", '')
            columns = self.user_selections.get(f"columns_{table_name}", [])

            if source_type and source_path and columns:
                ds_config = DataSourceConfig(
                    source_id=table_name.lower(),
                    source_type='csv' if source_type == 'CSV File' else 'database',
                    source_path=source_path,
                    table_name=table_name,
                    key_column=table['key_column'],
                    detail_columns=columns,
                    display_name=table['display_name'],
                )
                config.data_sources.append(ds_config)
                config.default_detail_columns[table_name] = columns

        return {
            'status': 'configured',
            'config': config,
            'message': f'Enrichment configured with {len(config.data_sources)} data source(s).',
        }

    def execute_enrichment(self, mapping_path: str, config: EnrichmentConfig) -> dict:
        """Execute enrichment on a mapping file."""
        results = {
            'mapping_file': mapping_path,
            'enrichments': [],
            'total_original_rows': 0,
            'total_expanded_rows': 0,
        }

        current_path = mapping_path

        for ds_config in config.data_sources:
            self.enricher.load_reference_data(ds_config)
            stats = self.enricher.enrich_mapping_file(current_path, ds_config)

            results['enrichments'].append({
                'source': ds_config.display_name,
                'columns_added': [f'EXPANDED_{c}' for c in ds_config.detail_columns],
                'original_rows': stats['original_rows'],
                'expanded_rows': stats['expanded_rows'],
                'matched_rows': stats['matched_rows'],
            })

            results['total_original_rows'] = stats['original_rows']
            results['total_expanded_rows'] = stats['expanded_rows']
            current_path = stats['output_path']

        results['output_path'] = current_path
        return results


# ============================================================================
# MCP Tool Registration Functions
# ============================================================================

def register_enrichment_tools(mcp):
    """Register mapping enrichment tools with the MCP server."""

    config_store = EnrichmentConfigStore()

    @mcp.tool()
    def configure_mapping_enrichment(
        project_id: str,
        source_id: str,
        source_type: str,
        source_path: str,
        table_name: str,
        key_column: str,
        detail_columns: str,
        display_name: str = ""
    ) -> str:
        """
        Configure a data source for mapping enrichment.

        This allows you to specify which reference data (like Chart of Accounts)
        should be used to enrich mapping exports with additional detail columns.

        Args:
            project_id: The hierarchy project ID
            source_id: Unique identifier for this data source (e.g., 'coa', 'cost_centers')
            source_type: 'csv' or 'database'
            source_path: File path for CSV, or connection string for database
            table_name: Name of the table (e.g., 'DIM_ACCOUNT')
            key_column: Column used for matching (e.g., 'ACCOUNT_CODE')
            detail_columns: Comma-separated list of columns to include in enrichment
            display_name: Optional friendly name for this data source

        Returns:
            JSON with configuration status

        Example:
            configure_mapping_enrichment(
                project_id="my-project",
                source_id="coa",
                source_type="csv",
                source_path="C:/data/DIM_ACCOUNT.csv",
                table_name="DIM_ACCOUNT",
                key_column="ACCOUNT_CODE",
                detail_columns="ACCOUNT_ID,ACCOUNT_NAME,ACCOUNT_BILLING_CATEGORY_CODE",
                display_name="Chart of Accounts"
            )
        """
        # Get or create config
        config = config_store.get_config(project_id)
        if not config:
            config = EnrichmentConfig(project_id=project_id)

        # Parse detail columns
        columns = [c.strip() for c in detail_columns.split(',') if c.strip()]

        # Create data source config
        ds_config = DataSourceConfig(
            source_id=source_id,
            source_type=source_type,
            source_path=source_path,
            table_name=table_name,
            key_column=key_column,
            detail_columns=columns,
            display_name=display_name or table_name,
        )

        # Update or add data source
        existing_idx = next(
            (i for i, ds in enumerate(config.data_sources) if ds.source_id == source_id),
            None
        )
        if existing_idx is not None:
            config.data_sources[existing_idx] = ds_config
        else:
            config.data_sources.append(ds_config)

        # Update default columns
        config.default_detail_columns[table_name] = columns

        # Save config
        config_store.save_config(config)

        return json.dumps({
            'status': 'configured',
            'project_id': project_id,
            'source_id': source_id,
            'detail_columns': columns,
            'total_sources': len(config.data_sources),
        }, indent=2)

    @mcp.tool()
    def get_enrichment_config(project_id: str) -> str:
        """
        Get the enrichment configuration for a project.

        Args:
            project_id: The hierarchy project ID

        Returns:
            JSON with enrichment configuration including all data sources
        """
        config = config_store.get_config(project_id)
        if not config:
            return json.dumps({
                'status': 'not_found',
                'message': f'No enrichment configuration found for project {project_id}',
            })

        return json.dumps(config.to_dict(), indent=2)

    @mcp.tool()
    def enrich_mapping_file(
        mapping_path: str,
        project_id: str,
        output_path: str = ""
    ) -> str:
        """
        Enrich a mapping CSV file with configured reference data.

        Uses the enrichment configuration for the project to expand mappings
        with detail columns from reference data sources.

        Args:
            mapping_path: Path to the mapping CSV file
            project_id: Project ID with enrichment configuration
            output_path: Optional output path (defaults to /enriched/ subdirectory)

        Returns:
            JSON with enrichment statistics
        """
        config = config_store.get_config(project_id)
        if not config:
            return json.dumps({
                'status': 'error',
                'message': f'No enrichment configuration found for project {project_id}',
            })

        if not config.data_sources:
            return json.dumps({
                'status': 'error',
                'message': 'No data sources configured for enrichment',
            })

        enricher = MappingEnricher(config)
        all_stats = []
        current_path = mapping_path

        for ds_config in config.data_sources:
            try:
                enricher.load_reference_data(ds_config)
                stats = enricher.enrich_mapping_file(
                    current_path,
                    ds_config,
                    output_path if output_path else None
                )
                all_stats.append({
                    'source': ds_config.display_name,
                    'columns': ds_config.detail_columns,
                    **stats
                })
                current_path = stats['output_path']
            except Exception as e:
                all_stats.append({
                    'source': ds_config.display_name,
                    'error': str(e),
                })

        return json.dumps({
            'status': 'completed',
            'enrichments': all_stats,
            'final_output': current_path,
        }, indent=2)

    @mcp.tool()
    def get_available_columns_for_enrichment(
        source_type: str,
        source_path: str,
        table_name: str = ""
    ) -> str:
        """
        Get available columns from a data source for enrichment configuration.

        Use this to see what columns are available before configuring enrichment.

        Args:
            source_type: 'csv' or 'database'
            source_path: File path for CSV, connection string for database
            table_name: Required for database sources

        Returns:
            JSON with list of available columns
        """
        loader = ReferenceDataLoader()

        try:
            if source_type == 'csv':
                with open(source_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    columns = reader.fieldnames or []
                    # Get sample values for first few columns
                    sample_row = next(reader, {})
            else:
                from sqlalchemy import create_engine, inspect
                engine = create_engine(source_path)
                inspector = inspect(engine)
                columns = [col['name'] for col in inspector.get_columns(table_name)]
                sample_row = {}

            return json.dumps({
                'status': 'success',
                'source_type': source_type,
                'columns': columns,
                'column_count': len(columns),
                'sample_values': {k: v for k, v in list(sample_row.items())[:5]} if sample_row else {},
            }, indent=2)
        except Exception as e:
            return json.dumps({
                'status': 'error',
                'message': str(e),
            })

    @mcp.tool()
    def suggest_enrichment_after_hierarchy(
        project_id: str,
        hierarchy_name: str,
        mapping_data_json: str
    ) -> str:
        """
        AI-driven suggestion for post-hierarchy enrichment.

        After creating a hierarchy, call this to get AI recommendations for
        enriching the mapping exports with reference data.

        Args:
            project_id: The hierarchy project ID
            hierarchy_name: Name of the created hierarchy
            mapping_data_json: JSON string of mapping rows

        Returns:
            JSON with enrichment suggestions and questions for the user
        """
        try:
            mapping_data = json.loads(mapping_data_json)
        except json.JSONDecodeError:
            return json.dumps({
                'status': 'error',
                'message': 'Invalid mapping data JSON',
            })

        agent = EnrichmentAgent(config_store)
        detected = agent.detect_source_tables(mapping_data)
        prompt = agent.generate_enrichment_prompt(project_id, hierarchy_name)

        return json.dumps(prompt, indent=2)

    return {
        'configure_mapping_enrichment': configure_mapping_enrichment,
        'get_enrichment_config': get_enrichment_config,
        'enrich_mapping_file': enrich_mapping_file,
        'get_available_columns_for_enrichment': get_available_columns_for_enrichment,
        'suggest_enrichment_after_hierarchy': suggest_enrichment_after_hierarchy,
    }

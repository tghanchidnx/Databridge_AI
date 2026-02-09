# Changelog

All notable changes to DataBridge AI Community Edition will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.39.0] - 2026-02-09

### Added
- Initial public release of DataBridge AI Community Edition
- Plugin-based architecture for extensibility
- Core data reconciliation tools:
  - `load_csv` - Load and preview CSV files
  - `load_json` - Load and preview JSON files
  - `query_database` - Execute SQL queries
  - `profile_data` - Generate data statistics
  - `compare_hashes` - Hash-based row comparison
  - `fuzzy_match_columns` - RapidFuzz-based matching
  - `extract_text_from_pdf` - PDF text extraction
  - `diff_text` - Text comparison
  - `find_files` - File discovery
  - `get_license_status` - License tier information
- Web dashboard UI on port 5050
- MIT License for open-source use

### Features by Tier
- **Community (Free)**: Core reconciliation, fuzzy matching, PDF extraction
- **Pro (Licensed)**: Cortex AI, Wright Pipeline, GraphRAG, Data Catalog
- **Enterprise (Custom)**: Custom agents, white-labeling, SLA support

## [Unreleased]

### Planned
- Additional file format support (Excel, Parquet)
- Enhanced fuzzy matching algorithms
- Batch processing capabilities
- REST API endpoint

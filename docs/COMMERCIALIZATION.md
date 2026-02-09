# DataBridge AI Commercialization Structure

## Overview

DataBridge AI uses a tiered product structure with an open-source base (Community Edition) and licensed premium components (Pro/Enterprise).

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                              DataBridge AI Product Tiers                              │
├──────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│  ┌────────────────┐  ┌────────────────┐  ┌──────────────────┐  ┌────────────────┐   │
│  │ COMMUNITY (CE) │  │  PRO (Licensed)│  │  PRO EXAMPLES    │  │   ENTERPRISE   │   │
│  │   Free         │  │ GitHub Packages│  │  Licensed Add-on │  │    Custom      │   │
│  │   Public PyPI  │  │  License Key   │  │  GitHub Packages │  │  Dedicated     │   │
│  ├────────────────┤  ├────────────────┤  ├──────────────────┤  ├────────────────┤   │
│  │ • Data Recon.  │  │ Everything CE  │  │ 47 test files    │  │ Everything Pro │   │
│  │ • Fuzzy Match  │  │ + Cortex AI    │  │ 19 use-case      │  │ + Custom agents│   │
│  │ • PDF/OCR      │  │ + Wright       │  │   tutorials      │  │ + White-label  │   │
│  │ • Data Profile │  │ + GraphRAG     │  │ • Beginner (4)   │  │ + SLA support  │   │
│  │ • dbt Basic    │  │ + Observability│  │ • Financial (7)  │  │ + On-premise   │   │
│  │ • Data Quality │  │ + Full Catalog │  │ • Faux Objects(8)│  │ + Training     │   │
│  │ • UI Dashboard │  │ + Lineage      │  │ • CE tests       │  │                │   │
│  │                │  │ + Orchestrator │  │ • Pro tests      │  │                │   │
│  │ ~106 tools     │  │ ~277 tools     │  │ Requires Pro key │  │ 341+ tools     │   │
│  └────────────────┘  └────────────────┘  └──────────────────┘  └────────────────┘   │
│                                                                                      │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

## Directory Structure

```
Databridge_AI/                    # PRIVATE - Main development repo
├── src/                          # Full 341-tool implementation
│   ├── plugins/                  # License management system
│   │   ├── __init__.py          # LicenseManager class
│   │   └── registry.py          # Plugin discovery
│   ├── server.py                # Tier-aware tool registration
│   └── [22 modules]             # All tool modules
├── databridge-ce/               # PUBLIC - Community Edition
│   ├── src/
│   │   ├── server.py           # CE-specific server
│   │   └── config.py           # CE configuration
│   ├── plugins/                 # CE plugins
│   ├── ui/                      # Dashboard
│   ├── pyproject.toml          # PyPI: databridge-ai
│   └── LICENSE                  # MIT
├── databridge-pro/              # PRIVATE - Pro Edition package
│   ├── src/
│   │   ├── __init__.py         # Pro plugin registration
│   │   ├── cortex/             # Cortex AI tools
│   │   ├── wright/             # Wright Pipeline
│   │   ├── graphrag/           # GraphRAG Engine
│   │   └── [other pro modules]
│   ├── pyproject.toml          # GitHub Packages: databridge-ai-pro
│   └── LICENSE                  # Proprietary
├── databridge-ai-examples/      # PRIVATE - Pro Examples package
│   ├── src/
│   │   ├── __init__.py         # Examples registration
│   │   ├── use_cases/          # 19 tutorial use cases
│   │   └── tests/              # CE & Pro test suites
│   │       ├── ce/             # CE module tests
│   │       ├── pro/            # Pro module tests
│   │       └── conftest.py     # Shared fixtures
│   ├── pyproject.toml          # GitHub Packages: databridge-ai-examples
│   └── README.md
└── scripts/
    └── generate_license.py      # License key generator
```

## License Key System

### Format
```
DB-{TIER}-{CUSTOMER_ID}-{EXPIRY}-{SIGNATURE}

Examples:
- DB-CE-FREE0001-20990101-000000000000    (CE - perpetual)
- DB-PRO-ACME0001-20270209-a1b2c3d4e5f6   (Pro - 1 year)
- DB-ENTERPRISE-BIGCORP-20280101-xyz123   (Enterprise - custom)
```

### Validation
- Offline hash-based validation (no server required)
- SHA256 signature verification
- Expiry date checking
- Tier-based feature gating

### Generation
```bash
python scripts/generate_license.py PRO ACME001 365
# Output: DB-PRO-ACME001-20270209-a1b2c3d4e5f6
```

## Distribution

### Community Edition (Public)
```bash
# Install from PyPI
pip install databridge-ai

# With optional dependencies
pip install databridge-ai[pdf,ocr,dbt,all]
```

### Pro Edition (Private)
```bash
# Set license key
export DATABRIDGE_LICENSE_KEY="DB-PRO-..."

# Configure private index
pip config set global.extra-index-url https://pypi.databridge.ai/simple/

# Install
pip install databridge-ai-pro
```

### Pro Examples (Tests & Tutorials)
```bash
# Set license key
export DATABRIDGE_LICENSE_KEY="DB-PRO-..."

# Install CE tests + beginner use cases
pip install databridge-ai-examples

# Install with Pro tests + advanced use cases
pip install databridge-ai-examples[pro]
```

### Team Development (Full)
```bash
# Clone private repo
git clone https://github.com/tghanchidnx/Databridge_AI.git

# Install in development mode
pip install -e .
```

## Module Classification

### Community Edition (~106 tools)
| Phase | Module | Tools | Status |
|-------|--------|-------|--------|
| 0 | File Discovery | 3 | ✅ CE |
| 1 | Data Loading | 3 | ✅ CE |
| 2 | Data Profiling | 2 | ✅ CE |
| 3 | Hashing/Comparison | 3 | ✅ CE |
| 4 | Fuzzy Matching | 2 | ✅ CE |
| 5 | PDF/OCR | 3 | ✅ CE |
| 6 | Workflow | 4 | ✅ CE |
| 7 | Transform | 2 | ✅ CE |
| 8 | Documentation | 1 | ✅ CE |
| 13 | Templates (basic) | 10 | ✅ CE |
| 16 | Diff Utilities | 6 | ✅ CE |
| 24 | dbt Integration | 8 | ✅ CE |
| 25 | Data Quality | 7 | ✅ CE |

### Pro Edition (~171 additional tools)
| Phase | Module | Tools | Status |
|-------|--------|-------|--------|
| 9 | Hierarchy Builder | 44 | 🔒 Pro |
| 10 | Connections | 16 | 🔒 Pro |
| 11 | Schema Matcher | 5 | 🔒 Pro |
| 12 | Data Matcher | 4 | 🔒 Pro |
| 14 | Orchestrator | 16 | 🔒 Pro |
| 18 | Faux Objects | 18 | 🔒 Pro |
| 19-20 | Cortex AI | 26 | 🔒 Pro |
| 23 | Console Dashboard | 5 | 🔒 Pro |
| 26 | Wright Pipeline | 29 | 🔒 Pro |
| 27 | Lineage | 11 | 🔒 Pro |
| 28 | Git/CI-CD | 12 | 🔒 Pro |
| 29 | Data Catalog | 19 | 🔒 Pro |
| 30 | Versioning | 12 | 🔒 Pro |
| 31 | GraphRAG | 10 | 🔒 Pro |
| 32 | Observability | 15 | 🔒 Pro |

### Pro Examples (Tests & Tutorials)
| Category | Contents | Count |
|----------|----------|-------|
| Beginner Use Cases | 01-04: Pizza, friends, school, sports | 4 cases |
| Financial Use Cases | 05-11: SEC EDGAR, Apple, Microsoft | 7 cases |
| Faux Objects Use Cases | 12-19: Domain persona tutorials | 8 cases |
| CE Test Suite | Data loading, hashing, fuzzy, dbt, quality, diff | ~12 files |
| Pro Test Suite | Hierarchy, cortex, catalog, versioning, wright | ~15 files |
| Shared Fixtures | conftest.py, sample data | 2 files |

## GitHub Actions Workflows

### CE: Publish to PyPI
```yaml
# .github/workflows/publish-pypi.yml
on:
  release:
    types: [published]
jobs:
  publish:
    - uses: pypa/gh-action-pypi-publish@release/v1
```

### Pro: Publish to GitHub Packages
```yaml
# .github/workflows/publish-pro.yml
on:
  release:
    types: [published]
jobs:
  publish:
    - run: twine upload --repository-url https://upload.pypi.org/legacy/ dist/*
```

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `DATABRIDGE_LICENSE_KEY` | License key for Pro features | For Pro/Enterprise |
| `DATABRIDGE_LICENSE_SECRET` | Secret for license generation | Admin only |
| `DATABRIDGE_DATABASE_URL` | SQLAlchemy connection string | Optional |
| `DATABRIDGE_FUZZY_THRESHOLD` | Default fuzzy match threshold | Optional (default: 80) |

## API Reference

### LicenseManager
```python
from src.plugins import get_license_manager

mgr = get_license_manager()
print(mgr.tier)              # 'CE', 'PRO', or 'ENTERPRISE'
print(mgr.is_pro())          # True if Pro or higher
print(mgr.get_status())      # Full status dict
```

### Pro Registration
```python
from databridge_ai_pro import register_pro_tools, validate_license

if validate_license():
    register_pro_tools(mcp)
```

## Security Considerations

1. **License Secret**: Keep `DATABRIDGE_LICENSE_SECRET` private
2. **Key Distribution**: Distribute keys securely to customers
3. **Repo Access**: Keep `Databridge_AI` and `databridge-pro` private
4. **PyPI Tokens**: Use trusted publishing (OIDC) where possible

## Support

- **Community**: GitHub Issues, Community Forums
- **Pro**: Email support (support@databridge.ai)
- **Enterprise**: Priority support with SLA, dedicated account manager

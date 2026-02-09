# DataBridge AI Examples & Tests

*Use-case tutorials and test suites for DataBridge AI. Part of the **Pro Examples** sub-tier.*

---

## Installation

```bash
# CE tests + beginner use cases (requires databridge-ai base)
pip install databridge-ai-examples

# Include Pro tests + advanced use cases (requires Pro license)
pip install databridge-ai-examples[pro]
```

## Requirements

- Python 3.10+
- `databridge-ai >= 0.40.0` (Community Edition base)
- A valid Pro license key for full access (`DATABRIDGE_LICENSE_KEY` env var)
- Optional: `databridge-ai-pro >= 0.40.0` for Pro test suite

### License Key Configuration

```bash
# Set your license key
export DATABRIDGE_LICENSE_KEY="DB-PRO-YOURKEY-20260101-signature"
```

**License Key Format:** `DB-{TIER}-{CUSTOMER_ID}-{EXPIRY}-{SIGNATURE}`

Contact sales@databridge.ai for pricing and license keys.

## What's Included

### Use-Case Tutorials (19 total)

| Category | Cases | Description |
|----------|-------|-------------|
| **Beginner** | 01-04 | Pizza shop sales, friend matching, school hierarchies, sports comparison |
| **Financial** | 05-11 | SEC EDGAR analysis, Apple/Microsoft financials, balance sheets, full pipelines |
| **Faux Objects** | 12-19 | Domain persona tutorials (financial, oil & gas, manufacturing, SaaS, etc.) |

### Test Suites (47 files)

| Suite | Module | Tests |
|-------|--------|-------|
| `tests/ce/` | Community Edition | Data loading, hashing, fuzzy matching, dbt, data quality, diff utilities |
| `tests/pro/` | Pro Edition | Hierarchy, cortex, catalog, versioning, wright, lineage, observability |
| `tests/conftest.py` | Shared fixtures | Common fixtures and sample data helpers |

## Running Tests

```bash
# Run CE tests only
pytest src/tests/ce/

# Run Pro tests (requires databridge-ai-pro)
pytest src/tests/pro/

# Run all tests with coverage
pytest --cov=databridge_ai_examples src/tests/
```

## Editions

This package is part of the DataBridge AI product family:

| Package | Distribution | License |
|---------|-------------|---------|
| `databridge-ai` | PyPI (public) | MIT (Free) |
| `databridge-ai-pro` | GitHub Packages | Proprietary (License Key) |
| `databridge-ai-examples` | GitHub Packages | Proprietary (Requires Pro Key) |

## License

Proprietary - requires a valid DataBridge AI Pro or Enterprise license key. See [LICENSE](LICENSE) for full terms.

Copyright (c) 2024-2026 DataBridge AI. All Rights Reserved.

## Support

- Sales: sales@databridge.ai
- Support: support@databridge.ai
- Documentation: [GitHub Wiki](https://github.com/tghanchidnx/Databridge_AI/wiki)

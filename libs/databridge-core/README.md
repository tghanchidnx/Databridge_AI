# DataBridge Core

Shared utilities and base classes for DataBridge AI applications.

## Installation

```bash
pip install -e .
```

## Contents

- **Config** - Base settings classes using Pydantic Settings
- **Database** - Database connection management
- **CLI** - Rich console utilities and formatters
- **Audit** - Audit logging utilities
- **MCP** - Model Context Protocol utilities

## Usage

```python
from databridge_core import console, DatabaseManager, AuditLogger
from databridge_core.config import BaseAppSettings
```

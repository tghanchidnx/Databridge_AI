# DataBridge Models

Shared data models and enums for DataBridge AI applications.

## Installation

```bash
pip install -e .
```

## Contents

- `Base` - SQLAlchemy declarative base with common mixins
- `SQLDialect` - Supported SQL dialects enum
- `TableStatus` - Table status enum
- `FormulaType` - Formula type enum
- `AggregationType` - Aggregation type enum
- `JoinType` - Join type enum

## Usage

```python
from databridge_models import Base, SQLDialect, TableStatus
from databridge_models.enums import FormulaType, AggregationType
```

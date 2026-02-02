# Gemini Rename Fixes - Technical Details

## Status: 1 Fix Remaining

---

## Fix #1: Librarian Test Version Assertion

### Problem
`apps/databridge-librarian/tests/unit/core/test_config.py` line 21:
```python
assert settings.version == "1.0.0"
```

But `apps/databridge-librarian/src/core/config.py` has:
```python
version: str = Field(
    default="3.0.0",
    description="Application version",
)
```

### Fix Required
Change line 21 from:
```python
assert settings.version == "1.0.0"
```
To:
```python
assert settings.version == "3.0.0"
```

---

## Previously Fixed (No Action Needed)

### Researcher Integration Module
The `librarian_client.py` now has correct class names:
- `LibrarianConnectionMode` (was V3ConnectionMode)
- `LibrarianProject` (was V3Project)
- `LibrarianHierarchy` (was V3Hierarchy)
- `LibrarianMapping` (was V3Mapping)
- `LibrarianClientResult` (was V3ClientResult)
- `LibrarianHierarchyClient` (was V3HierarchyClient)

### Librarian Test app_name
Test now correctly expects:
```python
assert settings.app_name == "DataBridge AI Librarian - Hierarchy Builder"
```

---

## Optional Cleanup (Low Priority)

### V3/V4 References in Docstrings
Some files still have "V3" or "V4" in comments/docstrings:
- ~20 files in `apps/databridge-librarian/src/`
- 2 files in `apps/databridge-researcher/src/`

### Deprecation Warnings
- Replace `datetime.utcnow()` with `datetime.now(datetime.UTC)`
- Replace Pydantic class-based config with `ConfigDict`

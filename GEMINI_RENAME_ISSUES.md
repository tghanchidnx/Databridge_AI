# Gemini Rename Issues - Complete Audit

## Summary

**Rename Status: INCOMPLETE**

The folder/file names were renamed but V3/V4 references remain in file contents.

---

## Test Results

| Test Suite | Passed | Failed | Status |
|------------|--------|--------|--------|
| Librarian | 336 | 1 | Version mismatch in test |
| Researcher | 473 | 0 | PASS |
| Root Tests | 67 | 0 | PASS |
| Discovery | 357 | 0 | PASS |

**Total: 1,233 tests, 1 failure**

---

## Files Still Containing V3/V4 References

### Source Files (5 files)
| File | Issue |
|------|-------|
| `apps/databridge-librarian/PLAN.md` | Contains V3 paths and references |
| `apps/databridge-librarian/requirements-dev.txt` | Header comment says "V3" |
| `apps/databridge-researcher/DIMENSION_MODELING_ENHANCEMENTS.md` | Multiple V3/V4 references |
| `apps/databridge-researcher/requirements-dev.txt` | Header comment says "V4" |
| `apps/databridge-researcher/skills/variance-analyst-prompt.txt` | References V3 |

### Configuration Files (6 files)
| File | Issue |
|------|-------|
| `apps/databridge-librarian/.dockerignore` | Comment says "V3" |
| `apps/databridge-librarian/.env.example` | Header says "V3" |
| `apps/databridge-librarian/Dockerfile` | Labels say "V3" |
| `apps/databridge-researcher/.dockerignore` | Comment says "V4" |
| `apps/databridge-researcher/.env.example` | Header says "V4", V3 integration settings |
| `apps/databridge-researcher/Dockerfile` | Labels say "V4" |

### Documentation Files (14 files)
| File | Issue |
|------|-------|
| `docs/API_REFERENCE.md` | V3/V4 references |
| `docs/DEPLOYMENT_GUIDE.md` | Multiple V3/V4 references |
| `docs/FPA_SQL_DEEP_ANALYSIS.md` | V3/V4 references |
| `docs/PITCH_DECK_REASONING.md` | V3/V4 references |
| `docs/SECURITY_REPORT.md` | V3/V4 references |
| `Gemini/ai_agent_strategy.md` | V3/V4 references |
| `Gemini/chronosphere_context_plan.md` | V3/V4 references |
| `Gemini/comprehensive_deployment_plan.md` | V3/V4 references |
| `Gemini/e2e_test/MANUAL_TEST_GUIDE.md` | V3/V4 references |
| `Gemini/gemini_enhancement_plan.md` | V3/V4 references |
| `Gemini/model_discovery_engine_plan.md` | V3/V4 references |
| `Gemini/prompt_to_claude.md` | V3/V4 references |
| `Gemini/sql_analysis_and_recommendations.md` | V3/V4 references |
| `Gemini/suggestions.md` | V3/V4 references |

### Auto-Generated Files (can be deleted/regenerated)
- `*.egg-info/` directories
- `__pycache__/` directories
- `.pytest_cache/` directories
- `.coverage` files

---

## Critical Fix Required (1)

**Test Assertion**: `apps/databridge-librarian/tests/unit/core/test_config.py:21`
- Expects version `"1.0.0"` but config has `"3.0.0"`

---

## Recommendation

1. **Fix the failing test** (1 line change)
2. **Update source/config files** (11 files) - Replace V3→Librarian, V4→Researcher
3. **Update documentation** (14 files) - Replace V3→Librarian, V4→Researcher
4. **Clean auto-generated files** - Delete and regenerate

---

## Naming Convention

| Old | New |
|-----|-----|
| V3 | Librarian |
| V4 | Researcher |
| DataBridge AI V3 | DataBridge AI Librarian |
| DataBridge AI V4 | DataBridge AI Researcher |
| databridge-v3 | databridge-librarian |
| databridge-v4 | databridge-researcher |

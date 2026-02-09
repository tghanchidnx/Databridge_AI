# DataBridge AI PyPI Publishing Plan

## Overview

This document outlines the step-by-step plan to publish DataBridge AI packages to PyPI and GitHub Packages based on the tiered licensing structure.

---

## Phase 1: Pre-Publishing Preparation

### 1.1 Verify Package Structure

```bash
# Verify CE package builds correctly
cd databridge-ce
pip install build
python -m build
pip install dist/*.whl --force-reinstall
python -c "from src.server import mcp; print('CE package OK')"
```

### 1.2 Update Package Metadata

**databridge-ce/pyproject.toml:**
- [ ] Update author name and email
- [ ] Update repository URLs to public repo
- [ ] Verify version is 0.39.0
- [ ] Verify all classifiers are correct

**databridge-pro/pyproject.toml:**
- [ ] Update author name and email
- [ ] Verify dependency on `databridge-ai>=0.39.0`
- [ ] Verify version matches CE version

### 1.3 Create Public Repository

```bash
# Create new public repo for CE
gh repo create tghanchidnx/databridge-ai --public --description "DataBridge AI Community Edition - Open-source data reconciliation engine"

# Clone and set up
git clone https://github.com/tghanchidnx/databridge-ai.git
cd databridge-ai

# Copy CE files
cp -r ../Databridge_AI/databridge-ce/* .

# Initialize
git add .
git commit -m "Initial release of DataBridge AI Community Edition v0.39.0"
git push
```

---

## Phase 2: PyPI Account Setup

### 2.1 Create/Verify PyPI Account

1. Go to https://pypi.org/account/register/
2. Verify email address
3. Enable 2FA (required for publishing)

### 2.2 Create API Token

1. Go to https://pypi.org/manage/account/token/
2. Create token with scope "Entire account" (for first publish) or project-specific
3. Save token securely

### 2.3 Configure Trusted Publishing (Recommended)

1. Go to https://pypi.org/manage/account/publishing/
2. Add new pending publisher:
   - PyPI Project Name: `databridge-ai`
   - Owner: `tghanchidnx`
   - Repository: `databridge-ai`
   - Workflow: `publish-pypi.yml`
   - Environment: `pypi`

---

## Phase 3: TestPyPI Validation

### 3.1 Publish to TestPyPI First

```bash
cd databridge-ce

# Build
python -m build

# Upload to TestPyPI
pip install twine
twine upload --repository testpypi dist/*
# Enter __token__ as username, paste API token as password
```

### 3.2 Test Installation from TestPyPI

```bash
# Create fresh virtual environment
python -m venv test_env
source test_env/bin/activate  # or test_env\Scripts\activate on Windows

# Install from TestPyPI
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ databridge-ai

# Verify
python -c "from src.server import mcp; print('Success!')"
```

### 3.3 Verify Package Contents

```bash
# Check package info
pip show databridge-ai

# List installed files
pip show -f databridge-ai
```

---

## Phase 4: Production PyPI Publishing

### 4.1 Manual Publishing (First Time)

```bash
cd databridge-ce

# Clean previous builds
rm -rf dist/ build/ *.egg-info

# Build fresh
python -m build

# Check package
twine check dist/*

# Upload to PyPI
twine upload dist/*
# Enter __token__ as username, paste API token as password
```

### 4.2 GitHub Actions Publishing (Subsequent Releases)

**Option A: Using Trusted Publishing (Recommended)**

```yaml
# .github/workflows/publish-pypi.yml
- name: Publish to PyPI
  uses: pypa/gh-action-pypi-publish@release/v1
  # No password needed with trusted publishing
```

**Option B: Using API Token**

1. Add `PYPI_API_TOKEN` secret to GitHub repository
2. Workflow uses:
```yaml
- name: Publish to PyPI
  uses: pypa/gh-action-pypi-publish@release/v1
  with:
    password: ${{ secrets.PYPI_API_TOKEN }}
```

### 4.3 Create GitHub Release

```bash
# Tag the release
git tag -a v0.39.0 -m "DataBridge AI Community Edition v0.39.0"
git push origin v0.39.0

# Create release via GitHub UI or CLI
gh release create v0.39.0 --title "DataBridge AI CE v0.39.0" --notes "Initial public release of DataBridge AI Community Edition"
```

---

## Phase 5: GitHub Packages for Pro Edition

### 5.1 Configure GitHub Packages

```bash
# Authenticate with GitHub Packages
pip config set global.extra-index-url https://ghcr.io/v2/tghanchidnx

# Or use environment variable
export PIP_EXTRA_INDEX_URL=https://ghcr.io/v2/tghanchidnx
```

### 5.2 Publish Pro Package

```bash
cd databridge-pro

# Build
python -m build

# Upload to GitHub Packages
twine upload --repository-url https://upload.pypi.org/legacy/ dist/*
```

### 5.3 Verify Pro Installation

```bash
# Set license key
export DATABRIDGE_LICENSE_KEY="DB-PRO-TEST0001-20270209-xxxxxx"

# Install
pip install databridge-ai-pro --extra-index-url https://ghcr.io/v2/tghanchidnx

# Verify
python -c "from databridge_ai_pro import get_pro_status; print(get_pro_status())"
```

---

## Phase 6: Post-Publishing Tasks

### 6.1 Verify Public Installation

```bash
# Test fresh install
pip install databridge-ai

# Verify tools
python -c "
from src.server import mcp
status = mcp.get_license_status() if hasattr(mcp, 'get_license_status') else 'OK'
print(f'DataBridge AI installed successfully')
"
```

### 6.2 Update Documentation

- [ ] Update README badges with PyPI version
- [ ] Add installation instructions to wiki
- [ ] Update CLAUDE.md and GEMINI.md with PyPI info

### 6.3 Announce Release

- [ ] GitHub release notes
- [ ] Update project website (if applicable)
- [ ] Social media announcement (if applicable)

---

## Checklist Summary

### Pre-Publishing
- [ ] Package builds successfully
- [ ] All tests pass
- [ ] License files in place
- [ ] README.md complete
- [ ] CHANGELOG.md updated
- [ ] Version numbers consistent

### PyPI Setup
- [ ] PyPI account created
- [ ] 2FA enabled
- [ ] API token generated
- [ ] Trusted publishing configured (optional)

### Publishing
- [ ] TestPyPI upload successful
- [ ] TestPyPI installation verified
- [ ] Production PyPI upload successful
- [ ] Installation from PyPI verified
- [ ] GitHub release created

### Pro Edition
- [ ] GitHub Packages configured
- [ ] Pro package published
- [ ] License validation tested
- [ ] Pro installation verified

---

## Quick Reference Commands

```bash
# Build package
python -m build

# Check package
twine check dist/*

# Upload to TestPyPI
twine upload --repository testpypi dist/*

# Upload to PyPI
twine upload dist/*

# Install from PyPI
pip install databridge-ai

# Install with extras
pip install databridge-ai[pdf,ocr,dbt,all]

# Generate license
python scripts/generate_license.py PRO CUSTOMER 365

# Verify license
python scripts/generate_license.py --validate DB-PRO-xxx
```

---

## Troubleshooting

### "File already exists" Error
- Increment version number in pyproject.toml
- Or use `--skip-existing` flag

### Authentication Failed
- Verify API token is correct
- Use `__token__` as username
- Check 2FA is enabled on PyPI account

### Package Not Found After Publishing
- Wait 5-10 minutes for PyPI index to update
- Check package name spelling
- Verify package was uploaded successfully

### Import Errors After Installation
- Check `packages` in pyproject.toml `[tool.hatch.build.targets.wheel]`
- Verify `__init__.py` files exist
- Check for missing dependencies

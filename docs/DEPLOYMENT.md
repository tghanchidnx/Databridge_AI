# DataBridge AI - Deployment Workflow

## Branch Strategy

```
dev ──→ test ──→ master
 │        │         │
 │        │         └── Production: stable releases, PyPI publishes
 │        └── Staging: integration testing, pre-release validation
 └── Development: active feature work, experimental changes
```

## Workflow

### 1. Feature Development (on `dev`)

```bash
# Switch to dev branch
git checkout dev

# Make changes, commit
git add <files>
git commit -m "feat: Add new feature"

# Push to remote
git push origin dev
```

CI runs automatically on push to `dev`.

### 2. Promote dev → test

**Option A: GitHub Actions (recommended)**
```bash
gh workflow run promote.yml -f source=dev -f target=test
```

**Option B: Manual PR**
```bash
gh pr create --base test --head dev --title "Promote dev → test"
```

**Option C: Direct merge (fast)**
```bash
git checkout test
git merge dev
git push origin test
```

### 3. Promote test → master

**Option A: GitHub Actions (recommended)**
```bash
gh workflow run promote.yml -f source=test -f target=master
```

**Option B: Manual PR**
```bash
gh pr create --base master --head test --title "Promote test → master"
```

### 4. Hotfix (urgent production fix)

```bash
# Branch from master
git checkout master
git checkout -b hotfix/fix-description

# Fix, commit, push
git add <files>
git commit -m "fix: Critical fix description"
git push origin hotfix/fix-description

# PR to master
gh pr create --base master --title "hotfix: Fix description"

# After merge, backport to dev and test
git checkout dev && git merge master && git push origin dev
git checkout test && git merge master && git push origin test
```

## CI/CD Pipelines

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| `ci.yml` | Push/PR to dev, test, master | Lint, test, security scan |
| `promote.yml` | Manual dispatch | Create promotion PR with validation |
| `release.yml` | Tag on master | Build and publish releases |
| `publish-pypi.yml` | Release published | Publish to PyPI |

## Quick Reference

```bash
# Check current branch
git branch

# Sync dev with latest master (start of new work)
git checkout dev
git merge master
git push origin dev

# See what's in dev but not in test
git log --oneline test..dev

# See what's in test but not in master
git log --oneline master..test

# Trigger promotion
gh workflow run promote.yml -f source=dev -f target=test
gh workflow run promote.yml -f source=test -f target=master
```

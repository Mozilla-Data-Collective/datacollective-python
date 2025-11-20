## Release Workflow

This repository uses GitHub Actions and branch-specific workflows for
publishing releases.

### Branches

- `main` \- primary development branch; merging to `main` triggers
  automated version bumping.
- `test-pypi` \- deploying releases to TestPyPI.
- `pypi` \- deploying releases to the production PyPI index.

### Automated steps

1. **Prepare release on `main`**

   When a pull request is merged into `main`, a workflow runs:

   ```bash
   uv run python scripts/dev.py prepare-release
   ```

   This command:
   - Runs the full check suite.
   - Bumps the version.
   - Pushes the updated commit and tag back to `main`.

2. **Deploy to TestPyPI**

   Merging the updated `main` into `test-pypi` runs:

   ```bash
   uv run python scripts/dev.py publish-test
   ```

   This builds and publishes the new version to TestPyPI.

3. **Deploy to PyPI**

   After validating the package from TestPyPI, merge the same `main`
   commit into `pypi` to run:

   ```bash
   uv run python scripts/dev.py publish
   ```

   This publishes the package to the production PyPI index.

### Recommended local workflow

Before opening release-related pull requests:

1. Run the full checks without modifying files:

   ```bash
   uv run python scripts/dev.py all
   ```

2. Optionally preview the version bump locally:

   ```bash
   uv run python scripts/dev.py prepare-release
   ```

   The GitHub Actions workflow runs the same command when the PR
   is merged into `main`.

3. After the automated version bump lands on `main`, open PRs from:
   - `main` to `test-pypi` to deploy to TestPyPI.
   - `main` to `pypi` to deploy to PyPI (once validated).

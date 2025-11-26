## Release Workflow

This repository uses GitHub Actions and branch-specific workflows for
publishing releases.

### Branches

- `main` - primary development branch. When a pull request is merged into `main` the repository workflow:
  - Runs the full check suite.
  - Bumps the version.
  - Opens a `release/vX.Y.Z` pull request back onto `main`. Auto-merge is enabled on that PR, so once required checks pass the version commit lands on `main` automatically.
- `test-pypi` - receives releases from `main` to deploy to TestPyPI.
- `pypi` - receives releases from `main` to deploy to the production PyPI index.

### Automated steps

1. **Prepare release on `main`**

   When a pull request is merged into `main`, the release workflow runs the full checks, performs the version bump, and opens the `release/vX.Y.Z` pull request onto `main`. That PR is configured to auto-merge once required checks complete, so the version commit is applied to `main` without manual intervention.

2. **Deploy to TestPyPI**

   Merge the updated `main` into `test-pypi` to deploy that version to TestPyPI. The following command runs automatically in the workflow:

   ```bash
   uv run python scripts/dev.py publish-test
   ```

3. **Deploy to PyPI**

   After validating the package on TestPyPI, merge `main` into `pypi` to deploy to production. The following command runs automatically in the workflow:

   ```bash
   uv run python scripts/dev.py publish
   ```

### Recommended local workflow

Before opening release-related pull requests:

1. Run the full checks without modifying files:

   ```bash
   uv run python scripts/dev.py all
   ```

2. Optionally rehearse the version bump locally:

   ```bash
   uv run python scripts/dev.py prepare-release
   ```

   The repository workflow performs the same steps when `main` changes.

3. Follow the branch merge order so TestPyPI receives the version before production:

   - `main` -> `test-pypi`
   - `main` -> `pypi`

### Required GitHub Actions secrets

- `TEST_PYPI_API_TOKEN` - token for publishing to TestPyPI (username `__token__`).
- `PYPI_API_TOKEN` - token for publishing to PyPI (username `__token__`).

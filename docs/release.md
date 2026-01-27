## Release Workflow

This project uses:

- `pyproject.toml` as the single source of truth for the version.
- `bump-my-version` to bump the version and create Git tags.
- GitHub Releases to trigger automatic publishing to PyPI via GitHub Actions.

Publishing to PyPI happens **only** when a GitHub Release is created for a version tag (for example, `v0.1.0`).

---

## Prerequisites

Before doing a release, make sure you have:

- Push access to the main repository.
- A local Python environment with `bump-my-version` installed:

```bash
uv tool install bump-my-version
```

- An up-to-date local clone:

```bash
git checkout main
git pull origin main
```

---

## 1. Prepare the code for release

1. Make sure all desired changes are merged into `main`.
2. Run tests and checks locally (adjust commands to your setup):

```bash
pre-commit run --all-files
pytest -q
```

---

## 2. Choose the version bump

Decide what type of version bump you need according to [Semantic Versioning](https://semver.org/):

- `patch`: bug fixes only (e.g. `0.0.34` → `0.0.35`)
- `minor`: new features, backwards compatible (e.g. `0.0.34` → `0.1.0`)
- `major`: breaking changes (e.g. `0.0.34` → `1.0.0`)

Check the potential versioning paths with:

```bash
bump-my-version show-bump
```

And verify that the version you plan to release is the expected one:

```bash
bump-my-version show --increment minor new_version
```

Finally, you can run a dry run to see what will happen:

```bash
bump-my-version bump minor --dry-run -vv
```

---

## 3. Bump the version using bump-my-version

Run **one** of the following from the repository root:

```bash
# choose exactly one:
bump-my-version bump patch
# or
bump-my-version bump minor
# or
bump-my-version bump major
```

What this does:

- Updates the version in `pyproject.toml`.
- Commits the version bump.
- Creates a Git tag `vX.Y.Z` matching the new version.


---

## 4. Push the changes and tag

Push the new commit and the created tag to GitHub:

```bash
git push origin main
git push origin vX.Y.Z
```

Replace `vX.Y.Z` with the tag created in the previous step (for example, `v0.0.35`).

---

## 5. Create a GitHub Release

1. Go to the repository page on GitHub.
2. Open the `Releases` section.
3. Click `Draft a new release`.
4. Select the tag you just pushed (for example, `v0.0.35`).
5. Set the release title to the same value (e.g. `v0.0.35`).
6. Optionally add release notes (high-level changes).
7. Click `Publish release`.

This will trigger the `Publish to PyPI` GitHub Actions workflow.

---

## 6. Automatic publish to PyPI

Once the GitHub Release is published:

- GitHub Actions automatically:
  - Checks out the code.
  - Builds the distribution.
  - Uploads the package to PyPI using the `PYPI_API_TOKEN` secret.

You can monitor the progress:

1. Open the `Actions` tab in GitHub.  
2. Open the `Publish to PyPI` workflow run associated with your release.  
3. Wait for it to complete successfully.

---

## 7. Verify the release on PyPI

After the workflow succeeds:

1. Go to the project page on PyPI.
2. Confirm that the new version `X.Y.Z` is listed.
3. Optionally install the package in a clean environment to verify:

```bash
pip install --upgrade datacollective==X.Y.Z
```

---

## Notes and Best Practices

- **Single source of truth**: The version is defined only in `pyproject.toml`. Do **not** manually edit `__version__` or other files for versioning.

- **Tag format**: Always use `vX.Y.Z` tags (for example, `v0.0.35`). The `bump-my-version` configuration enforces this.

- **Manual workflow dispatch (advanced)**: In rare cases, you can re-run the publish job from the `Actions` tab using `workflow_dispatch`, but normally you should always go through a GitHub Release.
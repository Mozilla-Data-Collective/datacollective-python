# Release Workflow

This project uses:

- `bump-my-version` as the single source of truth for the version and updating it.
- GitHub Releases to trigger automatic publishing to PyPI via GitHub Actions.

Publishing to PyPI happens **only** when a GitHub Release is created for a version tag (for example, `0.1.0`).

---

## Prerequisites

Before doing a release, make sure you have:

- Push access to the repository.
- A local Python environment with `bump-my-version` installed:

```bash
uv tool install bump-my-version
```

- `uv` available on your `PATH`: the version bump runs `uv lock` for you (see step 4).

- An up-to-date local clone with all changes merged into `main`:

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

Create a release branch from the up-to-date `main`, named after the version you are about to release:

```bash
git switch -c release-X.Y.Z
```

Then run **one** of the following from the repository root:

```bash
# choose exactly one:
bump-my-version bump patch -vv
# or
bump-my-version bump minor -vv
# or
bump-my-version bump major -vv
```

This will update the version in `pyproject.toml` and `src/datacollective/__init__.py`, and then run `uv lock` to update `uv.lock`.

---

## 4. Check uv.lock

`bump-my-version` runs `uv lock` automatically after bumping (see `pre_commit_hooks` in `pyproject.toml`), so `uv.lock` should already carry the new version. CI installs with `uv sync --locked` and fails if it does not. Verify with:

```bash
uv lock --check
```

If the `uv lock` hook fails (for example `uv` is not on your `PATH`, or you are offline), the version is already bumped in `pyproject.toml` and `src/datacollective/__init__.py` but `uv.lock` is stale. Do **not** re-run the bump, as that would refuse the dirty tree or bump a second time. Either finish the step by hand:

```bash
uv lock
```

or undo the bump and start again from step 3:

```bash
git restore pyproject.toml src/datacollective/__init__.py
```

---

## 5. Merge the version bump through a pull request

Commit the version bump on the release branch, push it and open a pull request against `main`:

```bash
git add pyproject.toml src/datacollective/__init__.py uv.lock
git commit -m "Bump version to X.Y.Z"
git push -u origin release-X.Y.Z
gh pr create --base main --title "Bump version to X.Y.Z" --body "Release X.Y.Z"
```

Replace `X.Y.Z` with the new version (for example, `0.0.35`).

Wait for the required checks to pass, then merge the pull request. Afterwards, update your local `main`:

```bash
git switch main
git pull origin main
```

Confirm that `main` now carries the new version before going further:

```bash
grep '^version' pyproject.toml
```

---

## 6. Create a GitHub Release

Only do this once the bump pull request is merged (see the warning at the top of this page).

1. Go to the repository page on GitHub.
2. Open the `Releases` section.
3. Click `Draft a new release`.
4. Create a new tag named `X.Y.Z` (for example, `0.0.35`) with `main` as the target, so that it points at the merged bump commit.
5. Set the release title to the same value (e.g. `0.0.35`).
6. Add release notes (high-level changes).
7. Click `Publish release`.

This will trigger the `Publish to PyPI` GitHub Actions workflow.

---

## 7. Automatic publish to PyPI

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

## 8. Verify the release on PyPI

After the workflow succeeds:

1. Go to the project page on PyPI.
2. Confirm that the new version `X.Y.Z` is listed.
3. Optionally install the package in a clean environment to verify:

```bash
pip install --upgrade datacollective==X.Y.Z
```

---

## Notes and Best Practices

- **Single source of truth**: The version is managed only by bump-my-version. Do **not** manually edit the version in any file.

- **Tag format**: Always use `X.Y.Z` tags (for example, `0.0.35`). The `bump-my-version` configuration enforces this.

- **Manual workflow dispatch (advanced)**: In rare cases, you can re-run the publish job from the `Actions` tab using `workflow_dispatch`, but normally you should always go through a GitHub Release.
# Mozilla Data Collective Python SDK Library

Welcome to the documentation for the `datacollective` Python client for the
[Mozilla Data Collective](https://datacollective.mozillafoundation.org/) REST API.

This library helps you:

- Authenticate with the Mozilla Data Collective.
- Download datasets to local storage.
- Load supported datasets into AI-friendly formats, such as pandas DataFrames.

## Installation

Install from PyPI:

```bash
pip install datacollective
```

You can also use uv or other Python tooling as desired, as long as the package datacollective is installed in your environment.


## Getting an API Key

To use the Mozilla Data Collective API, you need an API key:

1. Sign in to the Mozilla Data Collective dashboard.
2. Create or retrieve an API key from your account/settings page.
3. Keep your key secret and do not commit it to version control.

## Configuration

The client reads configuration from environment variables and `.env` files.

### Environment variables

Required:

- `MDC_API_KEY` - Your Mozilla Data Collective API key.

Optional:

- `MDC_API_URL` - API endpoint (defaults to the production URL).
- `MDC_DOWNLOAD_PATH` - Local directory where datasets will be downloaded
  (defaults to `~/.mozdata/datasets`).

Example using environment variables directly:

```bash
export MDC_API_KEY=your-api-key-here
export MDC_API_URL=https://datacollective.mozillafoundation.org/api
export MDC_DOWNLOAD_PATH=~/.mozdata/datasets
```

### `.env` file

The client will automatically load configuration from a `.env` file in your
project root or present working directory.

Create a file named `.env`:

```bash
# MDC API Configuration
MDC_API_KEY=your-api-key-here
MDC_API_URL=https://datacollective.mozillafoundation.org/api
MDC_DOWNLOAD_PATH=~/.mozdata/datasets
```

> **Security note:** do not commit `.env` files to version control, as they
> contain secrets.

## Basic Usage

### Download a dataset

Use `save_dataset_to_disk` to download a dataset to the configured download path:

```python
from datacollective import save_dataset_to_disk

dataset = save_dataset_to_disk("your-dataset-id")

# Depending on the implementation, `dataset` may contain metadata
# about the downloaded files or a higher-level dataset object.
```

The files will be stored under `MDC_DOWNLOAD_PATH` (default `~/.mozdata/datasets`).

## Loading and Querying Datasets

> **Note:** in-memory dataset loading is currently supported only for certain datasets.

You can load supported datasets into memory and convert them to a `pandas`
`DataFrame` for analysis:

```python
from datacollective import load_dataset

dataset = load_dataset("your-dataset-id")

# Convert to pandas
df = dataset.to_pandas()

# Inspect available splits (e.g., train, dev, test)
print(dataset.splits)
```

Once loaded into a `DataFrame`, you can use standard `pandas` operations
to filter, aggregate, and analyze the data.

## Get dataset details

You can retrieve info from the datasheet of a dataset without downloading it:

```python
from datacollective import get_dataset_info

info = get_dataset_info("your-dataset-id")
print(info)
```

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

## API Reference

For a detailed API reference, see:

- `datacollective.datasets`
- `datacollective.api_utils`
- `datacollective.dataset_loading_scripts.registry`
- `datacollective.dataset_loading_scripts.common_voice`

The `docs/api.md` file is configured to be processed by the API
documentation plugin for MkDocs.

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

## API Reference

For a detailed API reference, see the [API Reference](api.md) section of the documentation.

> [!NOTE]
> This section is intended for maintainers of the `datacollective` library.

## Tests

Run the full test suite:
```bash
pytest -v
```

Note that the e2e tests require a valid `MDC_TEST_API_KEY` and a `MDC_TEST_API_URL` key set in your environment. Pytest will skip the live e2e tests automatically if either is missing.


## Release Workflow

Check out the [Release Workflow](release.md) document for details on how to
publish new versions of the library to PyPI using GitHub Actions.
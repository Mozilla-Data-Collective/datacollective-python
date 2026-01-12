<p align="center">
  <picture>
    <!-- When the user prefers dark mode, show the white logo -->
    <source media="(prefers-color-scheme: dark)" srcset="./docs/mdc_logo_white.png">
    <!-- When the user prefers light mode, show the black logo -->
    <source media="(prefers-color-scheme: light)" srcset="./docs/mdc_logo.png">
    <!-- Fallback: default to the black logo -->
    <img src="./docs/mdc_logo.png" width="35%" alt="Project logo"/>
  </picture>
</p>

<div align="center">

[![Published](https://github.com/Mozilla-Data-Collective/datacollective-python/actions/workflows/publish.yml/badge.svg)](https://github.com/Mozilla-Data-Collective/datacollective-python/actions/workflows/publish.yml/)
[![Docs](https://github.com/Mozilla-Data-Collective/datacollective-python/actions/workflows/docs.yml/badge.svg)](https://github.com/Mozilla-Data-Collective/datacollective-python/actions/workflows/docs.yml/)
[![Tests](https://github.com/Mozilla-Data-Collective/datacollective-python/actions/workflows/tests.yml/badge.svg)](https://github.com/Mozilla-Data-Collective/datacollective-python/actions/workflows/tests.yml/)

</div>

# Mozilla Data Collective Python API Library

Python library for interfacing with the [Mozilla Data Collective](https://datacollective.mozillafoundation.org/) REST API.

## Installation

```bash
pip install datacollective
```

## Quick Start

1. **Get your API key** from the Mozilla Data Collective [dashboard](https://datacollective.mozillafoundation.org/api-reference)

2. **Set the API key in your environment variable (or create `.env` file add it there)**:

**IMPORTANT NOTE:** Before trying to access any dataset, make sure you have thoroughly **read and agreed** to the specific dataset's conditions & licensing terms.

```
export MDC_API_KEY=your-api-key-here
```

3. **Get your dataset ID from the last section of the dataset URL at the MDC website**. 

> [!TIP]
> You can find the `dataset-id` by looking at the URL of the dataset's page on MDC platform. The ID is the unique string of characters located at the very end of the URL, after the `/datasets/` path. For example, for URL `https://datacollective.mozillafoundation.org/datasets/cminc35no007no707hql26lzk` dataset id will be `cminc35no007no707hql26lzk`.

4. **Save a dataset locally**:
```
from datacollective import save_dataset_to_disk

dataset_path = save_dataset_to_disk("your-dataset-id")
```

5. **Get information & metadata about a dataset**:

```
from datacollective import get_dataset_details

details = get_dataset_details("your-dataset-id")
```

6. **Load the dataset into a pandas DataFrame _(Only Common Voice datasets are supported right now)_**:

```
from datacollective import load_dataset

dataset = load_dataset("your-dataset-id")
```

## For more details, visit [our docs](https://Mozilla-Data-Collective.github.io/datacollective-python/)

## License

This project is released under [MPL (Mozilla Public License) 2.0](./LICENSE).

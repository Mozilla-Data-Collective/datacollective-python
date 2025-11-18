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

1. **Get your API key** from the Mozilla Data Collective dashboard

2. **Set the API key in your environment variable**:

```
export MDC_API_KEY=your-api-key-here
```

3.**Use the client**:
```
from datacollective import load_dataset

dataset = load_dataset("your-dataset-id")
```

## For more details, visit [our docs](https://Mozilla-Data-Collective.github.io/datacollective-python/)

## License

This project is released under [MPL (Mozilla Public License) 2.0](./LICENSE).

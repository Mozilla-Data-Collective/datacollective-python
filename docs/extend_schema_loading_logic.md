# Extending Schema Loading Logic

This document is for **developers** who want to add new loading strategies or task contracts within the MDC Python SDK.

Dispatch is **strategy-based**: the schema's `root_strategy` field (default: `"index"`) selects the loader class, and any strategy can be combined with any task. The optional `task` field only adds a validation step — the loaded DataFrame must contain the task's required logical columns.

## 1. How to add a new strategy

Supporting a new strategy (e.g. **manifest** — one JSON manifest per dataset) involves creating a new loader class and registering it.

### Step 1: Create the loader class

Create a new file under `src/datacollective/schema_loaders/strategies/`, for example `manifest.py`:

```python
from __future__ import annotations
from pathlib import Path
import pandas as pd
from datacollective.schema import DatasetSchema
from datacollective.schema_loaders.base import BaseSchemaLoader

class ManifestLoader(BaseSchemaLoader):
    """Load a dataset described by a single JSON manifest."""

    def __init__(self, schema: DatasetSchema, extract_dir: Path) -> None:
        super().__init__(schema, extract_dir)
        # Validate required schema fields up front
        if not schema.index_file:
            raise ValueError("manifest schema must specify 'index_file'")

    def load(self) -> pd.DataFrame:
        # BaseSchemaLoader provides shared helpers:
        # 1. Locate and read the index file
        raw_df = self._load_index_file()

        # 2. Apply column mappings and dtypes
        return self._apply_column_mappings(raw_df)
```

### Step 2: Shared helpers in `BaseSchemaLoader`

When implementing `load()`, you can leverage these methods from the base class:

| Method | Purpose |
|---|---|
| `_load_index_file()` | Reads the index file (CSV/TSV/pipe) based on schema settings. |
| `_resolve_index_file()` | Recursively finds the index file in the extraction directory. |
| `_apply_column_mappings()` | Renames columns and applies dtypes (e.g., `file_path`, `category`). |

### Step 3: Register the loader

1. **Add to the Enum**: add your strategy to the `Strategy` enum in `src/datacollective/schema_loaders/base.py`.
2. **Register the class** in `src/datacollective/schema_loaders/registry.py`:

```python
from datacollective.schema_loaders.strategies.manifest import ManifestLoader

_STRATEGY_REGISTRY: dict[Strategy, Type[BaseSchemaLoader]] = {
    Strategy.INDEX: IndexLoader,
    ...
    Strategy.MANIFEST: ManifestLoader,  # Add your new strategy here
}
```

3. **Update Schema**: if the strategy requires new YAML fields, add them to `DatasetSchema` in `src/datacollective/schema.py`.

### Loading Strategies (`Strategy` enum)

| Enum Member | YAML Value | Description |
|---|---|---|
| `Strategy.INDEX` | `"index"` (default) | Loads a single delimited index file, optionally applying column mappings. |
| `Strategy.MULTI_SPLIT` | `"multi_split"` | Loads multiple split files matching a pattern, adding a `split` column. |
| `Strategy.MULTI_SECTIONS` | `"multi_sections"` | Loads one index file per section directory, adding a `section` column. |
| `Strategy.PAIRED_GLOB` | `"paired_glob"` | Pairs audio files with sidecar files: `.txt` transcriptions, or JSON metadata/utterance files (with `format: "json"` and optional `record_path`). |
| `Strategy.GLOB` | `"glob"` | Walks directory-structured datasets, deriving metadata from the path hierarchy. |

## 2. How to add a task contract

The optional `task` field validates the loaded DataFrame against the task's required logical columns. Contracts live in `src/datacollective/schema_loaders/contracts.py`:

```python
TASK_CONTRACTS: dict[str, frozenset[str]] = {
    "ASR": frozenset({"audio_path", "transcription"}),
    "TTS": frozenset({"audio_path", "transcription"}),
    "LLM": frozenset({"text"}),
    # Add your new task contract here, e.g.
    # "MT": frozenset({"source_text", "target_text"}),
}
```

A contract violation raises `TaskValidationError`. Schemas whose task has no contract (e.g. `OTH`), or with no task at all, load without validation.

> **Note for registry schemas:** keep the `task` field in existing `schema.yaml`
> files — older SDK versions still require it.

## 3. Architecture Overview

### Data Flow

When a user calls `load_dataset("id")`:

1. **`download_dataset()`**: Downloads the archive. (Skipped if already downloaded; previously called `save_dataset_to_disk()`)
2. **`_extract_archive()`**: Extracts it to a local directory. (Skipped if already extracted)
3. **`_resolve_schema()`**: Locates or downloads `schema.yaml`.
4. **`_parse_schema()`**: Validates YAML into a `DatasetSchema` object.
5. **`_load_dataset_from_schema()`**:
    - If the schema specifies `extract_files`, extracts inner archives (skipped when already extracted).
    - Resolves the strategy loader from the **Registry** (`root_strategy`, default `"index"`).
    - If the task has a contract and the schema declares column mappings, fails fast when the declared logical columns cannot satisfy the contract.
    - Calls `loader.load()`.
    - Validates the loaded DataFrame against the task contract (when one exists).
    - Returns the final **pandas DataFrame**.

### Module Map

| Module | Responsibility |
|---|---|
| `datacollective.schema` | Pydantic models and YAML parsing. |
| `datacollective.schema_loaders.base` | Abstract base class, shared helpers, and strategy definitions. |
| `datacollective.schema_loaders.registry` | Strategy-to-loader mapping and load orchestration. |
| `datacollective.schema_loaders.contracts` | Task contracts and their validation. |
| `datacollective.schema_loaders.cache_schema` | Local schema caching and checksum validation. |
| `datacollective.schema_loaders.strategies.*` | One loader per strategy (index, multi_split, multi_sections, paired_glob, glob). |

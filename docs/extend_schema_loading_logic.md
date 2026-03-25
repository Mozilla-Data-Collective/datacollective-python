# Extending Schema Loading Logic

This document is for **developers** who want to add support for new tasks or implement new loading strategies within the MDC Python SDK.

## 1. How to add a new task type

Supporting a new task (e.g., **MT** — Machine Translation) involves creating a new loader class and registering it.

### Step 1: Create the loader class

Create a new file under `src/datacollective/schema_loaders/tasks/`, for example `mt.py`:

```python
from __future__ import annotations
from pathlib import Path
import pandas as pd
from datacollective.schema import DatasetSchema
from datacollective.schema_loaders.base import BaseSchemaLoader

class MTLoader(BaseSchemaLoader):
    """Load a machine-translation dataset."""

    def __init__(self, schema: DatasetSchema, extract_dir: Path) -> None:
        super().__init__(schema, extract_dir)
        # Validate required schema fields
        if not schema.index_file:
            raise ValueError("MT schema must specify 'index_file'")

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

Register your new class in `src/datacollective/schema_loaders/registry.py`:

```python
from datacollective.schema_loaders.tasks.mt import MTLoader

_TASK_REGISTRY: dict[str, Type[BaseSchemaLoader]] = {
    "ASR": ASRLoader,
    "TTS": TTSLoader,
    "MT":  MTLoader,  # Add your new task here
}
```

## 2. How to extend or update strategies

Strategies define the high-level approach to locating data (e.g., using an index file vs. globbing).

### Loading Strategies (`Strategy` enum)

Strategies are defined in the `Strategy` enum in `src/datacollective/schema_loaders/base.py`:

| Enum Member | YAML Value | Description |
|---|---|---|
| `Strategy.MULTI_SPLIT` | `"multi_split"` | Loads multiple files matching a pattern. |
| `Strategy.PAIRED_GLOB` | `"paired_glob"` | Pairs audio files with `.txt` files. |
| `Strategy.GLOB` | `"glob"` | Generic single-pattern globbing. |

### Adding a new strategy

1. **Add to the Enum**: Add your new strategy to the `Strategy` class in `base.py`.
2. **Implement Logic**: Add a branch in the relevant loader's `load()` method to handle the new strategy.
3. **Update Schema**: If the strategy requires new YAML fields, add them to `DatasetSchema` in `src/datacollective/schema.py`.

## 3. Architecture Overview

### Data Flow

When a user calls `load_dataset("id")`:

1. **`download_dataset()`**: Downloads the archive. (Skipped if already downloaded; previously called `save_dataset_to_disk()`)
2. **`_extract_archive()`**: Extracts it to a local directory. (Skipped if already extracted)
3. **`_resolve_schema()`**: Locates or downloads `schema.yaml`.
4. **`parse_schema()`**: Validates YAML into a `DatasetSchema` object.
5. **`load_dataset_from_schema()`**: 
    - Finds the correct loader in the **Registry**.
    - Calls `loader.load()`.
    - Returns the final **pandas DataFrame**.

### Module Map

| Module | Responsibility |
|---|---|
| `datacollective.schema` | Pydantic models and YAML parsing. |
| `datacollective.schema_loaders.base` | Abstract base class and strategy definitions. |
| `datacollective.schema_loaders.registry` | Task-to-loader mapping. |
| `datacollective.schema_loaders.cache_schema` | Local schema caching and checksum validation. |
| `datacollective.schema_loaders.tasks.*` | Implementation of task-specific logic (ASR, TTS). |

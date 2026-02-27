# Schema-Based Dataset Loading

## Overview

Every dataset on the Mozilla Data Collective (MDC) platform has an
associated **`schema.yaml`** file. This declarative file tells the SDK *how* to
turn the raw files inside the archive into a ready-to-use **pandas DataFrame**, 
without executing any custom code outside the datacollective library.

```python
from datacollective import load_dataset

df = load_dataset("your-dataset-id")
print(df.head())
```

Under the hood, `load_dataset()` performs the following steps automatically:

1. **Resolve the schema**: check local cache or the schema registry for `schema.yaml`. If the dataset is not registered this step raises a warning, so we never download an unsupported archive.
2. **Download** the archive (with resume support). The schema we fetched in step 1 tells the loader how the files are structured.
3. **Extract** the `.tar.gz` / `.zip` to a local directory.
4. **Parse** the YAML into a validated `DatasetSchema` (Pydantic model) and dispatch to the task-specific loader (ASR, TTS, …), which returns the final **DataFrame**.

The schema file describes:

- **What task** the dataset is for (ASR, TTS, …).
- **How to find** the data files (index file path, glob pattern, etc.).
- **How to map** raw columns / files into a clean DataFrame.

### Minimal example

```yaml
dataset_id: "common-voice-gsw-24"
task: "ASR"
format: "tsv"
index_file: "train.tsv"
columns:
  audio_path:
    source_column: "path"
    dtype: "file_path"
  transcription:
    source_column: "sentence"
    dtype: "string"
```

This tells the SDK: *"Read `train.tsv` as tab-separated, take the `path`
column as audio file paths and the `sentence` column as transcriptions."*


## Schema fields reference

### Required fields

Every schema **must** have:

| Field | Type | Required | Description |
|---|---|---|---|
| `dataset_id` | `str` | ✓ | Unique dataset identifier on MDC. |
| `task` | `str` | ✓ | Task type: determines which loader is used (e.g. `"ASR"`, `"TTS"`). |

### Loading strategies

The remaining fields depend on which **strategy** the dataset uses.  The
strategy is inferred from the fields present in the schema:

| Strategy | When to use                                                         | Key fields |
|---|---------------------------------------------------------------------|---|
| **Index-based** (default) | A metadata file (CSV / TSV / pipe-delimited) lists each sample.     | `format`, `index_file`, `columns` |
| **Multi-split** | Multiple split files (train, dev, test, …) each containing samples. | `root_strategy: "multi_split"`, `splits` |
| **Paired-glob** | Each audio file has a matching `.txt` file, no index file at all.   | `root_strategy: "paired_glob"`, `file_pattern`, `audio_extension` |

### Index-based fields

| Field | Default | Required | Description |
|---|---|---|---|
| `format` | — | ✓ | File format: `"csv"`, `"tsv"`, or `"pipe"`. |
| `index_file` | — | ✓ | Path to the metadata file, relative to the dataset root. |
| `columns` | — | ✓ | Mapping of logical column names → source columns (see below). |
| `base_audio_path` | `""` | ✗ | Directory prefix prepended to `file_path` dtype columns. |
| `separator` | Inferred from `format` | ✗ | Explicit column separator override (e.g. `"\|"`). |
| `has_header` | `true` | ✗ | Whether the index file has a header row. When `false`, `source_column` must be a positional integer. |
| `encoding` | `"utf-8"` | ✗ | File encoding (e.g. `"utf-8-sig"` for files with a BOM). |

### Multi-split fields

| Field | Default | Required | Description |
|---|---|---|---|
| `root_strategy` | — | ✓ | Must be `"multi_split"`. |
| `splits` | — | ✓ | List of split names to load (e.g. `["train", "dev", "test"]`). |
| `splits_file_pattern` | `"**/*.tsv"` | ✗ | Glob pattern to locate split files. |
| `columns` | *(optional)* | ✗ | Column mappings applied to every split frame. |
| `base_audio_path` | `""` | ✗ | Directory prefix prepended to `file_path` dtype columns. |

### Paired-glob fields

| Field | Default | Required | Description |
|---|---|---|---|
| `root_strategy` | — | ✓ | Must be `"paired_glob"`. |
| `file_pattern` | — | ✓ | Glob pattern to find text files (e.g. `"**/*.txt"`). |
| `audio_extension` | — | ✓ | Extension of the matching audio files (e.g. `".webm"`). |


## Column mapping

Used by **index-based** and **multi-split** strategies.  Each key under
`columns` is the **logical** column name that will appear in the resulting
DataFrame:

```yaml
columns:
  audio_path:
    source_column: "path"       # column name in the index file
    dtype: "file_path"          # see dtype table below
  transcription:
    source_column: "sentence"
    dtype: "string"
  speaker_id:
    source_column: "client_id"
    dtype: "category"
    optional: true              # skip silently if the column is missing
```

For **headerless** files (`has_header: false`), use a positional integer
instead of a column name:

```yaml
columns:
  audio_path:
    source_column: 0
    dtype: "file_path"
  transcription:
    source_column: 1
    dtype: "string"
```

### Supported dtypes

| dtype | Behaviour |
|---|---|
| `string` | Cast to `str` (default). |
| `file_path` | Resolve to an absolute path: `extract_dir / base_audio_path / value`. |
| `category` | Cast to pandas `Categorical`. |
| `int` | Numeric coercion → nullable `Int64`. |
| `float` | Numeric coercion → `float64`. |

## Content mapping

Used by **glob-based** strategies to describe how file contents become
DataFrame columns:

```yaml
content_mapping:
  text: "file_content"     # each file's text → "text" column
  meta_source: "file_name" # filename → "meta_source" column
```

---

## Complete examples

For full examples for each task and strategy, visit the respective task documentation pages under [docs/loaders/](./loaders/).

## Schema caching

The SDK caches the `schema.yaml` inside the extracted dataset directory to
minimise API calls:

1. When `load_dataset()` runs, it obtains the **archive checksum** from the
   download plan.
2. If a local `schema.yaml` exists and its stored `checksum` matches the
   archive checksum, the cached copy is used and no network request needed.
3. If the checksums differ (dataset was updated) or no cache exists, the
   schema is fetched from the remote registry and saved locally with the
   current checksum.
4. If the remote registry has no schema, a local cache is used as a fallback
   when available.

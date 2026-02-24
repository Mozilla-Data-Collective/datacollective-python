# ASR Loader

Loader for **ASR** (Automatic Speech Recognition) datasets.

There are two parsing strategies for ASR datasets, controlled by the `root_strategy` field in the schema.

## Strategies

### Index-based (default)

A CSV or TSV index file maps audio paths to transcriptions and other metadata columns.

**Controlled by:**

| Field | Description |
|---|---|
| `format` | File format (`"csv"` or `"tsv"`). |
| `index_file` | Path to the metadata file, relative to the dataset root. |
| `columns` | Mapping of logical column names to source columns and dtypes. |
| `base_audio_path` | *(optional)* Directory prefix prepended to `file_path` dtype columns. |

### Multi-split strategy (`root_strategy: "multi_split"`)

Each split (e.g. `train`, `dev`, `test`) is stored in a separate file. The loader locates all matching files, filters by the configured split names, adds a `split` column to each, applies column mappings, and concatenates all frames into a single DataFrame. The `split` value is taken from the file stem.

**Controlled by:**

| Field | Description |
|---|---|
| `splits` | List of split names to load (e.g. `["train", "dev", "test"]`). |
| `splits_file_pattern` | *(optional)* Glob pattern to locate split files (default: `"**/*.tsv"`). |
| `columns` | *(optional)* Column mappings applied to every split frame. |
| `base_audio_path` | *(optional)* Directory prefix prepended to `file_path` dtype columns. |

---

## Examples

### Index-based schema

```yaml
dataset_id: "common-voice-gsw-24"
task: "ASR"
format: "csv"
index_file: "train.csv"
base_audio_path: "clips/"
columns:
  audio_path:
    source_column: "path"
    dtype: "file_path"
  transcription:
    source_column: "sentence"
    dtype: "string"
  speaker_id:
    source_column: "client_id"
    dtype: "category"
    optional: true
```

### Multi-split schema

```yaml
dataset_id: "common-voice-gsw-24"
task: "ASR"
root_strategy: "multi_split"
splits:
  - dev
  - train
  - test
  - validated
  - invalidated
  - reported
  - other
splits_file_pattern: "**/*.tsv"
base_audio_path: "clips/"
columns:
  audio_path:
    source_column: "path"
    dtype: "file_path"
  transcription:
    source_column: "sentence"
    dtype: "string"
  speaker_id:
    source_column: "client_id"
    dtype: "category"
    optional: true
```


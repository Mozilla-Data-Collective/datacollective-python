# TTS Loader

Loader for **TTS** (Text-to-Speech) datasets.

There are three parsing strategies for TTS datasets, controlled by the `root_strategy` field in the schema.

## Strategies

### Index-based (default)

A CSV / TSV / pipe-delimited index file maps audio paths to transcriptions.

**Controlled by:**

| Field | Required | Description |
|---|---|---|
| `format` | ✗ | Optional file format hint (`"csv"`, `"tsv"`, `"pipe"`). When omitted, the loader infers it from `index_file` where possible. |
| `index_file` | ✓ | Path to the index file, relative to the dataset root. |
| `base_audio_path` | ✗ | Directory prefix or list of directories used to resolve `file_path` dtype columns. |
| `columns` | ✗ | Column mappings from source columns to logical names. |
| `separator` | ✗ | Explicit column separator (e.g. `"\|"`). |
| `has_header` | ✗ | Whether the index file has a header row. When `false`, `source_column` must be a positional integer. |
| `encoding` | ✗ | File encoding (e.g. `"utf-8-sig"` for files with a BOM). |

### Paired-file / glob-based (`root_strategy: "paired_glob"`)

Each audio file has a matching `.txt` file with the same stem. The loader recursively finds all text files, reads the transcription, and pairs them with the corresponding audio file. The parent directory name is captured as a `split` column in the resulting DataFrame.

**Controlled by:**

| Field | Required | Description |
|---|---|---|
| `file_pattern` | ✓ | Glob pattern used to find text files (e.g. `"**/*.txt"`). |
| `audio_extension` | ✓ | Extension of the matching audio files (e.g. `".webm"`). |

### Multi-sections strategy (`root_strategy: "multi_sections"`)

For archives split into multiple section directories, each containing its own
index file (e.g. `dataset/General/metadata.tsv`, `dataset/Chat/metadata.tsv`).
The loader reads the index file of every listed section, adds a `section`
column with the directory name, and concatenates the raw frames. Column
mappings are **not** applied — the original index columns are returned as-is.

**Controlled by:**

| Field | Required | Description |
|---|---|---|
| `sections` | ✓ | List of section directory names to load (e.g. `["General", "Chat"]`). Unlisted directories are ignored. |
| `section_root` | ✓ | Directory containing the section subdirectories, relative to the dataset root. |
| `index_file` | ✓ | Name of the per-section index file (e.g. `"metadata.tsv"`), resolved as `section_root/<section>/<index_file>`. |
| `format` | ✗ | Optional file format hint (`"csv"`, `"tsv"`, `"pipe"`). |

---

## Examples

### Index-based schema

```yaml
# Example: pipe-delimited, headerless metadata
dataset_id: "aso-ckb-tts"
task: "TTS"
format: "pipe"
separator: "|"
has_header: false
index_file: "metadata.csv"
base_audio_path: "wavs/"
columns:
  audio_path:
    source_column: 0        # positional index (no header)
    dtype: "file_path"
  transcription:
    source_column: 1
    dtype: "string"
```

If the index stores IDs instead of directly joinable filenames, the shared
`file_path` resolver also supports search-based matching:

```yaml
dataset_id: "example-tts"
task: "TTS"
index_file: "metadata.csv"
base_audio_path:
  - "wavs/"
  - "backup_wavs/"

columns:
  audio_path:
    source_column: "clip_id"
    dtype: "file_path"
    path_match_strategy: "exact"   # or "contains"
    file_extension: ".wav"
  transcription:
    source_column: "text"
    dtype: "string"
```

You can also build filenames and directory roots from metadata columns without
hardcoding dataset-specific logic:

```yaml
dataset_id: "example-tts-dynamic-path"
task: "TTS"
index_file: "metadata.tsv"
base_audio_path: "${split}/"

columns:
  audio_path:
    source_column: "sentence_id"
    dtype: "file_path"
    file_extension: ".wav"
    path_template: "${speaker_id}_khm_${value}"
  transcription:
    source_column: "text"
    dtype: "string"
```

For each row, that resolves to
`dataset_root / <split>/<speaker_id>_khm_<sentence_id>.wav`.

### Paired-glob schema

```yaml
dataset_id: "pl-PL-darkman"
task: "TTS"
root_strategy: "paired_glob"
file_pattern: "**/*.txt"
audio_extension: ".webm"
```

### Multi-sections schema

For a layout like `dataset/General/metadata.tsv`, `dataset/Chat/metadata.tsv`:

```yaml
dataset_id: "example-tts-sections"
task: "TTS"
root_strategy: "multi_sections"
section_root: "dataset"
sections:
  - "General"
  - "Chat"
index_file: "metadata.tsv"
format: "tsv"
```

The resulting DataFrame contains the raw index columns plus a `section`
column (`General` / `Chat`).

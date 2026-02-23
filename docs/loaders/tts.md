# TTS Loader

Loader for **TTS** (Text-to-Speech) datasets.

There are two parsing strategies for TTS datasets, controlled by the `root_strategy` field in the schema.

## Strategies

### Index-based (default)

A CSV / TSV / pipe-delimited index file maps audio paths to transcriptions.

**Controlled by:**

| Field | Description |
|---|---|
| `format` | File format (`"csv"`, `"tsv"`, `"pipe"`). |
| `index_file` | Path to the index file, relative to the dataset root. |
| `base_audio_path` | Directory prefix prepended to `file_path` dtype columns. |
| `columns` | Column mappings from source columns to logical names. |
| `separator` | Explicit column separator (e.g. `"\|"`). |
| `has_header` | Whether the index file has a header row. When `false`, `source_column` must be a positional integer. |
| `encoding` | File encoding (e.g. `"utf-8-sig"` for files with a BOM). |

### Paired-file / glob-based (`root_strategy: "paired_glob"`)

Each audio file has a matching `.txt` file with the same stem. The loader recursively finds all text files, reads the transcription, and pairs them with the corresponding audio file. The parent directory name is captured as a `split` column in the resulting DataFrame.

**Controlled by:**

| Field | Description |
|---|---|
| `file_pattern` | Glob pattern used to find text files (e.g. `"**/*.txt"`). |
| `audio_extension` | Extension of the matching audio files (e.g. `".webm"`). |
| `content_mapping` | Optional mapping of file content to DataFrame columns. |

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

### Paired-glob schema

```yaml
dataset_id: "pl-PL-darkman"
task: "TTS"
root_strategy: "paired_glob"
file_pattern: "**/*.txt"
audio_extension: ".webm"
```


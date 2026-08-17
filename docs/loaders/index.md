# Index Strategy (`root_strategy: "index"`)

A single delimited index file (CSV / TSV / pipe) lists one row per sample,
e.g. mapping audio paths to transcriptions and other metadata columns.

When the schema declares `columns`, the loader applies the column mappings
(renaming, dtype conversion, file-path resolution). When `columns` is omitted,
the raw index file is returned as-is.

**Controlled by:**

| Field | Required | Description |
|---|---|---|
| `root_strategy` | ✓ | Must be `"index"`. |
| `index_file` | ✓ | Path to the index file, relative to the dataset root. |
| `columns` | ✗ | Mapping of logical column names to source columns and dtypes. When omitted, the raw index file is returned unchanged. |
| `base_audio_path` | ✗ | Directory prefix or list of directories used to resolve `file_path`/`file_content` dtype columns. |
| `format` | ✗ | Optional file format hint (`"csv"`, `"tsv"`, `"pipe"`). When omitted, the loader infers it from `index_file` where possible. |
| `separator` | ✗ | Explicit column separator (e.g. `"\|"`). |
| `has_header` | ✗ | Whether the index file has a header row. When `false`, `source_column` must be a positional integer. |
| `encoding` | ✗ | File encoding (e.g. `"utf-8-sig"` for files with a BOM). |
| `strict` | ✗ | Disable archive heuristics: `index_file` must exist at its literal relative path (no recursive search), no separator sniffing, exact column-name matching only. |

---

## Examples

### Basic index schema (ASR)

```yaml
dataset_id: "cmj8u48g4005lnxzp98cpr7b2"
task: "ASR"
root_strategy: "index"
format: "tsv"

index_file: "ss-corpus-shi.tsv"
base_audio_path: "audios/"

columns:
  audio_path:
    source_column: "audio_file"
    dtype: "file_path"
  transcription:
    source_column: "transcription"
    dtype: "string"
  speaker_id:
    source_column: "client_id"
    dtype: "category"
    optional: true
  audio_id:
    source_column: "audio_id"
    dtype: "string"
    optional: true
  duration_ms:
    source_column: "duration_ms"
    dtype: "int"
    optional: true
  prompt_id:
    source_column: "prompt_id"
    dtype: "string"
    optional: true
  prompt:
    source_column: "prompt"
    dtype: "string"
    optional: true
  votes:
    source_column: "votes"
    dtype: "int"
    optional: true
  age:
    source_column: "age"
    dtype: "category"
    optional: true
  gender:
    source_column: "gender"
    dtype: "category"
    optional: true
  language:
    source_column: "language"
    dtype: "category"
    optional: true
  split:
    source_column: "split"
    dtype: "category"
    optional: true
  char_per_sec:
    source_column: "char_per_sec"
    dtype: "float"
    optional: true
  quality_tags:
    source_column: "quality_tags"
    dtype: "string"
    optional: true
```

### Pipe-delimited, headerless metadata (TTS)

```yaml
dataset_id: "aso-ckb-tts"
task: "TTS"
root_strategy: "index"
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

### Search-based audio resolution

When the metadata stores an ID or partial filename instead of a directly
joinable relative path, `file_path` columns can search within one or more
audio roots:

```yaml
dataset_id: "example-asr"
task: "ASR"
root_strategy: "index"
index_file: "data/metadata.csv"
base_audio_path:
  - "data/recipes/"
  - "data/giving_gift/"

columns:
  audio_path:
    source_column: "Sentence ID"
    dtype: "file_path"
    path_match_strategy: "exact"   # or "contains"
    file_extension: ".wav"
  transcription:
    source_column: "Sentences"
    dtype: "string"
```

`path_match_strategy: "direct"` remains the default and preserves the existing
`extract_dir / base_audio_path / value` behavior. The loader also trims BOMs
and surrounding header whitespace, and can retry common delimiters
automatically when a file initially parses as a single column.

If the true audio filename is composed from multiple metadata columns, use
`path_template` instead of a fuzzy search:

```yaml
dataset_id: "khmer-asr-cultural-dataset-4e33cd05"
task: "ASR"
root_strategy: "index"
index_file: "data/metadata.csv"
base_audio_path:
  - "data/recipes/"
  - "data/giving_gift/"

columns:
  audio_path:
    source_column: "Sentence ID"
    dtype: "file_path"
    file_extension: ".wav"
    path_template: "${Speaker ID}_khm_${Sentence ID}.wav"
  transcription:
    source_column: "Sentences"
    dtype: "string"
```

Template placeholders reference raw metadata column names exactly, and
`${value}` refers to the current `source_column` value. Relative paths are
resolved from the dataset root inferred from the resolved `index_file`.

If the audio directory itself varies per row, `base_audio_path` can use the
same placeholder syntax:

```yaml
dataset_id: "khmer-asr-cultural-dataset-4e33cd05"
task: "ASR"
root_strategy: "index"
index_file: "data/metadata.csv"
base_audio_path: "data/${Split}/"

columns:
  audio_path:
    source_column: "Sentence ID"
    dtype: "file_path"
    file_extension: ".wav"
    path_template: "${Speaker ID}_khm_${value}"
  transcription:
    source_column: "Sentences"
    dtype: "string"
```

That resolves each row as
`dataset_root / data/<Split>/<Speaker ID>_khm_<Sentence ID>.wav`.

### File-content dtype

When the index file stores paths to transcription files instead of inline text,
use `dtype: "file_content"` to read the file contents into the DataFrame:

```yaml
dataset_id: "speech-data-nupe"
task: "ASR"
root_strategy: "index"
index_file: "Metadata.csv"
base_audio_path:
  - "Speaker_id_1"
  - "Speaker_id_2"

columns:
  audio_path:
    source_column: "Audio_File_Path"
    dtype: "file_path"
    file_extension: ".wav"
  transcription:
    source_column: "Transcript_File_Path"
    dtype: "file_content"
    file_extension: ".txt"
  speaker_id:
    source_column: "Speaker_ID"
```

The `file_content` dtype reuses the same path resolution as `file_path`
(`base_audio_path`, `file_extension`, `path_match_strategy`, `path_template`)
but returns the file's text content instead of the resolved path.

### Raw index file (no column mappings)

A dataset described by a single delimited index file, returned as-is. Note
that a schema without `columns` should only declare a `task` whose raw columns
already satisfy the task contract (or a task without one, e.g. `OTH`) —
otherwise every load emits a `TaskValidationWarning`.

```yaml
dataset_id: "common-voice-text-langid"
task: "OTH"
root_strategy: "index"
format: "tsv"
index_file: "data.tsv"

columns:
  sentence:
    source_column: "sentence"
    dtype: "string"
  language:
    source_column: "lang"
    dtype: "category"
```

When `columns` is omitted, the index file is returned as a raw DataFrame with
its original columns.

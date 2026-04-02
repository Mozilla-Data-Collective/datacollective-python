# ASR Loader

Loader for **ASR** (Automatic Speech Recognition) datasets.

There are two parsing strategies for ASR datasets, controlled by the `root_strategy` field in the schema.

## Strategies

### Index-based (default)

A CSV or TSV index file maps audio paths to transcriptions and other metadata columns.

**Controlled by:**

| Field | Required | Description |
|---|---|---|
| `index_file` | ✓ | Path to the metadata file, relative to the dataset root. |
| `columns` | ✓ | Mapping of logical column names to source columns and dtypes. |
| `base_audio_path` | ✗ | *(optional)* Directory prefix or list of directories used to resolve `file_path` dtype columns. |
| `format` | ✗ | Optional file format hint (`"csv"`, `"tsv"`, `"pipe"`). When omitted, the loader infers it from `index_file` where possible. |

### Multi-split strategy (`root_strategy: "multi_split"`)

Each split (e.g. `train`, `dev`, `test`) is stored in a separate file. The loader locates all matching files, filters by the configured split names, adds a `split` column to each, applies column mappings, and concatenates all frames into a single DataFrame. The `split` value is taken from the file stem.

**Controlled by:**

| Field | Required | Description |
|---|---|---|
| `splits` | ✓ | List of split names to load (e.g. `["train", "dev", "test"]`). |
| `splits_file_pattern` | ✗ | *(optional)* Glob pattern to locate split files (default: `"**/*.tsv"`). |
| `columns` | ✗ | *(optional)* Column mappings applied to every split frame. |
| `base_audio_path` | ✗ | *(optional)* Directory prefix or list of directories used to resolve `file_path` dtype columns. |

---

## Examples

### Index-based schema

```yaml
dataset_id: "cmj8u48g4005lnxzp98cpr7b2"
task: "ASR"
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

### Search-based audio resolution

When the metadata stores an ID or partial filename instead of a directly
joinable relative path, `file_path` columns can search within one or more
audio roots:

```yaml
dataset_id: "example-asr"
task: "ASR"
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

### Multi-split schema

```yaml
dataset_id: "cmj8u3okr0001nxxbeshupy5k"
task: "ASR"
root_strategy: "multi_split"

splits:
  - dev
  - invalidated
  - other
  - reported
  - test
  - train
  - validated

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
  sentence_id:
    source_column: "sentence_id"
    dtype: "string"
    optional: true
  sentence_domain:
    source_column: "sentence_domain"
    dtype: "category"
    optional: true
  up_votes:
    source_column: "up_votes"
    dtype: "int"
    optional: true
  down_votes:
    source_column: "down_votes"
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
  accents:
    source_column: "accents"
    dtype: "category"
    optional: true
  variant:
    source_column: "variant"
    dtype: "category"
    optional: true
  locale:
    source_column: "locale"
    dtype: "category"
    optional: true

```

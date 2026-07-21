# ASR Loader

Loader for **ASR** (Automatic Speech Recognition) datasets.

There are three parsing strategies for ASR datasets, controlled by the `root_strategy` field in the schema.

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

### Paired-glob JSON strategy (`root_strategy: "paired_glob"`)

For datasets with **no central index file**, where each audio file is paired
with a JSON sidecar (e.g. `recording.merged.json` + `recording.wav`). The
loader globs for the JSON files, flattens each one into rows, and applies the
regular column mappings.

When `record_path` is set, the named top-level JSON key must hold a **list of
records** (e.g. time-aligned utterances) and each record becomes one DataFrame
row. The remaining top-level keys are flattened with dot notation
(`audio.filename`, `metadata.speaker2_gender`, …) and repeated on every row of
that file. Without `record_path`, each JSON file yields a single row.

Audio pairing does not rely on filename-stem matching: source the `audio_path`
column from a filename field inside the JSON and resolve it with
`path_match_strategy: "exact"`.

**Controlled by:**

| Field | Required | Description |
|---|---|---|
| `format` | ✓ | Must be `"json"`. |
| `file_pattern` | ✓ | Glob pattern to find the JSON sidecars (e.g. `"**/*.merged.json"`). |
| `columns` | ✓ | Mapping of logical column names to (dot-notation) source columns and dtypes. |
| `record_path` | ✗ | *(optional)* Top-level JSON key holding the list of records; one row per record. |
| `audio_extension` | ✗ | *(optional)* Extension of the paired audio files (e.g. `".wav"`), documentation / fallback. |

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

If the true audio filename is composed from multiple metadata columns, use
`path_template` instead of a fuzzy search:

```yaml
dataset_id: "khmer-asr-cultural-dataset-4e33cd05"
task: "ASR"
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

### Paired-glob JSON schema

Each `*.merged.json` sidecar describes one WAV recording: an `audio` block
(with the exact WAV filename), a flat `metadata` block, and a `transcriptions`
array of time-aligned utterances. The schema below yields one row per
utterance, with the per-recording `audio.*` / `metadata.*` fields repeated on
every row:

```yaml
dataset_id: "xxx"
task: "ASR"
root_strategy: "paired_glob"
format: "json"

file_pattern: "**/*.merged.json"
audio_extension: ".wav"

record_path: "transcriptions"

columns:
  audio_path:
    source_column: "audio.filename"
    dtype: "file_path"
    path_match_strategy: "exact"
  transcription:
    source_column: "text"
    dtype: "string"
  utterance_id:
    source_column: "utt_id"
    dtype: "string"
    optional: true
  speaker_id:
    source_column: "speaker"
    dtype: "category"
    optional: true
  start_time:
    source_column: "start_time"
    dtype: "float"
    optional: true
  end_time:
    source_column: "end_time"
    dtype: "float"
    optional: true
  audio_duration_sec:
    source_column: "audio.duration_sec"
    dtype: "float"
    optional: true
  sample_rate_hz:
    source_column: "audio.sample_rate_hz"
    dtype: "int"
    optional: true
  gender:
    source_column: "metadata.speaker2_gender"
    dtype: "category"
    optional: true
  topic:
    source_column: "metadata.user_topic"
    dtype: "string"
    optional: true
  validation_id:
    source_column: "metadata.val_id"
    dtype: "string"
    optional: true
```

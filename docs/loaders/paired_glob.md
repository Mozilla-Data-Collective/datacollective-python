# Paired-Glob Strategy (`root_strategy: "paired_glob"`)

For datasets with **no central index file**, where each audio file is paired
with a sidecar file. Two variants exist, selected by the `format` field:

- **Text sidecars** (default): each audio file has a matching `.txt` file with
  the same stem containing the transcription.
- **JSON sidecars** (`format: "json"`): each audio file has a JSON sidecar
  with metadata and (optionally) a list of utterance records.


## Text-sidecar variant (default)

The loader recursively finds all text files matching `file_pattern`, reads the
transcription, and pairs each with the audio file sharing the same stem. The
parent directory name is captured as a `split` column. The resulting DataFrame
always contains `audio_path` and `transcription`, so it satisfies the ASR/TTS
contracts by construction.

**Controlled by:**

| Field | Required | Description |
|---|---|---|
| `file_pattern` | ✓ | Glob pattern used to find text files (e.g. `"**/*.txt"`). |
| `audio_extension` | ✓ | Extension of the matching audio files (e.g. `".webm"`). |

## JSON-sidecar variant (`format: "json"`)

The loader globs for the JSON files, flattens each one into rows, and applies
the regular column mappings.

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

### Text-sidecar schema

```yaml
dataset_id: "pl-PL-darkman"
task: "TTS"
root_strategy: "paired_glob"
file_pattern: "**/*.txt"
audio_extension: ".webm"
```

### JSON-sidecar schema

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

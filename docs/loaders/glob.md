# Glob Strategy (`root_strategy: "glob"`)

The loader recursively finds all files matching `file_pattern` and derives
metadata from each matched file's path. What ends up in the DataFrame is
controlled by an optional `columns` mapping over **path-derived sources**;
when `columns` is omitted, a fixed default output is used.


**Controlled by:**

| Field | Required | Description |
|---|---|---|
| `root_strategy` | ✓ | Must be `"glob"`. |
| `file_pattern` | ✓ | Glob pattern to match files (e.g. `"**/*.wav"`). |
| `columns` | ✗ | Mapping of logical column names to **path-derived sources** (see below). When omitted, the default output is used. |
| `splits` | ✗ | List of subdirectory names to glob through. Each becomes a value in the `split` column. When omitted, the glob runs from the dataset root. |
| `extract_files` | ✗ | List of inner archives to extract before loading (see below). |


## Column mapping (path-derived sources)

Each entry under `columns` works like the regular column mapping (logical
name, `dtype`, `optional`, …), except that `source_column` names a value
derived from the matched file's path instead of a column in an index file:

| `source_column` | Value per matched file |
|---|---|
| `path` | Absolute path to the file |
| `name` | File name, with extension (`a.wav`) |
| `stem` | File name without extension (`a`) |
| `parent` | Parent directory name (same as `parents[0]`) |
| `parents[N]` | Name of the Nth ancestor directory (`parents[1]` = grandparent). Empty string when the path is not that deep. |
| `content` | Text content of the file, read with `schema.encoding` and stripped. Only read when referenced. |

Any other `source_column` value fails loader construction with a `ValueError`
listing the supported sources. `dtype` conversions (`category`, `int`,
`float`, `string`) apply as usual. With a mapping, glob output can satisfy a
task contract (e.g. `LLM` via `content` → `text`).


## Default output (no `columns`)

For each matched file, the resulting DataFrame contains:

| Column | Source |
|---|---|
| `audio_path` | Absolute path to the file |
| `speaker_id` | Grandparent directory name |
| `language` | Parent directory name |
| `split` | *(only when `splits` is set)* The split directory the file was found in |


This is equivalent to the mapping:

```yaml
columns:
 audio_path:
   source_column: "path"
 language:
   source_column: "parent"
 speaker_id:
   source_column: "parents[1]"
```

---

## Inner archive extraction

Some datasets ship as an outer archive containing inner `.tar.gz` or `.zip`
files. The `extract_files` field lists these inner archives (paths relative to
the dataset root). The SDK extracts them automatically before loading and
skips re-extraction on subsequent runs.

```yaml
extract_files:
  - "Train.tar.gz"
  - "Dev.tar.gz"
```

This field is strategy-agnostic — it works with any loader, not just glob.

---

## Examples

### Flat directory (no splits, default output)


A dataset where audio files are organised as `speaker_id/language/utterance.wav` directly under the dataset root.

```yaml
dataset_id: "tidyvoicex2-asv"
task: "OTH"
root_strategy: "glob"
file_pattern: "**/*.wav"
```

Resulting DataFrame:

| audio_path | speaker_id | language |
|---|---|---|
| `/path/to/id020001/es/es_19400023.wav` | `id020001` | `es` |
| `/path/to/id020001/fr/fr_19399754.wav` | `id020001` | `fr` |


### Custom column mapping


The same layout, but with explicit names and dtypes — and the utterance ID
derived from the file stem:


```yaml
dataset_id: "tidyvoicex2-asv"
task: "OTH"
root_strategy: "glob"
file_pattern: "**/*.wav"
columns:
 audio_path:
   source_column: "path"
 utterance_id:
   source_column: "stem"
 language:
   source_column: "parent"
   dtype: "category"
 speaker_id:
   source_column: "parents[1]"
   dtype: "category"
```


### Text dataset via `content` (LLM)


A text corpus organised as `language/document.txt`, loaded with the file
contents as the `text` column — this satisfies the `LLM` task contract:


```yaml
dataset_id: "my-text-corpus"
task: "LLM"
root_strategy: "glob"
file_pattern: "**/*.txt"
columns:
 text:
   source_column: "content"
 language:
   source_column: "parent"
   dtype: "category"
 file_name:
   source_column: "name"
```


### With splits and inner archives

A dataset containing `Train.tar.gz` and `Dev.tar.gz`, each extracting to a directory with the same `speaker_id/language/utterance.wav` structure.

```yaml
dataset_id: "tidyvoicex-asv"
task: "OTH"
root_strategy: "glob"
file_pattern: "**/*.wav"
splits:
  - "TidyVoiceX_Train"
  - "TidyVoiceX_Dev"
extract_files:
  - "TidyVoiceX_Train.tar.gz"
  - "TidyVoiceX_Dev.tar.gz"
```

Resulting DataFrame:

| audio_path | speaker_id | language | split |
|---|---|---|---|
| `/path/to/TidyVoiceX_Train/id011210/en/en_40387752.wav` | `id011210` | `en` | `TidyVoiceX_Train` |
| `/path/to/TidyVoiceX_Dev/id013915/lt/lt_36478609.wav` | `id013915` | `lt` | `TidyVoiceX_Dev` |

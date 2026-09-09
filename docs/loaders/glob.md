# Glob Strategy (`root_strategy: "glob"`)

The loader recursively finds all files matching `file_pattern` and derives
metadata from the directory structure. For each matched file, the resulting
DataFrame contains:

| Column | Source |
|---|---|
| `audio_path` | Absolute path to the file |
| `speaker_id` | Grandparent directory name |
| `language` | Parent directory name |
| `split` | *(only when `splits` is set)* The split directory the file was found in |


**Controlled by:**

| Field | Required | Description |
|---|---|---|
| `root_strategy` | ✓ | Must be `"glob"`. |
| `file_pattern` | ✓ | Glob pattern to match files (e.g. `"**/*.wav"`). |
| `splits` | ✗ | List of subdirectory names to glob through. Each becomes a value in the `split` column. When omitted, the glob runs from the dataset root. |
| `extract_files` | ✗ | List of inner archives to extract before loading (see below). |

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

### Flat directory (no splits)

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
# Multi-Split Strategy (`root_strategy: "multi_split"`)

Each split (e.g. `train`, `dev`, `test`) is stored in a separate file. The
loader locates all matching files, filters by the configured split names, adds
a `split` column to each, applies column mappings (when declared), and
concatenates all frames into a single DataFrame. The `split` value is taken
from the file stem.

**Controlled by:**

| Field | Required | Description |
|---|---|---|
| `splits` | ✓ | List of split names to load (e.g. `["train", "dev", "test"]`). |
| `splits_file_pattern` | ✗ | *(optional)* Glob pattern to locate split files (default: `"**/*.tsv"`). |
| `columns` | ✗ | *(optional)* Column mappings applied to every split frame. When omitted, the raw columns plus `split` are returned. |
| `base_audio_path` | ✗ | *(optional)* Directory prefix or list of directories used to resolve `file_path` dtype columns. |

---

## Example

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

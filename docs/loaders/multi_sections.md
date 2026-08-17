# Multi-Sections Strategy (`root_strategy: "multi_sections"`)

For archives split into multiple section directories, each containing its own
index file (e.g. `dataset/General/metadata.tsv`, `dataset/Chat/metadata.tsv`).
The loader reads the index file of every listed section, adds a `section`
column with the directory name, and concatenates the frames. When the schema
declares `columns`, the mappings are applied to each section frame (the
`section` column is kept); when omitted, the raw index columns are returned
as-is.

**Controlled by:**

| Field | Required | Description |
|---|---|---|
| `sections` | ✓ | List of section directory names to load (e.g. `["General", "Chat"]`). Unlisted directories are ignored. |
| `section_root` | ✓ | Directory containing the section subdirectories, relative to the dataset root. |
| `index_file` | ✓ | Name of the per-section index file (e.g. `"metadata.tsv"`), resolved as `section_root/<section>/<index_file>`. |
| `columns` | ✗ | *(optional)* Column mappings applied to every section frame. When omitted, the raw index columns plus `section` are returned. |
| `format` | ✗ | Optional file format hint (`"csv"`, `"tsv"`, `"pipe"`). |

---

## Example

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
columns:
  audio_path:
    source_column: "audio"
    dtype: "file_path"
  transcription:
    source_column: "text"
    dtype: "string"
```

The resulting DataFrame contains the mapped columns plus a `section` column
(`General` / `Chat`). Without `columns`, the raw index columns are returned
instead (in that case a `task` with a contract can only be declared when the
raw columns already satisfy it).

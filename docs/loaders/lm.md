# LM Loader

Loader for **LM** (Language Model / text) datasets.

The LM loader recursively scans the extracted dataset directory for text files matching a glob pattern. Each matching file becomes one row in the resulting DataFrame.

## Strategies

### Glob-based (`root_strategy: "glob"`)

Every text file found by the glob pattern is read and its contents (and/or metadata) are mapped to DataFrame columns via `content_mapping`.

**Controlled by:**

| Field | Description |
|---|---|
| `file_pattern` | Glob pattern used to find text files (e.g. `"**/*.txt"`). |
| `content_mapping` | Describes how file content and metadata map to DataFrame columns. |
| `encoding` | *(optional)* File encoding (e.g. `"utf-8-sig"` for files with a BOM). Default: `"utf-8"`. |

#### `content_mapping` fields

| Field | Description |
|---|---|
| `text` | Set to `"file_content"` to read the full file text into the `text` column. |
| `meta_source` | Set to `"file_name"` to capture the file name into the `meta_source` column. |

---

## Examples

### Glob-based schema

```yaml
dataset_id: "my-lm-dataset"
task: "LM"
root_strategy: "glob"
file_pattern: "**/*.txt"
content_mapping:
  text: "file_content"
  meta_source: "file_name"
```


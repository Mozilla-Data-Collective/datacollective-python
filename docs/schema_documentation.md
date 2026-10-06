# Schema-Based Dataset Loading

## Overview

Every dataset on the Mozilla Data Collective (MDC) platform has an
associated **`schema.yaml`** file. This declarative file tells the SDK *how* to
turn the raw files inside the archive into a ready-to-use **pandas DataFrame**, 
without executing any custom code outside the datacollective library.

```python
from datacollective import load_dataset

df = load_dataset("your-dataset-id")
print(df.head())
```

Under the hood, `load_dataset()` performs the following steps automatically:

1. **Resolve the schema**: check local cache or the schema registry for `schema.yaml`. If the dataset is not registered this step raises a warning, so we never download an unsupported archive.
2. **Download** the archive (with resume support). The schema we fetched in step 1 tells the loader how the files are structured.
3. **Extract** the `.tar.gz` / `.zip` to a local directory.
4. **Parse** the YAML into a validated `DatasetSchema` (Pydantic model) and dispatch to the loader for the schema's `root_strategy` (index, glob, …), which returns the final **DataFrame**. When the schema declares a `task` with a known contract (ASR, TTS, LLM), the loaded DataFrame is checked for the task's required logical columns; a `TaskValidationWarning` is emitted if any are missing (the DataFrame is still returned).

The schema file describes:

- **How to find** the data files (index file path, glob pattern, etc.).
- **How to map** raw columns / files into a clean DataFrame.
- Optionally, **what task** the dataset is for (ASR, TTS, …), which the loaded DataFrame is checked against (violations emit a warning).

### Minimal example

```yaml
dataset_id: "common-voice-gsw-24"
task: "ASR"
root_strategy: "index"
format: "tsv"
index_file: "train.tsv"
columns:
  audio_path:
    source_column: "path"
    dtype: "file_path"
  transcription:
    source_column: "sentence"
    dtype: "string"
```

This tells the SDK: *"Read `train.tsv` as tab-separated, take the `path`
column as audio file paths and the `sentence` column as transcriptions."*


## Schema fields reference

### Required fields

Every schema **must** have:

| Field | Type | Required | Description |
|---|---|---|---|
| `dataset_id` | `str` | ✓ | Unique dataset identifier on MDC. |
| `root_strategy` | `str` | ✓ | Loading strategy: `"index"`, `"multi_split"`, `"multi_sections"`, `"paired_glob"`, or `"glob"`. There is no default — every schema must set it explicitly. |
| `task` | `str` | ✗ | *(optional)* Task type as defined on the MDC Platform (`"ASR"`, `"TTS"`, …). When set to a task with a known contract, the loaded DataFrame is checked for the task's required logical columns (e.g. ASR/TTS: `audio_path` + `transcription`; LLM: `text`); missing columns emit a `TaskValidationWarning` (shown even with `enable_logging=False`) but the DataFrame is still returned. Tasks without a contract (e.g. `"OTH"`) load without validation. |


### Loading strategies

The remaining fields depend on which **strategy** the dataset uses.  The
strategy is selected with the required `root_strategy` field:

| Strategy | When to use                                                         | Key fields |
|---|---------------------------------------------------------------------|---|
| **Index-based** | A metadata file (CSV / TSV / pipe-delimited) lists each sample.     | `root_strategy: "index"`, `index_file`, `columns` |
| **Multi-split** | Multiple split files (train, dev, test, …) each containing samples. | `root_strategy: "multi_split"`, `splits` |
| **Paired-glob** | Each audio file has a matching sidecar file (`.txt` for TTS, JSON for ASR), no index file at all. | `root_strategy: "paired_glob"`, `file_pattern`, `audio_extension` (TTS) / `format: "json"`, `record_path`, `columns` (ASR) |
| **Multi-sections** | Multiple section directories, each with its own index file. | `root_strategy: "multi_sections"`, `sections`, `section_root`, `index_file` |
| **Glob** | Directory-structured dataset with metadata encoded in the path hierarchy. | `root_strategy: "glob"`, `file_pattern` |

### Index-based fields

| Field | Default | Required | Description |
|---|---|---|---|
| `root_strategy` | — | ✓ | Must be `"index"`. |
| `format` | Inferred from the file extension | ✗ | Format hint: `"csv"`, `"tsv"`, or `"pipe"`. Required when the extension is not `.csv`, `.tsv`, `.tab`, `.psv` or `.pipe` (e.g. `meta.txt`) and `separator` is not set. See [How delimited files are read](#how-delimited-files-are-read). |
| `index_file` | — | ✓ | Path to the metadata file, relative to the dataset root. |
| `columns` | — | ✓ | Mapping of logical column names → source columns (see below). |
| `base_audio_path` | `""` | ✗ | Directory prefix or list of directories used to resolve `file_path` dtype columns. Entries may also use `${column}` placeholders from the current metadata row. |
| `separator` | Inferred from `format` or `index_file` | ✗ | Explicit column separator override (e.g. `"\|"`). |
| `has_header` | `true` | ✗ | Whether the index file has a header row. When `false`, `source_column` must be a positional integer. |
| `encoding` | `"utf-8"` | ✗ | File encoding (e.g. `"utf-8-sig"` for files with a BOM). |
| `na_values` | `[""]` | ✗ | Cell values read as missing. Only the listed values count, so by default only empty cells are missing and words such as `NA`, `None` or `null` stay text. Applies to every strategy that reads delimited files. |
| `quoting` | `"none"` for tab-separated files, `"minimal"` otherwise | ✗ | How `"` is treated: `"minimal"` parses CSV-style quoted fields, `"none"` reads `"` as an ordinary character. Applies to every strategy that reads delimited files. See [Quoting in delimited files](#quoting-in-delimited-files). |
| `strict` | `false` | ✗ | Disable archive heuristics for deterministic loading: `index_file` must exist at its literal path relative to the dataset root (no recursive search) and source column names must match exactly (no fuzzy matching). For `multi_split`, every declared split must have a split file. Applies to every strategy that reads delimited files. |

The `index_file` lookup is deterministic even without `strict`: the literal
relative path wins when it exists; otherwise the tree is searched recursively
and the shallowest match is used — multiple matches at the same depth raise an
error instead of picking one arbitrarily.

### Multi-split fields

| Field | Default | Required | Description |
|---|---|---|---|
| `root_strategy` | — | ✓ | Must be `"multi_split"`. |
| `splits` | — | ✓ | List of split names to load (e.g. `["train", "dev", "test"]`). A declared split with no file emits a `DataLoadWarning` (an error with `strict: true`). |
| `splits_file_pattern` | `"**/*.tsv"` | ✗ | Glob pattern to locate split files. The shallowest match per split wins; two matches at the same depth (e.g. `a/train.tsv` and `b/train.tsv`) raise an error. |
| `columns` | *(optional)* | ✗ | Column mappings applied to every split frame. |
| `base_audio_path` | `""` | ✗ | Directory prefix or list of directories used to resolve `file_path` dtype columns. Entries may also use `${column}` placeholders from the current metadata row. |

### Paired-glob fields

**TTS (text sidecars)** — each audio file has a matching `.txt` file with the
transcription; pairing is done on the filename stem:

| Field | Default | Required | Description |
|---|---|---|---|
| `root_strategy` | — | ✓ | Must be `"paired_glob"`. |
| `file_pattern` | — | ✓ | Glob pattern to find text files (e.g. `"**/*.txt"`). |
| `audio_extension` | — | ✓ | Extension of the matching audio files (e.g. `".webm"`). |
| `columns` | — | ✗ | Optional mappings applied over the derived `audio_path` / `transcription` / `split` sources (rename, dtype, drop). The `split` column is kept. When omitted, the default `audio_path` / `transcription` / `split` output is returned. |

**ASR (JSON sidecars)** — each audio file has a matching JSON file holding the
audio filename, metadata, and (optionally) a list of time-aligned utterance
records:

| Field | Default | Required | Description |
|---|---|---|---|
| `root_strategy` | — | ✓ | Must be `"paired_glob"`. |
| `format` | — | ✓ | Must be `"json"`. |
| `file_pattern` | — | ✓ | Glob pattern to find the JSON sidecars (e.g. `"**/*.merged.json"`). |
| `columns` | — | ✓ | Column mappings over the flattened JSON; nested keys use dot notation (e.g. `audio.filename`, `metadata.speaker2_gender`). |
| `record_path` | — | ✗ | Top-level JSON key holding a list of records (e.g. `"transcriptions"`); each record becomes one row and the remaining top-level keys are repeated per row. When omitted, each JSON file yields one row. |
| `audio_extension` | — | ✗ | Extension of the paired audio files (e.g. `".wav"`). Pairing normally comes from a filename field inside the JSON, mapped as a `file_path` column with `path_match_strategy: "exact"`. |

See the [paired-glob strategy](./loaders/paired_glob.md) for a complete example.

### Multi-sections fields

| Field | Default | Required | Description |
|---|---|---|---|
| `root_strategy` | — | ✓ | Must be `"multi_sections"`. |
| `sections` | — | ✓ | List of section directory names to load. Unlisted directories are ignored. |
| `section_root` | — | ✓ | Directory containing the section subdirectories, relative to the dataset root. |
| `index_file` | — | ✓ | Name of the per-section index file, resolved as `section_root/<section>/<index_file>`. |

Column mappings are applied to each section's index file when `columns` is
declared (otherwise the raw columns are returned), and a `section` column with
the directory name is added before concatenation.

### Glob fields

| Field | Default | Required | Description |
|---|---|---|---|
| `root_strategy` | — | ✓ | Must be `"glob"`. |
| `file_pattern` | — | ✓ | Glob pattern to match files (e.g. `"**/*.wav"`). |
| `columns` | — | ✗ | Mapping of logical column names to **path-derived sources**: `path`, `name`, `stem`, `parent`, `parents[N]`, `content` (file text). When omitted, the default output is `audio_path`, `language` (parent directory), `speaker_id` (grandparent directory). See the [glob strategy](./loaders/glob.md) page. |
| `splits` | — | ✗ | List of subdirectory names to glob through. Each becomes a value in the `split` column. When omitted, the glob runs from the dataset root. |

### Inner archive extraction

| Field | Default | Required | Description |
|---|---|---|---|
| `extract_files` | — | ✗ | List of archive paths (relative to dataset root) to extract before loading. Supports `.tar.gz`, `.tar.bz2`, `.tar.xz`, and `.zip`. Extraction is skipped on subsequent runs. |

This field is task-agnostic — it works with any loader.


## How delimited files are read

Index, multi-split and multi-sections files are read the same way. The loader
does not guess anything about the file; what it does is set by the schema:

- **Separator.** Taken from `separator`, else `format`, else the file
  extension (`.csv` → `,`; `.tsv`/`.tab` → tab; `.psv`/`.pipe` → `|`). The
  file contents are never inspected: when none of the three applies (e.g.
  `meta.txt` with no `format`), loading fails and asks for `format` or
  `separator`. An unknown `format` value also fails.
- **Values are text.** Every value is read exactly as written. `0012` stays
  `"0012"` and a transcript `1.50` stays `"1.50"`. Numbers and categories come
  only from a column mapping's `dtype` (`int`, `float`, `category`). Without
  `columns`, every column of the returned DataFrame is text.
- **Missing values.** Only values listed in `na_values` are missing. The
  default `[""]` means empty cells only; pandas' own list (`NA`, `None`,
  `null`, `n/a`, `nan`, …) is not used, because those are real transcripts and
  codes. List them to restore that behaviour for a column of codes, e.g.
  `na_values: ["", "NA"]` (this applies to every column in the file).
- **Trailing separators.** A separator at the end of data lines but not the
  header (common in exported files) does not shift the columns.

## Quoting in delimited files

Delimited files (CSV, TSV, pipe-separated) disagree on what a `"` means:

- **Quoted files.** CSV-style writers wrap a value in `"…"` when it contains
  the separator, a line break or a `"`, and double any `"` inside it. The
  quotes are syntax, not data. `pandas.DataFrame.to_csv` does this (including
  with `sep="\t"`), as do Python's `csv.writer`, Excel and Google Sheets.
- **Unquoted files.** Every `"` is part of the text. Common Voice TSVs work
  this way: a sentence such as `"Hi," she said.` is stored exactly as written.

Reading a file with the wrong rule corrupts it silently, so the loader uses the
`quoting` field:

| `quoting` | Behaviour |
|---|---|
| *(omitted)* | `"none"` for tab-separated files, `"minimal"` for everything else. |
| `"minimal"` | CSV-style: quoted values are unwrapped and may contain the separator or line breaks. |
| `"none"` | `"` is an ordinary character. Values cannot contain the separator or line breaks. |

The separator is resolved first (`separator`, then `format`, then the file
extension); a tab separator selects `"none"` by default.

### Example: Common Voice (no quoting)

`train.tsv` contains:

```text
path	sentence
a.mp3	"Hi," she said.
b.mp3	"Quoted start with no closing quote.
c.mp3	This row is swallowed.
d.mp3	A quote" in the middle.
e.mp3	Last.
```

The default for TSVs reads all five rows exactly as written. With
`quoting: minimal`, two things go wrong without any error:

- Row `a` loses its quotes: `"Hi,"` is read as a quoted value, giving
  `Hi, she said.`
- Row `b` opens a quoted value that runs to the next `"` in the file, across
  tabs and line breaks. Rows `b`, `c` and `d` become a single row, so 5 rows
  load as 3.

### Example: TSV written by pandas (quoted)

The file was produced by
`df.to_csv("train.tsv", sep="\t", index=False)` from a frame containing the
sentence `He said "hi"`:

```text
path	sentence
a.mp3	"He said ""hi"""
```

With the default (`"none"`), the sentence is loaded as `"He said ""hi"""`,
quotes and all. Declare the quoting instead:

```yaml
index_file: "train.tsv"
quoting: "minimal"
```

The sentence is now loaded as `He said "hi"`. If a quoted value contains a tab
or a line break, the default is worse still: the row is split apart, which
either raises a parse error or shifts values into the wrong columns.

### Example: CSV that does not quote (no quoting)

A CSV can have the same problem when its values start with `"` without being
quoted:

```text
path,sentence
a.mp3,"Quoted start
b.mp3,next row
c.mp3,a quote" here
d.mp3,last
```

The CSV default (`"minimal"`) reads this as 2 rows. Declare `quoting: "none"`
to keep all 4. This only works if no value contains a comma, since an unquoted
value cannot hold the separator.

### Warnings

Both mistakes are silent, so the loader checks the result and emits a
`DataLoadWarning`:

- **Lines merged into quoted values** (when `quoting` is `"minimal"`, including
  the default for CSVs): the file has more non-empty data lines than parsed
  rows. That is expected when quoted values span several lines. Otherwise a
  stray `"` has swallowed rows, and `quoting: "none"` fixes it.
- **Values that look quoted** (when a tab-separated file is read with the
  default `"none"`): a value starts and ends with `"` and contains a doubled
  `""`, which is what a quoting writer produces. Set `quoting: "minimal"` if
  the file was written that way, or `quoting: "none"` to confirm the quotes are
  data and silence the warning. A quoted value without an inner `"` (quoted
  only because it contains a tab or line break) is not detected.

To act on these in code, filter for them like any other warning:

```python
import warnings
from datacollective.errors import DataLoadWarning

warnings.simplefilter("error", DataLoadWarning)  # fail instead of warn
```

---

## Column mapping

Used by every strategy. Each key under `columns` is the **logical** column
name that will appear in the resulting DataFrame.  For the glob strategy,
`source_column` names a path-derived source (`path`, `parent`, `content`, …)
instead of an index-file column — see the [glob strategy](./loaders/glob.md)
page; for the paired-glob text variant it names one of the derived
`audio_path` / `transcription` / `split` sources.  For the index-based
strategies:

```yaml
columns:
  audio_path:
    source_column: "path"       # column name in the index file
    dtype: "file_path"          # see dtype table below
  transcription:
    source_column: "sentence"
    dtype: "string"
  speaker_id:
    source_column: "client_id"
    dtype: "category"
    optional: true              # skip silently if the column is missing
```

For datasets where the index stores an ID instead of the full audio filename,
you can opt into search-based file resolution:

```yaml
base_audio_path:
  - "data/recipes/"
  - "data/giving_gift/"

columns:
  audio_path:
    source_column: "Sentence ID"
    dtype: "file_path"
    path_match_strategy: "exact"   # "direct" (default), "exact", or "contains"
    file_extension: ".wav"         # optional, helps when the index omits the suffix
```

With `path_match_strategy: "exact"`, the loader searches the configured
`base_audio_path` directories for a matching filename or stem. With
`"contains"`, it searches for a filename or relative path containing the
source value. If that source value is not unique enough on its own, use
`path_template` to build the real filename from multiple metadata columns:

```yaml
base_audio_path:
  - "data/recipes/"
  - "data/giving_gift/"

columns:
  audio_path:
    source_column: "Sentence ID"
    dtype: "file_path"
    file_extension: ".wav"
    path_template: "${Speaker ID}_khm_${Sentence ID}.wav"
```

`path_template` placeholders reference raw index-file columns exactly as they
appear in the metadata, and `${value}` refers to the current column's source
value.

`base_audio_path` can use the same placeholder syntax when the containing
directory also depends on metadata columns:

```yaml
base_audio_path: "data/${Split}/"

columns:
  audio_path:
    source_column: "Sentence ID"
    dtype: "file_path"
    file_extension: ".wav"
    path_template: "${Speaker ID}_khm_${value}"
```

In that example, each row resolves to
`dataset_root / data/<Split>/<Speaker ID>_khm_<Sentence ID>.wav`.

For **headerless** files (`has_header: false`), use a positional integer
instead of a column name:

```yaml
columns:
  audio_path:
    source_column: 0
    dtype: "file_path"
  transcription:
    source_column: 1
    dtype: "string"
```

### Supported dtypes

| dtype | Behaviour |
|---|---|
| `string` | Cast to `str` (default). |
| `file_path` | Resolve to an absolute path. By default this is `dataset_root / base_audio_path / value`, but the loader can also search one or more `base_audio_path` roots when `path_match_strategy` is set, or render filenames and directory roots from metadata columns with `path_template` and templated `base_audio_path` entries. |
| `file_content` | Like `file_path`, but instead of keeping the resolved path, reads the file and returns its text content. Useful when the index stores paths to transcription files rather than inline text. Supports the same resolution options (`base_audio_path`, `file_extension`, `path_match_strategy`, `path_template`). |
| `category` | Cast to pandas `Categorical`. |
| `int` | Numeric coercion → nullable `Int64`. |
| `float` | Numeric coercion → `float64`. |

Column mappings are validated at parse time: an unknown `dtype` value or an
unknown key inside a mapping entry (e.g. `dtpye:`) raises a `ValueError`
instead of being silently ignored. Unknown **top-level** schema keys emit a
`SchemaValidationWarning` (with a "did you mean …?" hint for near-misses) and
are kept under the `extra` catch-all, and unknown `root_strategy` values fail
at parse time.

---

## Complete examples

For full examples for each strategy, visit the respective strategy documentation pages under [docs/loaders/](./loaders/).

## Schema caching

The SDK caches the `schema.yaml` inside the extracted dataset directory to
minimise API calls:

1. When `load_dataset()` runs, it obtains the **archive checksum** from the
   download plan.
2. If a local `schema.yaml` exists and its stored `checksum` matches the
   archive checksum, the cached copy is used and no network request needed.
3. If the checksums differ (dataset was updated) or no cache exists, the
   schema is fetched from the remote registry and saved locally with the
   current checksum.
4. If the remote registry has no schema, a local cache is used as a fallback
   when available.

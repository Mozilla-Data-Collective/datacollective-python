# Migrating Schemas to SDK 0.6.0

SDK 0.6.0 changes how `schema.yaml` files are dispatched and validated. This
guide explains what changed, how to tell whether a schema is affected, and how
to migrate it — organised by loading strategy, with a before/after example for
each case.

## What changed in 0.6.0

**Before 0.6.0**, the `task` field selected the loader (`ASR`, `TTS`, `OTH`),
each task loader supported only some strategies, and a missing `root_strategy`
silently fell back to index-based loading.

**Since 0.6.0:**

1. **`root_strategy` is required.** There is no default. A schema without it
   fails with:

        ValueError: Schema must specify 'root_strategy'. Supported strategies:
        index, multi_split, multi_sections, paired_glob, glob

2. **Dispatch is strategy-based and task-agnostic.** The `root_strategy` field
   alone selects the loader; any strategy can be combined with any task
   (e.g. `TTS` + `multi_split` is now valid).

3. **`task` is optional and only validates.** When set to a task with a known
   contract, the loaded DataFrame is expected to contain that task's logical
   columns:

    | Task | Required columns in the loaded DataFrame |
    |---|---|
    | `ASR` | `audio_path`, `transcription` |
    | `TTS` | `audio_path`, `transcription` |
    | `LLM` | `text` |
    | `OTH` (or any other value) | *no validation* |

    A violation emits a `TaskValidationWarning` listing the missing and
    available columns — the DataFrame is **still returned**. The warning is
    issued through Python's `warnings` module, so it is shown even when
    package logging is disabled (`enable_logging=False`).

4. **`multi_sections` now applies `columns`.** Previously column mappings were
   silently ignored for this strategy; now they are applied when declared
   (the `section` column is kept).

5. **Unknown `root_strategy` values now error.** Previously a typo (e.g.
   `root_strategy: "multisplit"`) silently fell through to index-based
   loading; now it raises
   `ValueError: Unknown root_strategy 'multisplit'. Supported strategies: …`.

6. **`content_mapping` is removed.** The field was never consumed by any
   loader ("reserved for future use"); its use case is covered by the glob
   strategy's `columns` mapping. Schemas still carrying the block parse fine —
   it lands in the schema's `extra` catch-all and is ignored, so no migration
   is needed beyond optionally deleting it.

7. **`index_file` resolution is deterministic.** The literal path relative to
   the dataset root wins when it exists; otherwise the recursive search picks
   the shallowest match as before, but **multiple matches at the same depth
   now raise an error** instead of silently picking one. Affected archives
   (equal-depth duplicate index files) must set `index_file` to an explicit
   relative path. A new optional `strict: true` field additionally disables
   the recursive search, separator sniffing, and fuzzy column-name matching.

> **Keep the `task` field.** Even though 0.6.0 no longer needs it for
> dispatch, SDK versions **before** 0.6.0 require `task` and use it to select
> the loader. Registry schemas must keep it so both old and new SDKs can load
> the dataset.

## Migration checklist

For every `schema.yaml` in the registry:

1. **Add `root_strategy`** if it is missing. Pre-0.6.0, only index-based
   schemas could omit it, so a schema without `root_strategy` becomes
   `root_strategy: "index"`.
2. **Check the task contract.** If `task` is `ASR` or `TTS`, the loaded
   DataFrame should contain `audio_path` and `transcription` (every load
   emits a `TaskValidationWarning` otherwise):
    - If the schema declares `columns`, those two must appear as **logical
      column names** (the keys of the `columns` mapping). Rename keys like
      `audio` → `audio_path` or `text` → `transcription` if needed.
    - If the schema declares no `columns`, the **raw file headers** must
      already be named `audio_path` and `transcription` — otherwise add a
      `columns` block that maps them.
3. **Fix silent misconfigurations** that 0.6.0 now surfaces: typo'd
   `root_strategy` values, and inert `columns` blocks on `multi_sections`
   schemas (now applied — verify the mappings are correct or remove them).

The sections below walk through each strategy.

---

## Case 1: Index-based schemas

**Who is affected:** every schema without a `root_strategy` field. This was
the implicit default, so this is the most common case.

**Migration:** add `root_strategy: "index"`. If the task is `ASR`/`TTS`, also
make sure the logical column names include `audio_path` and `transcription`.

**Symptom if not migrated:**
`ValueError: Schema must specify 'root_strategy'. …`

### Before (pre-0.6.0)

```yaml
dataset_id: "my-asr-dataset"
task: "ASR"
format: "tsv"
index_file: "metadata.tsv"
base_audio_path: "clips/"
columns:
  audio:                        # ← non-contract logical name
    source_column: "path"
    dtype: "file_path"
  text:                         # ← non-contract logical name
    source_column: "sentence"
    dtype: "string"
```

### After (0.6.0)

```yaml
dataset_id: "my-asr-dataset"
root_strategy: "index"          # ← now required, no default
task: "ASR"
format: "tsv"
index_file: "metadata.tsv"
base_audio_path: "clips/"
columns:
  audio_path:                   # ← renamed to satisfy the ASR contract
    source_column: "path"
    dtype: "file_path"
  transcription:                # ← renamed to satisfy the ASR contract
    source_column: "sentence"
    dtype: "string"
```

Renaming only changes the **logical** (output) column names — the
`source_column` values stay whatever the file actually contains. If the old
logical names had downstream consumers, note that the loaded DataFrame's
column names change with them.

### Sub-case: index schema without `columns` (raw loading)

Pre-0.6.0, `TTS` and `OTH` index schemas could omit `columns` and return the
raw file. That still works in 0.6.0, **but** a raw `ASR`/`TTS` schema now
emits a `TaskValidationWarning` on every load unless the raw headers happen
to be named `audio_path` and `transcription`. Add a minimal `columns` block:

```yaml
# Before — loaded the raw file, columns were e.g. "wav" and "sentence"
dataset_id: "my-tts-dataset"
task: "TTS"
index_file: "meta.csv"
```

```yaml
# After — explicit strategy + mappings that satisfy the TTS contract
dataset_id: "my-tts-dataset"
root_strategy: "index"
task: "TTS"
index_file: "meta.csv"
columns:
  audio_path:
    source_column: "wav"
    dtype: "file_path"
  transcription:
    source_column: "sentence"
    dtype: "string"
```

For `OTH` (or any task without a contract), adding `root_strategy: "index"`
is the only required change — raw loading without `columns` keeps working.

---

## Case 2: Multi-split schemas (`root_strategy: "multi_split"`)

**Who is affected:** schemas already set `root_strategy: "multi_split"`, so no
dispatch change is needed. Only the task contract applies.

**Migration:**

- With `columns`: ensure the logical names include `audio_path` and
  `transcription` (same rename as Case 1).
- Without `columns`: the raw split files' headers must already contain
  `audio_path` and `transcription`; otherwise add a `columns` block. (Common
  Voice-style files with `path`/`sentence` headers need mappings.)

**Symptom if not migrated:** a `TaskValidationWarning` on every load (the
DataFrame is still returned).

### Before (pre-0.6.0)

```yaml
dataset_id: "my-multisplit-dataset"
task: "ASR"
root_strategy: "multi_split"
splits: ["train", "dev", "test"]
splits_file_pattern: "**/*.tsv"
# no columns → raw file headers (path, sentence, …) + a "split" column
```

### After (0.6.0)

```yaml
dataset_id: "my-multisplit-dataset"
task: "ASR"
root_strategy: "multi_split"
splits: ["train", "dev", "test"]
splits_file_pattern: "**/*.tsv"
base_audio_path: "clips/"
columns:                        # ← added so the output satisfies the ASR contract
  audio_path:
    source_column: "path"
    dtype: "file_path"
  transcription:
    source_column: "sentence"
    dtype: "string"
```

The `split` column is added automatically in both cases.

> New in 0.6.0: `multi_split` is no longer ASR-only — it can be used with any
> task.

---

## Case 3: Multi-sections schemas (`root_strategy: "multi_sections"`)

**Who is affected:** all of them, in two ways:

1. Pre-0.6.0 this strategy **ignored** `columns` and always returned the raw
   per-section index columns plus `section`. In 0.6.0, `columns` is applied
   when declared.
2. These schemas were TTS-only, and the TTS contract now expects
   `audio_path` + `transcription` in the output. Raw section files rarely use
   those exact header names, so most `multi_sections` schemas with
   `task: "TTS"` need a `columns` block added to load without warnings.

**Migration:** add a `columns` block mapping the section files' headers to
`audio_path`/`transcription`. If a schema already carried an (inert) `columns`
block, verify the mappings are actually correct — they now take effect.

**Symptom if not migrated:** a `TaskValidationWarning: Loaded dataset does
not satisfy the 'TTS' task contract: missing column(s) ['audio_path',
'transcription'] …` on every load (the DataFrame is still returned).

### Before (pre-0.6.0)

Layout: `dataset/General/metadata.tsv`, `dataset/Chat/metadata.tsv`, each with
headers `audio` and `text`.

```yaml
dataset_id: "my-sections-dataset"
task: "TTS"
root_strategy: "multi_sections"
section_root: "dataset"
sections: ["General", "Chat"]
index_file: "metadata.tsv"
format: "tsv"
# → raw columns: audio, text, section
```

### After (0.6.0)

```yaml
dataset_id: "my-sections-dataset"
task: "TTS"
root_strategy: "multi_sections"
section_root: "dataset"
sections: ["General", "Chat"]
index_file: "metadata.tsv"
format: "tsv"
columns:                        # ← now applied; output: audio_path, transcription, section
  audio_path:
    source_column: "audio"
    dtype: "file_path"
  transcription:
    source_column: "text"
    dtype: "string"
```

The `section` column is always kept, with or without mappings.

---

## Case 4: Paired-glob text schemas (`root_strategy: "paired_glob"`, text sidecars)

**Who is affected:** nobody — **no migration needed**.

These schemas already set `root_strategy`, and the loader constructs the
output columns itself (`audio_path`, `transcription`, `split`), so the
ASR/TTS contract is satisfied by construction.

### Valid before and after

```yaml
dataset_id: "my-paired-tts-dataset"
task: "TTS"
root_strategy: "paired_glob"
file_pattern: "**/*.txt"
audio_extension: ".webm"
```

> New in 0.6.0: the text-sidecar variant is no longer TTS-only. Pre-0.6.0,
> `task: "ASR"` with `paired_glob` demanded `format: "json"`; now an ASR
> dataset with `.txt` sidecars is expressible with exactly the schema above
> (just with `task: "ASR"`).

---

## Case 5: Paired-glob JSON schemas (`root_strategy: "paired_glob"`, `format: "json"`)

**Who is affected:** only schemas whose `columns` use non-contract logical
names. `root_strategy` and `format: "json"` were already required, and the
required fields are unchanged (`file_pattern`, `columns`; optional
`record_path`).

**Migration:** verify the `columns` keys include `audio_path` and
`transcription` when `task` is `ASR`/`TTS`; rename the logical keys if not
(same as Case 1).

**Symptom if not migrated:** a `TaskValidationWarning` on every load,
listing the missing contract columns (the DataFrame is still returned).

### Valid before and after

```yaml
dataset_id: "my-json-asr-dataset"
task: "ASR"
root_strategy: "paired_glob"
format: "json"
file_pattern: "**/*.merged.json"
record_path: "transcriptions"
columns:
  audio_path:                   # ← contract column
    source_column: "audio.filename"
    dtype: "file_path"
    path_match_strategy: "exact"
  transcription:                # ← contract column
    source_column: "text"
    dtype: "string"
  speaker_id:
    source_column: "speaker"
    dtype: "category"
    optional: true
```

---

## Case 6: Glob schemas (`root_strategy: "glob"`)

**Who is affected:** nobody with `task: "OTH"` — **no migration needed**.

`root_strategy` was already required for glob, the fields are unchanged, and
`OTH` has no contract, and the pre-0.6.0 fields work unchanged: without
`columns`, the loader keeps producing its default output (`audio_path`,
`language`, `speaker_id`, and `split` when `splits` is set).


New in 0.6.0 (optional): glob schemas may declare a `columns` mapping over
path-derived sources (`path`, `name`, `stem`, `parent`, `parents[N]`,
`content`) to control the output columns instead of relying on the default —
see the [glob strategy](loaders/glob.md) page. Existing schemas do not need
this to migrate.


### Valid before and after

```yaml
dataset_id: "my-glob-dataset"
task: "OTH"
root_strategy: "glob"
file_pattern: "**/*.wav"
splits: ["Train", "Dev"]
extract_files:
  - "Train.tar.gz"
  - "Dev.tar.gz"
```


> Caution: avoid combining `glob` with `task: "ASR"` or `task: "TTS"` unless
> a `columns` mapping actually produces the contract columns — the default
> glob output has no `transcription` column, so every load would emit a
> `TaskValidationWarning`.


---

## Verifying a migrated schema

Test the schema locally against the extracted dataset before submitting it to
the registry:

```python
from pathlib import Path
from datacollective.schema import _parse_schema
from datacollective.schema_loaders.registry import _load_dataset_from_schema

schema = _parse_schema(Path("path/to/extracted/schema.yaml"))
df = _load_dataset_from_schema(schema, extract_dir=Path("path/to/extracted/"))
print(df.head())
```

A migrated schema is correct when:

- it loads without `ValueError` (strategy resolution),
- it emits no `TaskValidationWarning` (run with
  `python -W error::UserWarning …` or `warnings.simplefilter("error")` to
  turn the warning into a hard failure during testing), **and**
- for `ASR`/`TTS`, `{"audio_path", "transcription"} <= set(df.columns)`.

Because the migrated schema keeps `task` and only *adds* fields that pre-0.6.0
loaders either required anyway (`columns` renames) or ignored
(`root_strategy: "index"` on index schemas, `columns` on `multi_sections`),
sanity-check the pre-0.6.0 behavior too if the dataset must stay loadable by
older SDKs — with one caveat: pre-0.6.0 `multi_sections` ignores the new
`columns` block, so old SDKs return the raw section columns while 0.6.0
returns the mapped ones.

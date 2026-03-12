# Adding Support for Your Dataset

If you are a **dataset owner** and want your dataset to be loadable via `load_dataset()`, you need to provide a `schema.yaml` file. This document describes how to create one, test it locally, and submit it to the registry.

## Overview

A `schema.yaml` file tells the MDC Python SDK how to interpret the files in your dataset archive. Instead of writing custom parsing code for every dataset, we use these declarative schemas to automatically map your files into a clean **pandas DataFrame**.

## How to create a `schema.yaml`

### Step 1: Inspect your archive

Extract your dataset and understand its file layout. This determines which **strategy** you should use:

- **Index-based**: You have a central metadata file (CSV, TSV, or pipe-delimited) that lists paths to files and their transcriptions/metadata.
- **Multi-split**: You have separate files for each split (e.g., `train.tsv`, `test.tsv`, `dev.tsv`).
- **Paired-glob**: There is no index file; instead, each audio file has a matching `.txt` file with the same name.

### Step 2: Identify the task

Determine which task your dataset belongs to. Most datasets on MDC are either **ASR** (Automatic Speech Recognition) or **TTS** (Text-to-Speech).

### Step 3: Write the schema

Create a file named `schema.yaml`. Start with the basic required fields:

```yaml
dataset_id: "your-dataset-id"   # The unique ID of the dataset on MDC
task: "ASR"                    # ASR or TTS
```

Then add the fields for your chosen strategy.

#### Example: Index-based ASR
If your dataset has a `metadata.tsv` file:

```yaml
dataset_id: "your-dataset-id"
task: "ASR"
format: "tsv"
index_file: "metadata.tsv"
base_audio_path: "clips/"      # Folder where audio files are located
columns:
  audio_path:
    source_column: "path"      # Name of the column in metadata.tsv
    dtype: "file_path"
  transcription:
    source_column: "sentence"
    dtype: "string"
```

### Step 4: Test locally

You can test your schema before submitting it. Place your `schema.yaml` in the folder where you extracted the dataset and run:

```python
from pathlib import Path
from datacollective.schema import _parse_schema
from datacollective.schema_loaders.registry import _load_dataset_from_schema

# Path to your local schema
schema = _parse_schema(Path("path/to/extracted/schema.yaml"))

# Load using the schema
df = _load_dataset_from_schema(schema, extract_dir=Path("path/to/extracted/"))

print(df.head())
```

If the DataFrame looks correct, your schema is ready!

### Step 5: Submit to the registry

Once tested, submit your `schema.yaml` to the [dataset-schema-registry](https://github.com/Mozilla-Data-Collective/dataset-schema-registry) repository. 
Place it under `registry/<your-dataset-id>/schema.yaml`.

## Next steps

- For a full list of available fields and data types, see the [Schema-Based Loading](schema_documentation.md) reference.
- If your dataset requires a custom loading logic not covered by existing strategies, see [Extending Schema Loading Logic](extend_schema_loading_logic.md).


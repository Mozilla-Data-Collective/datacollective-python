# Library Development

This page is intended for developers of this library with access to development environments.

## API endpoint overrides

The SDK defaults to the production API endpoint, so normal usage should not set
`MDC_API_URL`.

For internal development, staging, or test environments, you can override the
default API endpoint by setting `MDC_API_URL` in your shell or `.env` file.

Example shell configuration:

```bash
export MDC_API_URL=https://your-dev-environment.example/api
```

Example `.env` entry:

```bash
MDC_API_URL=https://your-dev-environment.example/api
```

Notes:

- The override applies to the SDK API requests.
- Custom non-production URLs are passed through unchanged.

## Live E2E tests

The live E2E tests are intended for internal SDK validation. They require:

- `MDC_TEST_API_KEY`
- `MDC_TEST_API_URL`

Example:

```bash
export MDC_TEST_API_KEY=your-test-api-key
export MDC_TEST_API_URL=https://your-dev-environment.example/api
pytest -v
```

Pytest skips the live E2E tests automatically if either variable is missing.

## OpenAPI contract test

The platform publishes its API contract at `https://mozilladatacollective.com/api/openapi.json`. 
The SDK has `tests/test_openapi_contract.py` to ensure that the manually maintained mirrored models
and contract are aligned. 

### What it checks

- Every endpoint the SDK calls exists in the spec with the same HTTP method.
- The `Task` and `Visibility` enums equal the values the update request accepts,
  and `Visibility` is a subset of what responses may return. The SDK enum covers 
  only settable values and read models keep `visibility` a plain string so 
  observe-only values (e.g. `hidden`) still parse.
- The draft, update and submit field sets equal the corresponding request schemas
  in both directions: the SDK never sends a field the platform rejects, and the
  platform never accepts a field the SDK cannot set.
- Every field declared on `Dataset`, `DatasetSubmission` and `DatasetDetails`
  exists in the spec's response schemas, so no field silently reads as `None`.
- The upload and download payloads match, and the response keys the SDK relies
  on (`downloadUrl`, `sizeBytes`, `fileUploadId`, `uploadId`, `url`, `partNumber`)
  are marked required by the spec.

When the spec and the SDK disagree, the test fails and stays red until the SDK model is 
updated (or the platform updates the spec).

### Which spec it runs against

By default the test reads a snapshot at `tests/fixtures/openapi.json`,
so the unit test suite is deterministic and offline. Refresh the snapshot when the
platform ships an API change:

```bash
curl -sL https://mozilladatacollective.com/api/openapi.json \
    | python -m json.tool > tests/fixtures/openapi.json
pytest tests/test_openapi_contract.py
```

Commit the refreshed snapshot together with any model changes it required, so the
diff shows what moved in the contract and how the SDK adapted.

Set `MDC_OPENAPI_SPEC` to a file path or URL to run against a different spec:

```bash
MDC_OPENAPI_SPEC=https://mozilladatacollective.com/api/openapi.json pytest tests/test_openapi_contract.py
MDC_OPENAPI_SPEC=../platform/openapi.json pytest tests/test_openapi_contract.py
```

## Related workflows

- For release steps, see [Release Workflow](release.md).
- For loader and schema extension work, see [Extending the Loading Logic](extend_schema_loading_logic.md)
  and [Adding a New Schema](add_new_schema.md).

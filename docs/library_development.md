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

The platform publishes its API contract at `<base-url>/api/openapi.json`, generated from
the same schemas its routes run on. The SDK's models are written by hand, so
`tests/test_openapi_contract.py` checks that they still agree with that contract.

### What it checks

- Every endpoint the SDK calls exists in the spec with the same HTTP method.
- The `Task` and `Visibility` enums equal the values the update request accepts.
  `Visibility` is additionally a subset of what responses may return: the SDK enum
  covers only settable values, and `DatasetDetails` is tolerant of unknown values so
  observe-only ones (e.g. `hidden`) still parse.
- The update and submit field sets equal the corresponding request schemas in both
  directions: the SDK never sends a field the platform rejects, and the platform never
  accepts a field the SDK cannot set. The draft field set only has to be accepted by the
  create request and cover its required fields, since the SDK creates drafts with the
  minimum payload and sends everything else via PATCH.
- Every field declared on a read model exists in the schema of the payload it parses:
  `DatasetDetails` against the dataset response and `DatasetSubmission` against the
  submission response. A field on the shared `Dataset` base therefore has to exist in
  both payloads; otherwise it would silently read as `None` on one of them.
- The upload and download payloads match, and the response keys the SDK relies
  on (`downloadUrl`, `sizeBytes`, `fileUploadId`, `uploadId`, `url`, `partNumber`)
  are marked required by the spec.

When the spec and the SDK disagree, the test fails and stays red until the SDK model is
updated (or the platform updates the spec). There is deliberately no allowlist of known
drift.

### Which spec it runs against

By default the test reads the snapshot at `tests/fixtures/openapi.json`, so the unit
test suite is deterministic and offline. Refresh the snapshot from production, the API the
released SDK talks to by default:

```bash
curl -sL https://mozilladatacollective.com/api/openapi.json \
    | python -m json.tool > tests/fixtures/openapi.json
pytest tests/test_openapi_contract.py
```

Commit the refreshed snapshot together with any model changes it required, so the
diff shows what moved in the contract and how the SDK adapted.

You can also set `MDC_OPENAPI_SPEC` to a file path or URL to run against a different spec:

```bash
MDC_OPENAPI_SPEC=https://dev.mozilladatacollective.com/api/openapi.json pytest tests/test_openapi_contract.py  # upcoming changes
MDC_OPENAPI_SPEC=../platform/openapi.json pytest tests/test_openapi_contract.py
```

### How drift is detected

The platform is allowed to change its API before the SDK catches up, so nothing here
gates platform merges. Instead:

- **SDK pull requests** run the contract test against the snapshot (`tests.yml`). This
  is the only place the test blocks anything: an SDK change that disagrees with the
  recorded contract.
- **`openapi-contract.yml`** runs daily (and on `workflow_dispatch`, optionally with a
  different `spec_url`). It fetches the production spec and, when it differs from the
  committed snapshot, opens or updates a pull request on the
  `chore/refresh-openapi-snapshot` branch with the new file. The PR body carries the
  contract test result:
    - green: the contract change is compatible with the SDK models; review and merge.
    - red: the platform contract moved. Check out the branch, adapt the models until the
      test passes, and push to the same branch.

  The job needs the repository setting *Allow GitHub Actions to create and approve pull
  requests*. By default it uses the workflow's own token, and GitHub does not run
  `pull_request` workflows for PRs created that way, so the CI checks on the PR stay
  empty until someone pushes to the branch. Requires a fine-grained personal access token with
  `contents: write` and `pull-requests: write` as the `OPENAPI_REFRESH_TOKEN` secret to
  get the normal checks on the PR as well.

## Related workflows

- For release steps, see [Release Workflow](release.md).
- For loader and schema extension work, see [Extending the Loading Logic](extend_schema_loading_logic.md)
  and [Adding a New Schema](add_new_schema.md).

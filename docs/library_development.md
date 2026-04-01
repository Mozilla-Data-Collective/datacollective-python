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
- The cutover compatibility shim rewrites the legacy production host
  `https://datacollective.mozillafoundation.org/api` to
  `https://mozilladatacollective.com/api`.
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

## Related workflows

- For release steps, see [Release Workflow](release.md).
- For loader and schema extension work, see [Extending the Loading Logic](extend_schema_loading_logic.md)
  and [Adding a New Schema](add_new_schema.md).

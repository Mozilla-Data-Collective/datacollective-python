# Mozilla Data Collective Python API Library

Python library for interfacing with the [Mozilla Data Collective](https://datacollective.mozillafoundation.org/) REST API.

## Pre-requisites

This package uses [uv](https://docs.astral.sh/uv/) to convert our source code into a distribution package. Please download and install `uv` before moving further.

## Installing and Testing Locally

First create a local virtual environment with `uv`

`uv venv`

Activate your virtual environment

`source .venv/bin/activate`

Install the package locally with `uv`

`uv pip install -e .`

Now you can import the package and test functionality. You will need to rerun `uv pip install -e .` again after any edits to the package for those updates to show up.

## Environment Configuration

The DataCollective client supports multiple environments through environment-specific `.env` files. This allows you to easily switch between different API endpoints, API keys, and configurations.

### Setting up Environment Files

1. **Create your environment file(s):**
   ```bash
   # For production (default)
   cp .env.example .env
   
   # For development
   cp .env.example .env.development
   
   # For staging
   cp .env.example .env.staging
   ```

   **Note:** If you don't have a `.env.example` file yet, create one with the following template:
   ```bash
   # MDC API Configuration
   MDC_API_KEY=your-api-key-here
   MDC_API_URL=https://datacollective.mozillafoundation.org/api
   MDC_DOWNLOAD_PATH=~/.mozdata/datasets
   ENVIRONMENT=production
   ```

2. **Configure your environment variables:**
   
   Edit your `.env` file (or environment-specific file) with your configuration:
   ```bash
   # Required: Your MDC API key
   MDC_API_KEY=your-api-key-here
   
   # Optional: API endpoint (defaults to production)
   MDC_API_URL=https://datacollective.mozillafoundation.org/api
   
   # Optional: Download path for datasets (defaults to ~/.mozdata/datasets)
   MDC_DOWNLOAD_PATH=~/.mozdata/datasets
   
   # Optional: Environment name (used for .env file selection)
   ENVIRONMENT=production
   ```

### Using Different Environments

The client automatically loads the appropriate `.env` file based on the environment:

- **Production** (default): Uses `.env`
- **Development**: Uses `.env.development` 
- **Staging**: Uses `.env.staging`
- **Custom**: Uses `.env.{environment_name}`

#### Example Usage

```python
from datacollective import DataCollective

# Use production environment (loads .env)
client = DataCollective()

# Use development environment (loads .env.development)
client = DataCollective(environment='development')

# Use staging environment (loads .env.staging)
client = DataCollective(environment='staging')

# Use custom environment (loads .env.custom)
client = DataCollective(environment='custom')
```

#### Testing Your Configuration

Test that your API key and configuration are being loaded correctly:

```python
>>> from datacollective import DataCollective
>>> client = DataCollective()
>>> client.api_key
'your-api-key-here'
>>> client.api_url
'https://datacollective.mozillafoundation.org/api'
>>> client.download_path
'/Users/username/.mozdata/datasets'
```

### Environment File Priority

The client loads environment variables in the following order:
1. Environment-specific file (e.g., `.env.development`)
2. Default `.env` file (if environment-specific file doesn't exist)
3. System environment variables (highest priority)

### Best Practices

- **Never commit `.env` files** to version control - they contain sensitive information
- **Always commit `.env.example`** as a template for other developers
- **Use descriptive environment names** (e.g., `development`, `staging`, `production`)
- **Keep environment-specific configurations minimal** - only override what's different
- **Use system environment variables** for CI/CD pipelines and production deployments

Once your done, exit your virtual environment

`deactivate`

## Testing

Tests are run by first installing the dev dependencies

`uv pip install -e ".[dev]"`

and then running tests with pytest

`pytest`

or 

`pytest --cov=datacollective`

## Development

This project uses modern Python development tools for code quality, formatting, and type checking.

### Development Dependencies

Install all development dependencies:

```bash
uv pip install -e ".[dev]"
```

### Code Formatting with Black

This project uses [Black](https://black.readthedocs.io/) for consistent code formatting.

**Format all code:**
```bash
uv run black src/ tests/
```

**Check formatting without making changes:**
```bash
uv run black --check src/ tests/
```

### Linting with Ruff

This project uses [Ruff](https://docs.astral.sh/ruff/) for fast linting and import sorting.

**Lint all code:**
```bash
uv run ruff check src/ tests/
```

**Fix linting issues automatically:**
```bash
uv run ruff check --fix src/ tests/
```

**Format imports:**
```bash
uv run ruff format src/ tests/
```

### Type Checking with MyPy

This project uses [MyPy](https://mypy.readthedocs.io/) for static type checking.

**Type check all code:**
```bash
uv run mypy src/
```

### Pre-commit Hooks

Set up automated formatting and linting on every commit:

```bash
# Install pre-commit hooks
uv run pre-commit install

# Run hooks manually on all files
uv run pre-commit run --all-files

# Run hooks on staged files only
uv run pre-commit run
```

### Development Scripts

Use the convenient development script for common tasks:

```bash
# Format code
uv run python scripts/dev.py format

# Lint code
uv run python scripts/dev.py lint

# Fix linting issues
uv run python scripts/dev.py fix

# Type check
uv run python scripts/dev.py typecheck

# Run tests
uv run python scripts/dev.py test

# Run all checks (format, lint, type check, and test)
uv run python scripts/dev.py all

# Version management
uv run python scripts/dev.py version
uv run python scripts/dev.py bump-patch
uv run python scripts/dev.py bump-minor
uv run python scripts/dev.py bump-major

# Build and publishing (without version bump)
uv run python scripts/dev.py clean
uv run python scripts/dev.py build
uv run python scripts/dev.py publish-test
uv run python scripts/dev.py publish

# Publishing with automatic version bump (recommended)
uv run python scripts/dev.py publish-bump-test  # TestPyPI
uv run python scripts/dev.py publish-bump       # PyPI
```

### Configuration

All tool configurations are defined in `pyproject.toml`:

- **Black**: 88-character line length, Python 3.9+ target
- **Ruff**: Comprehensive linting rules including pycodestyle, pyflakes, isort, and more
- **MyPy**: Strict type checking with proper error handling
- **Pre-commit**: Automated formatting and linting on every commit

### IDE Integration

For the best development experience, configure your IDE to use these tools:

**VS Code** - Add to your `settings.json`:
```json
{
    "python.formatting.provider": "black",
    "python.linting.enabled": true,
    "python.linting.ruffEnabled": true,
    "python.linting.mypyEnabled": true,
    "python.linting.lintOnSave": true,
    "editor.formatOnSave": true
}
```

**PyCharm** - Install the Black and Ruff plugins and configure them to run on save.

## Version Management

This project uses [bump2version](https://github.com/c4urself/bump2version) for automated version management. This tool automatically updates version numbers in all relevant files and creates git commits and tags.

### Installing bump2version

bump2version is included in the dev dependencies. Install it with:

```bash
uv pip install -e ".[dev]"
```

### Automated Publishing Workflow (Recommended)

The easiest way to publish is using the automated workflow that handles version bumping and publishing:

```bash
# For TestPyPI (testing)
uv run python scripts/dev.py publish-bump-test

# For PyPI (production)
uv run python scripts/dev.py publish-bump
```

This will automatically:
1. Bump the patch version (0.0.3 → 0.0.4)
2. Run all quality checks (format, lint, type check, tests)
3. Clean, build, and publish the package
4. Create a git commit and tag

### Manual Version Management

If you prefer more control, you can manage versions manually:

**Patch version** (0.0.1 → 0.0.2) - Bug fixes:
```bash
uv run bump2version patch
```

**Minor version** (0.0.2 → 0.1.0) - New features (backward compatible):
```bash
uv run bump2version minor
```

**Major version** (0.1.0 → 1.0.0) - Breaking changes:
```bash
uv run bump2version major
```

### Semantic Versioning

Follow [semantic versioning](https://semver.org/) principles:

- **MAJOR** (1.0.0): Breaking changes that are not backward compatible
- **MINOR** (0.1.0): New features that are backward compatible
- **PATCH** (0.0.1): Bug fixes that are backward compatible

**Examples:**
- `0.0.1` → `0.0.2`: Fixed a bug in dataset download
- `0.0.2` → `0.1.0`: Added new method for listing datasets
- `0.1.0` → `1.0.0`: Changed API interface (breaking change)

## Building

To create the distribution package, run:

`uv build`

This will create the distribution package in an auto-generated `dist` directory.

## Publishing to PyPI

This section covers how to publish the `datacollective` package to both TestPyPI (for testing) and PyPI (for production releases).

### Prerequisites

Before publishing, ensure you have:

1. **TestPyPI Account**: Create an account at [test.pypi.org](https://test.pypi.org/account/register/)
2. **PyPI Account**: Create an account at [pypi.org](https://pypi.org/account/register/)
3. **API Tokens**: Generate API tokens for both services (recommended over passwords)
4. **uv**: The package uses `uv` for building and publishing

### Setting up API Tokens

1. **TestPyPI Token**:
   - Go to [test.pypi.org/manage/account/token/](https://test.pypi.org/manage/account/token/)
   - Create a new token with scope "Entire account" (for testing)
   - Save the token securely

2. **PyPI Token**:
   - Go to [pypi.org/manage/account/token/](https://pypi.org/manage/account/token/)
   - Create a new token with scope "Entire account" (or limit to specific projects)
   - Save the token securely

### Pre-Publication Checklist

Before publishing, ensure you've completed these steps:

1. **Update Version** (if needed):
   ```bash
   # For bug fixes
   uv run python scripts/dev.py bump-patch
   
   # For new features
   uv run python scripts/dev.py bump-minor
   
   # For breaking changes
   uv run python scripts/dev.py bump-major
   ```

2. **Run Quality Checks**:
   ```bash
   # Run all checks (format, lint, type check, tests)
   uv run python scripts/dev.py all
   ```

3. **Clean Build Artifacts**:
   ```bash
   # Remove old build files to avoid conflicts
   uv run python scripts/dev.py clean
   ```

4. **Build Package**:
   ```bash
   # Build fresh package
   uv run python scripts/dev.py build
   ```

5. **Review Package**:
   ```bash
   # Check what files will be published
   ls -la dist/
   
   # Verify version
   uv run python scripts/dev.py version
   ```

### Publishing to TestPyPI

TestPyPI is a separate instance of PyPI for testing package uploads. Always test here first!

1. **Configure TestPyPI credentials**:
   ```bash
   # Set your TestPyPI token
   export UV_PUBLISH_TOKEN_testpypi="your-testpypi-token-here"
   ```

2. **Publish to TestPyPI** (automated workflow):
   ```bash
   # This will clean, build, and publish in one command
   uv run python scripts/dev.py publish-test
   ```

   Or manually:
   ```bash
   # Clean old build artifacts
   uv run python scripts/dev.py clean
   
   # Build fresh package
   uv run python scripts/dev.py build
   
   # Publish to TestPyPI
   uv publish --index testpypi
   ```

4. **Verify the upload**:
   - Visit [test.pypi.org/project/datacollective/](https://test.pypi.org/project/datacollective/)
   - Check that your package appears correctly

5. **Test installation from TestPyPI**:
   ```bash
   # Create a fresh virtual environment
   uv venv test-env
   source test-env/bin/activate
   
   # Install from TestPyPI
   uv pip install --index-url https://test.pypi.org/simple/ datacollective
   
   # Test the package
   python -c "from datacollective import DataCollective; print('Installation successful!')"
   ```

### Publishing to PyPI

Once you've successfully tested on TestPyPI, you can publish to the main PyPI:

1. **Configure PyPI credentials**:
   ```bash
   # Set your PyPI token
   export UV_PUBLISH_TOKEN_pypi="your-pypi-token-here"
   ```

2. **Publish to PyPI** (automated workflow):
   ```bash
   # This will clean, build, and publish in one command
   uv run python scripts/dev.py publish
   ```

   Or manually:
   ```bash
   # Clean old build artifacts
   uv run python scripts/dev.py clean
   
   # Build fresh package
   uv run python scripts/dev.py build
   
   # Publish to PyPI
   uv publish
   ```

3. **Verify the upload**:
   - Visit [pypi.org/project/datacollective/](https://pypi.org/project/datacollective/)
   - Check that your package appears correctly

4. **Test installation from PyPI**:
   ```bash
   # Create a fresh virtual environment
   uv venv prod-env
   source prod-env/bin/activate
   
   # Install from PyPI
   uv pip install datacollective
   
   # Test the package
   python -c "from datacollective import DataCollective; print('Installation successful!')"
   ```

### Version Management

This project uses automated version management with `bump2version` to ensure version numbers stay synchronized across all files.

#### Automated Version Bumping

Use the development script to bump versions automatically:

```bash
# Show current version
uv run python scripts/dev.py version

# Bump patch version (0.0.1 -> 0.0.2) - for bug fixes
uv run python scripts/dev.py bump-patch

# Bump minor version (0.0.1 -> 0.1.0) - for new features
uv run python scripts/dev.py bump-minor

# Bump major version (0.0.1 -> 1.0.0) - for breaking changes
uv run python scripts/dev.py bump-major
```

These commands will automatically:
- Update version numbers in `pyproject.toml` and `src/datacollective/__init__.py`
- Create a git commit with the version bump
- Create a git tag for the new version

#### Manual Version Management

If you need to update versions manually:

1. **Update version numbers**:
   - `pyproject.toml`: Update the `version` field
   - `src/datacollective/__init__.py`: Update the `__version__` variable

2. **Follow semantic versioning**:
   - `MAJOR.MINOR.PATCH` (e.g., 1.0.0, 1.0.1, 1.1.0, 2.0.0)
   - MAJOR: Breaking changes
   - MINOR: New features (backward compatible)
   - PATCH: Bug fixes (backward compatible)

3. **Create a git tag**:
   ```bash
   git tag v1.0.0
   git push origin v1.0.0
   ```

### Troubleshooting

**Common Issues:**

1. **Package already exists**: PyPI doesn't allow overwriting existing versions. Increment the version number.

2. **Authentication failed**: Verify your API tokens are correct and have the right permissions.

3. **Build errors**: Ensure all dependencies are properly specified in `pyproject.toml`.

4. **TestPyPI vs PyPI**: Remember that TestPyPI and PyPI are separate - you need to upload to both.

**Useful Commands:**

```bash
# Check package metadata
uv build --help

# Validate package before upload
uv publish --dry-run

# Check what files will be included
uv build --help
```

### Security Notes

- **Never commit API tokens** to version control
- **Use environment variables** or secure credential storage
- **Rotate tokens regularly** for security
- **Use scoped tokens** when possible (limit to specific projects)

### Automated Publishing (Optional)

For automated publishing, consider using GitHub Actions with secrets:

```yaml
# .github/workflows/publish.yml
name: Publish to PyPI

on:
  release:
    types: [published]

jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v2
      - name: Publish to PyPI
        run: uv publish
        env:
          UV_PUBLISH_TOKEN_pypi: ${{ secrets.PYPI_API_TOKEN }}
```

This workflow would automatically publish when you create a GitHub release.

# License

This repository is released under [MPL (Mozilla Public License) 2.0](./LICENSE).

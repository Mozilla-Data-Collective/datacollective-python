# Mozilla Data Collective Python API Library

Python library for interfacing with the [Mozilla Data Collective](https://datacollective.mozillafoundation.org/) REST API.

## Installation

Install the package using pip:

```bash
pip install datacollective
```

## Quick Start

1. **Get your API key** from the Mozilla Data Collective dashboard

2. **Set up your environment**:

If you have cloned the repository, you can run the following command:

   ```bash
   # Copy the example environment file
   cp .env.example .env
   ```

Otherwise, copy and paste the following into a file called `.env` in your present working directory.

```bash
MDC_API_KEY=<MDC_API_KEY> # change to your MDC API Key
MDC_API_URL=https://datacollective.mozillafoundation.org/api # change to MDC API URL endpoint
MDC_DOWNLOAD_PATH=~/.mozdata/datasets # change to where you want to download datasets
```

3. **Configure your API key** by editing `.env`:
   ```bash
   # Required: Your MDC API key
   MDC_API_KEY=your-api-key-here
   
   # Optional: Download path for datasets (defaults to ~/.mozdata/datasets)
   MDC_DOWNLOAD_PATH=~/.mozdata/datasets
   ```

4. **Start using the library**:
   ```python
   from datacollective import DataCollective
   
   # Initialize the client
   client = DataCollective()
   
   # Download a dataset
   client.get_dataset('mdc-dataset-id')
   ```

## Configuration

The client loads configuration from environment variables or `.env` files:

- `MDC_API_KEY` - Your Mozilla Data Collective API key (required)
- `MDC_API_URL` - API endpoint (defaults to production)
- `MDC_DOWNLOAD_PATH` - Where to download datasets (defaults to `~/.mozdata/datasets`)

### Environment Files

Create a `.env` file in your project root:

```bash
# MDC API Configuration
MDC_API_KEY=your-api-key-here
MDC_API_URL=https://datacollective.mozillafoundation.org/api
MDC_DOWNLOAD_PATH=~/.mozdata/datasets
```

**Note:** Never commit `.env` files to version control as they contain sensitive information.

## Basic Usage

```python
from datacollective import DataCollective

# Initialize client (loads from .env automatically)
client = DataCollective()

# Verify your configuration
print(f"API URL: {client.api_url}")
print(f"Download path: {client.download_path}")

# Download a dataset
dataset = client.get_dataset('your-dataset-id')
```

## Load and query datasets

**note:** today, this feature only works with Mozilla Common Voice datasets
```
from datacollective import DataCollective

client = DataCollective()

dataset = client.load_dataset("<dataset-id>") # Load dasaset into memory
df = dataset.to_pandas() # Convert to pandas for queryable form
dataset.splits # A list of all splits available in the dataset
```


## Multiple Environments

You can use different environment configurations:

```python
# Production environment (default, uses .env)
client = DataCollective()

# Development environment (uses .env.development)
client = DataCollective(environment='development')

# Staging environment (uses .env.staging)  
client = DataCollective(environment='staging')
```

## License

This project is released under [MPL (Mozilla Public License) 2.0](./LICENSE).

# Contributing to overturemaps-py

## Development Setup

This project uses [uv](https://docs.astral.sh/uv/) for dependency management and packaging.

### Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

### Setup Development Environment

```bash
# Clone the repository
git clone https://github.com/OvertureMaps/overturemaps-py.git
cd overturemaps-py

# Set up development environment with uv (recommended)
uv sync --dev

# Or with pip
pip install -e ".[dev,geopandas,toolkit]"
```

## Development Workflow

### Running Commands

```bash
# Run the CLI without installation (uv automatically handles dependencies)
uv run overturemaps --help
uv run overturemaps download --help
uv run overturemaps releases list

# Or activate the virtual environment
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
overturemaps --help
overturemaps download --help
```

### Running Tests

```bash
# Run all tests except integration tests
uv run pytest -v -m "not integration"

# Run specific test file
uv run pytest tests/test_cli.py -v

# Run with coverage
uv run pytest --cov=overturemaps --cov-report=html
```

### Code Quality

```bash
# Format and check code
uv run ruff format .
uv run ruff check .

# Auto-fix issues
uv run ruff check . --fix
```

### Building and Testing Distribution

```bash
# Build wheel and source distribution
uv build

# Test the built wheel in a clean environment
uv venv test-env
source test-env/bin/activate
uv pip install dist/overturemaps-*.whl
overturemaps --help
overturemaps releases list
deactivate
rm -rf test-env
```

## Project Structure

```
overturemaps/           # Main package
├── __init__.py         # Package initialization
├── cli.py              # Click CLI interface (includes all commands)
├── core.py             # Core download functionality
├── models.py           # Data models and types
├── releases.py         # Release management
├── changelog.py        # GERS changelog processing
├── fetch.py            # Feature fetching from S3
├── state.py            # Pipeline state management
└── backends/           # Storage backends
    ├── __init__.py     # Backend exports
    ├── base.py         # Base backend interface
    ├── geoparquet.py   # GeoParquet backend
    └── postgis.py      # PostGIS backend

tests/                  # Test suite
examples/               # Usage examples
```

## CLI Commands

The package provides a unified CLI with the following command groups:

**Core Commands:**
- `overturemaps download` - Download Overture data (supports streaming or backend modes)
- `overturemaps gers` - Query the GERS ID registry

**Incremental Update Commands:**
- `overturemaps releases` - List and inspect available releases
- `overturemaps changelog` - Query GERS changelog for changes
- `overturemaps update` - Run incremental updates

## Testing

We use pytest with the following markers:

- `integration`: Tests requiring real S3/network access (skipped by default)

To run integration tests:

```bash
uv run pytest -m integration
```

## Release Process

1. Update version in `pyproject.toml`
2. Build and test wheel: `uv build`
3. Create GitHub release with tag
4. Upload to PyPI as needed
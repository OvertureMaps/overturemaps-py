# Tests for overturemaps-py

## Running Tests

```bash
# Install dependencies
poetry install --with dev

# Run all tests
poetry run pytest

# Skip integration tests (no network required)
poetry run pytest -m "not integration"

# Run only integration tests
poetry run pytest -m integration
```

## Test Files

- `test_gers.py` - GERS registry and record batch reader tests (20 tests total)
- `test_releases.py` - Dynamic release fetching tests

## Test GERS ID

- `0b7fc702-49e7-4b35-81cd-a19acefe0696` - Dupont Circle Hotel, Washington DC

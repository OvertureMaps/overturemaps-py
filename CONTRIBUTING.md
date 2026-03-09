# Contributing

## Publishing

### Overview

The package is published to [PyPI](https://pypi.org/project/overturemaps/) via the
[`publish-pypi.yml`](.github/workflows/publish-pypi.yml) GitHub Actions workflow using
[OIDC trusted publishing](https://docs.pypi.org/trusted-publishers/), no API token should be used. This repo, workflow, and GitHub environment have been pre-configured in PyPI and Test PyPI for publishing.

### Releasing a new version

1. Update the version in [`pyproject.toml`](pyproject.toml) (field: `project.version`).
2. Commit and merge to `main`.
3. Create a GitHub Release (tag + title + notes). Publishing the release triggers the workflow automatically.
4. The workflow builds the package and publishes to PyPI in the [`pypi` GitHub environment](https://github.com/OvertureMaps/overturemaps-py/deployments).

### Dry-run / Test PyPI

Trigger the workflow manually via to publish to
[Test PyPI](https://test.pypi.org/project/overturemaps/) instead of production PyPI. This is useful
for verifying the build and publish pipeline end-to-end without affecting the real package index.
The workflow uses `skip-existing: true` for test publishes so version conflicts don't fail the run.

### Environments

| GitHub environment | Target index | Trigger |
|--------------------|--------------|---------|
| `pypi`             | PyPI (production) | GitHub Release published |
| `test-pypi`        | Test PyPI    | Manual `workflow_dispatch` |

Both environments are configured with OIDC trusted publisher entries on their respective package
indexes; no secrets or API tokens are stored in the repository.

## Testing

Tests run on pull requests and pushes to `main` via [`test-run.yml`](.github/workflows/test-run.yml).

To run tests locally:

```bash
uv sync --dev
uv run pytest tests/ -v
```

A CLI smoke test is also included in CI:

```bash
uv run overturemaps download --bbox=-71.068,42.353,-71.058,42.363 -f geojson --type=building -o boston.geojson
```

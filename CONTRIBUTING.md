# Contributing

## Publishing

### Overview

The package is published to [PyPI](https://pypi.org/project/overturemaps/) via the
[`publish-pypi.yml`](.github/workflows/publish-pypi.yml) GitHub Actions workflow using
[OIDC trusted publishing](https://docs.pypi.org/trusted-publishers/), no API token should be used. This repo, workflow, and GitHub environment have been pre-configured in PyPI and Test PyPI for publishing.

### Releasing a new version

Releases run automatically on every push to `main`. Do not edit `project.version` by hand; it is a
`0.0.0` placeholder. The publish and binary workflows stamp the release tag's version into the
build without committing it. The squash-merged PR title drives the next version, so PRs must use
[Conventional Commits](https://www.conventionalcommits.org/):

- `fix:` and `perf:` release a patch; `feat:` releases a minor version.
- Add `!` or a `BREAKING CHANGE:` footer for a major version.
- `docs:`, `chore:`, `ci:`, `test:`, and `build:` do not trigger a release.

This repository uses squash merges so the PR title is the commit message read by the release
workflow. Release notes are published on the GitHub Release; no version commit or `CHANGELOG.md`
is written back to `main`.

Publishing a GitHub Release triggers the following:

- The publish-pypi workflow builds the package and publishes to PyPI in the
  [`pypi` GitHub environment](https://github.com/OvertureMaps/overturemaps-py/deployments).
- The build-binaries workflow builds cross-OS binaries and attaches them to the release.
- `brew`'s BrewTestBot scans PyPI every 3 hours and updates our
  [Homebrew formula](https://github.com/Homebrew/homebrew-core/blob/main/Formula/o/overturemaps.rb).

### Dry-run / Test PyPI

Trigger `publish-pypi.yml` manually to publish to
[Test PyPI](https://test.pypi.org/project/overturemaps/) instead of production PyPI. This is useful
for verifying the build and publish pipeline without affecting the real package index. Manual runs
stamp a unique `0.0.0.dev<run_id><attempt>` version. The workflow uses `skip-existing: true` for test
publishes so version conflicts do not fail the run. Manually trigger `build-binaries.yml` to inspect
binary artifacts with the same development version format without attaching them to a release.

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

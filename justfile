#!/usr/bin/env just --justfile

latest_release := `curl -s https://stac.overturemaps.org | jq -r '.latest'`

@_default:
    {{ just_executable() }} --list

# Install all dependencies (including dev)
[group('setup')]
install:
    uv sync --dev

# Run unit tests only (no network)
[group('test')]
test:
    uv run pytest tests/ -m "not integration"

# Run the full test suite including integration tests
[group('test')]
test-all:
    uv run pytest tests/

# Run a quick CLI smoke test against the Boston bounding box
[group('test')]
smoke-test:
    uv run overturemaps download \
        --bbox=-71.068,42.353,-71.058,42.363 \
        -f geojson \
        --type=building \
        -o /tmp/boston-smoke.geojson
    @echo "Output written to /tmp/boston-smoke.geojson"

# Run a smoke test for a given type (default: building)
[group('test')]
[arg('type', pattern='building|place|segment|connector|locality|locality_area|administrative_boundary|land|land_cover|land_use|water')]
smoke-test-type type='building':
    uv run overturemaps download \
        --bbox=-71.068,42.353,-71.058,42.363 \
        -f geojson \
        --type={{ type }} \
        -o /tmp/boston-{{ type }}.geojson
    @echo "Output written to /tmp/boston-{{ type }}.geojson"

# Show the latest Overture release
[group('release')]
latest:
    @echo {{ latest_release }}

# List all available releases
[group('release')]
releases:
    uv run overturemaps releases list

# Build the package
[group('build')]
build:
    uv build

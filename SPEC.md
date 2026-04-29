# overturemaps-py — Project Specification

**Version**: 0.21.0
**Python**: ≥ 3.10
**License**: MIT
**Maintainers**: Adam Lastowka, Dana Bauer (@overturemaps.org)

---

## 1. Purpose and Scope

`overturemaps` is the official Python CLI and library for the Overture Maps Foundation. It provides:

1. **Direct S3 streaming**: Download Overture feature data (buildings, places, roads, etc.) from S3 without intermediate staging.
2. **Spatial filtering**: Bbox-filtered queries using per-row Parquet metadata to minimize bytes transferred.
3. **Multiple output formats**: GeoJSON, GeoJSONSeq, GeoParquet.
4. **GERS lookup**: Fetch a single feature by its Overture Global Entity Reference System (GERS) UUID.
5. **Changelog queries**: Inspect what changed between releases for a given area.
6. **State tracking**: Sidecar `.state` files to detect when local data is stale.

Everything reads from a single public, anonymous-access S3 bucket (`overturemaps-us-west-2`). No authentication required. No data is written to S3.

---

## 2. Architecture

### 2.1 Layered Design

```
CLI (cli.py)
  └── Core data access (core.py)
        ├── STAC catalog (stac.overturemaps.org)
        ├── S3 dataset (pyarrow S3FileSystem, anonymous)
        └── GERS registry (binary search over manifest)

Writers (writers.py)           ← output format writers + copy() pipeline
Releases (releases.py)       ← thin wrapper over core
Changelog (changelog.py)     ← parallel to core, same S3 bucket
State (state.py)             ← sidecar JSON files
Models (models.py)           ← pure data classes, no I/O
```

### 2.2 Data Access Patterns

**Default path (STAC-accelerated)**:
1. Fetch `https://stac.overturemaps.org/catalog.json` (in-process cached).
2. Download `/{release}/collections.parquet` (column-pruned, spatially filtered).
3. Extract the list of Parquet file paths that intersect the query bbox.
4. Open those files as a `pyarrow.dataset.Dataset` with row-level bbox predicate pushdown.
5. Stream `RecordBatch` objects through writer.

**Fallback path (no STAC)**:
- `--no-stac` flag skips step 2–3; opens the full S3 partition directly.

**GERS path**:
1. Fetch STAC catalog → registry manifest (`[filename, max_id]` sorted list).
2. Binary-search manifest to find the shard file.
3. Open shard; apply `filters=[("id", "=", gers_id)]`.

**Changelog path**:
1. Attempt STAC-based file list (future capability; currently always falls back).
2. Open S3 changelog partition directly with hive partitioning.
3. Apply spatial bbox filter and `change_type != "unchanged"`.

### 2.3 S3 Path Conventions

| Data | S3 Path |
|---|---|
| Feature data | `overturemaps-us-west-2/release/{release}/theme={theme}/type={type}/` |
| GERS registry | `overturemaps-us-west-2/registry/{filename}` |
| Changelog | `overturemaps-us-west-2/changelog/{release}/theme={theme}/type={type}/` |

---

## 3. Data Model

### 3.1 Feature Types and Themes

Exactly 15 feature types across 5 themes:

| Theme | Types |
|---|---|
| `addresses` | `address` |
| `base` | `bathymetry`, `infrastructure`, `land`, `land_cover`, `land_use`, `water` |
| `buildings` | `building`, `building_part` |
| `divisions` | `division`, `division_area`, `division_boundary` |
| `places` | `place` |
| `transportation` | `connector`, `segment` |

### 3.2 `BBox` Dataclass

Fields: `xmin: float`, `ymin: float`, `xmax: float`, `ymax: float`

**Validation invariants** (enforced at CLI boundary):
- `xmin`, `xmax` ∈ [-180, 180]
- `ymin`, `ymax` ∈ [-90, 90]
- `xmin ≤ xmax`
- `ymin ≤ ymax`

Serialization: `as_tuple()`, `as_dict()`, `from_dict()`.

### 3.3 `Backend` StrEnum

Values: `geojson`, `geojsonseq`, `geoparquet`.

Python 3.10 compatible (StrEnum added in 3.11; shim required).

### 3.4 `PipelineState` Dataclass

Persisted as JSON sidecar beside output files.

| Field | Type | Notes |
|---|---|---|
| `last_release` | `str` | e.g. `"2025-04-23.0"` |
| `last_run` | `str` | ISO 8601 datetime |
| `theme` | `str` | Overture theme name |
| `type` | `str` | Overture feature type |
| `bbox` | `BBox \| None` | `None` = global download |
| `backend` | `Backend` | Output format |
| `output` | `str` | **Absolute** output file path |

`output` MUST be stored as an absolute path (resolved at write time via `Path.resolve()`).

---

## 4. Public Python API

Exported from `overturemaps.__init__`:

```python
from overturemaps import record_batch_reader, get_all_overture_types
```

### 4.1 `record_batch_reader`

```python
def record_batch_reader(
    type: str,
    bbox: BBox | tuple | list | None = None,
    release: str | None = None,
    *,
    connect_timeout: int | None = None,
    request_timeout: int | None = None,
    stac: bool = True,
) -> pyarrow.RecordBatchReader | None
```

- Returns a streaming `RecordBatchReader`; returns `None` if no data matches.
- `bbox=None` queries globally (large data warning applies at CLI layer, not here).
- `release=None` resolves to latest release.
- When `stac=True`, uses STAC spatial index to minimize file scanning.

### 4.2 `get_all_overture_types`

```python
def get_all_overture_types() -> list[str]
```

Returns all 15 type keys in deterministic order.

### 4.3 `geodataframe` (optional, not in public API)

```python
def geodataframe(
    type: str,
    bbox: ...,
    release: str | None = None,
    ...
) -> GeoDataFrame
```

Requires `geopandas` extra (`pip install overturemaps[geopandas]`). Raises `ImportError` with install hint if `geopandas` not present.

### 4.4 `geoarrow_schema_adapter`

```python
def geoarrow_schema_adapter(schema: pa.Schema) -> pa.Schema
```

Tags the `geometry` column with `ARROW:extension:name = geoarrow.wkb`. Geometry stored as raw WKB binary in Parquet; this adapter makes it interpretable by downstream GeoArrow consumers (geopandas, GDAL, etc.) without re-serialization.

---

## 5. CLI Specification

Entry point: `overturemaps`

**Naked run (no subcommand)**: Prints a blue ASCII art "Overture Maps" banner (via `pyfiglet`, `slant` font) and version string to stderr, then prints standard help text to stdout.

**Output colorization**: CLI uses `click.secho`/`click.style` for context-appropriate color. Warnings print yellow; errors print red; success indicators print green; release names print cyan; changelog added/modified/deleted counts print green/yellow/red respectively. `colorama` provides Windows ANSI compatibility automatically via Click.

### 5.1 `download`

```
overturemaps download [OPTIONS]
```

| Option | Type | Required | Default | Notes |
|---|---|---|---|---|
| `--bbox` | `xmin,ymin,xmax,ymax` | No | global | Validated by `BboxParamType` |
| `-f` | `geojson\|geojsonseq\|geoparquet` | Yes | — | Output format |
| `-o / --output` | Path | Cond. | stdout | Required for `geoparquet` |
| `-t / --type` | Choice | Yes | — | Must be one of 15 types |
| `-r / --release` | str | No | latest | Validated against available releases |
| `--stac / --no-stac` | flag | No | `--stac` | Use STAC index |
| `--connect_timeout` | int | No | — | S3 connect timeout (seconds) |
| `--request_timeout` | int | No | — | S3 request timeout (seconds) |

**Warnings** (stderr, non-fatal):
- No `--bbox`: warn with estimated download size (~1.2 TB GeoJSON / ~400 GB GeoParquet).
- `--bbox` covers ≥ 1% of Earth's surface (`area_sq_deg ≥ 648`): warn with size estimates.

**Post-download**: Writes `.state` sidecar (see §3.4). Uses resolved absolute path.

**Geometry column**: If multiple geometry columns found in schema, raises `IOError`.

**GeoParquet metadata**: Strips file-level `bbox` from metadata; adds `covering` sub-object when row-level `bbox` column present.

### 5.2 `gers`

```
overturemaps gers [OPTIONS] GERS_ID
```

| Argument/Option | Notes |
|---|---|
| `GERS_ID` | Validated as UUID; normalized to lowercase-with-dashes |
| `-f` | Format (same choices as `download`) |
| `-o / --output` | Output path |
| `--connect_timeout` | S3 timeout |
| `--request_timeout` | S3 timeout |

**Without `-f`**: queries GERS registry, prints feature info to stderr, exits 0.
**With `-f`**: downloads and writes the feature.

### 5.3 `releases`

```
overturemaps releases list
overturemaps releases latest
overturemaps releases check -o FILE
overturemaps releases exists RELEASE
```

| Subcommand | Behavior | Exit codes |
|---|---|---|
| `list` | Print all releases, newest first | 0 |
| `latest` | Print latest release string | 0 |
| `check -o FILE` | Compare `.state` sidecar to latest release | 0 = up-to-date, 1 = stale or no state |
| `exists RELEASE` | Print `true`/`false` | 0 = exists, 1 = not found |

### 5.4 `changelog`

```
overturemaps changelog query [OPTIONS]
overturemaps changelog summary [OPTIONS]
```

**`query`**:
- Requires `--bbox`.
- Requires at least one of `--theme` or `--type`.
- `--theme` expands to all its types.
- Prints per-type added/modified/deleted counts.
- Prints totals when >1 type queried.

**`summary`**:
- No bbox; scans full theme/type.
- Prints grand totals when >1 type in results.

---

## 6. Writer Specification

All writers live in `overturemaps/writers.py`. All writers implement context manager protocol (`__enter__` / `__exit__`).

### 6.0 `BaseGeoJSONWriter`

Abstract base class managing file handle or stream lifecycle. Subclasses implement `write_feature(geom_str, props)` and optionally `finalize()`. Handles WKB → Shapely → GeoJSON geometry conversion and property column filtering (excludes `geometry` and `bbox` columns) in `write_batch()`.

### 6.1 `GeoJSONSeqWriter`

- One `{"type":"Feature","geometry":...,"properties":...}\n` per feature.
- Geometry: WKB → Shapely → `shapely.to_geojson()`.
- Properties: all non-`geometry`, non-`bbox` columns; `None` values excluded.
- Serialization: `orjson` for properties; Shapely's native GeoJSON for geometry.

### 6.2 `GeoJSONWriter`

- Wraps output in `{"type":"FeatureCollection","features":[...]}`
- Features comma-separated; `finalize()` closes the array and object.

### 6.3 GeoParquet writer

- Uses `pyarrow.parquet.ParquetWriter`.
- Strips file-level `bbox` from Parquet metadata.
- Adds `covering` → `bbox` sub-object in geo metadata when row-level `bbox` column present.
- Applies `geoarrow_schema_adapter()` to tag geometry column.

### 6.4 `copy(reader, writer)` contract

- Lives in `writers.py`; imported into `cli.py` for use by the `download` and `gers` commands.
- Iterates reader via `read_next_batch()` until `StopIteration`.
- Skips empty batches (zero rows).
- Reports progress to stderr via `tqdm`.

---

## 7. Changelog Module

### 7.1 `query_changelog_ids`

```python
def query_changelog_ids(
    release: str,
    theme: str,
    type_: str,
    bbox: BBox,
) -> dict[str, set[str]]
```

Returns `{change_type: set[id]}`. Keys present: `"added"`, `"data_changed"`, `"removed"`. `"unchanged"` excluded by filter.

`FileNotFoundError` / "No such file" → returns `{}` (changelog not available for this release/type).

### 7.2 `summarize_changelog`

```python
def summarize_changelog(
    release: str,
    theme: str,
    type_: str,
) -> dict[str, dict[str, dict[str, int]]]
```

Returns `{theme: {type: {change_type: count}}}`. Uses `pyarrow.compute.value_counts()` for efficiency; streams in batches.

### 7.3 STAC Fallback Design

`_get_changelog_files_from_stac()` returns `None` on any exception. Callers treat `None` as "fall back to direct S3 path scan." This design supports future STAC changelog integration without changing caller code.

---

## 8. Release Management

### 8.1 `releases.py` contracts

| Function | Contract |
|---|---|
| `list_releases()` | Non-empty; sorted newest-first |
| `get_latest_release()` | Raises `RuntimeError` if no releases found |
| `release_exists(r)` | Returns `False` on any network exception (never raises) |
| `get_next_release(r)` | Returns `None` if `r` is latest, not found, or any exception |

### 8.2 `ALL_RELEASES` proxy

Lazy proxy on `core.ALL_RELEASES`. Supports `[]`, iteration, `len()`, `repr()`. Backwards compatibility shim — do not expand its interface.

---

## 9. State Management

### 9.1 Sidecar convention

State file path = `{output_path}.state` (e.g. `boston.geojson.state`).

### 9.2 `save_state`

- Creates parent directories if missing.
- Writes JSON with `indent=2`.
- `output` field stored as absolute, resolved path.

### 9.3 `load_state`

- Returns `None` on: missing file, `FileNotFoundError`, `JSONDecodeError`, `KeyError`.
- Never raises. Callers must handle `None`.

---

## 10. Non-Functional Requirements

### 10.1 Performance

- **Streaming**: All data flows as `RecordBatch` streams. No full dataset loaded into memory.
- **STAC index**: Default behavior. Skips file-scanning for non-intersecting Parquet shards.
- **Predicate pushdown**: Row-group level via PyArrow; avoids reading non-matching row groups.
- **Batch tuning**: `use_threads=True`, `batch_readahead=16`, `fragment_readahead=4`.
- **GeoJSON throughput target**: ~10k points in ~31ms, ~10k polygons in ~44ms (Apple M-series baseline; see benchmarks).
- **GERS binary search**: O(log n) over manifest; avoids scanning all registry shards.

### 10.2 Reliability

- **Anonymous S3**: No credentials to rotate, expire, or leak.
- **STAC cache**: In-process only; no disk cache. TTL = process lifetime.
- **Changelog fallback**: STAC stub failure is silent; direct S3 scan always attempted.
- **Empty batch filtering**: Zero-row batches never passed to writers.
- **State load failures**: Always `None`, never exception. Stale state = staleness check reports "update available", not a crash.

### 10.3 Security

- **No credentials stored**: S3 access is always anonymous.
- **No remote code execution**: CLI only writes to local filesystem paths explicitly provided by user.
- **No shell execution** in library code: all S3 I/O via PyArrow; no subprocess calls.
- **GERS UUID normalization**: Input validated as UUID and normalized before use as filter predicate. Prevents injection into PyArrow filter expressions.
- **Bbox validation**: All four float bounds validated before use in network requests or filter expressions.

### 10.4 Compatibility

- **Python**: 3.10, 3.11, 3.12, 3.13 (tested in CI matrix).
- **StrEnum shim**: Python 3.10 compat required; do not use `enum.StrEnum` directly.
- **geopandas**: Optional extra. Library must import and function without it. `HAS_GEOPANDAS` flag gates all geopandas usage. Stub `GeoDataFrame` class prevents `NameError` at import time.

### 10.5 Observability

- **Progress bar**: `tqdm` to stderr during `copy()`. Never to stdout.
- **Warnings**: Large download warnings to stderr. Never suppress them.
- **GERS info**: Registry lookup results printed to stderr (not stdout) when no format specified.

---

## 11. Dependency Constraints

| Package | Min version | Rationale |
|---|---|---|
| `click` | 8.3.0 | CLI framework |
| `colorama` | 0.4.6 | ANSI color compatibility on Windows (used automatically by Click) |
| `orjson` | 3.9.0 | Fast JSON serialization for GeoJSON output |
| `pyfiglet` | 1.0.2 | ASCII art banner on naked CLI invocation |
| `pyarrow` | 15.0.2 | Parquet reading, S3 access, batch streaming |
| `shapely` | 2.1.0 | WKB decoding, GeoJSON conversion |
| `numpy` | 1.26.4 | PyArrow/Shapely interop; transitive dep of geopandas |
| `tqdm` | 4.67.3 | Progress reporting |

**Optional**:
- `geopandas>=1.1.0` — `pip install overturemaps[geopandas]`

**Dev**:
- `pytest>=8.0.0`, `pytest-mock>=3.11.0`, `pytest-benchmark>=5.0.0`

**Binary build** (dependency group `binary`; not installed by default):
- `pyinstaller>=6.0`, `pyinstaller-hooks-contrib>=2024.10`

---

## 12. Testing Requirements

### 12.1 Test categories

| Marker | Scope | Network | CI execution |
|---|---|---|---|
| (none) | Unit | No | `pytest tests/ -m "not integration"` |
| `integration` | Integration | Yes | CLI smoke test step only |
| `slow` | Slow unit/bench | No | On-demand |

### 12.2 Unit test coverage requirements

- `BBox`: all serialization methods, all validation failure modes.
- `Backend`: enum values, string coercion.
- `PipelineState`: round-trip serialize/deserialize with and without bbox.
- `BboxParamType`: valid input; 7 failure modes (wrong count, non-numeric, lon OOB, lat OOB, swapped x, swapped y); error messages must include usage examples.
- `copy()`: all non-empty batches written; empty batches skipped; empty reader handled.
- `download` state: output path stored as absolute resolved path.
- `releases exists`: exit code 0 + "true" on found; exit code 1 + error on not found.
- `save_state` / `load_state`: round-trip; missing file → None; bad JSON → None; creates nested dirs.
- `_bbox_area_sq_deg()`: small box, full Earth.
- Large bbox warning: fires at ≥1% Earth with correct size estimates.
- No bbox warning: fires with correct size estimates.
- `overturemaps.__main__`: importable without side effects; calls `cli()` when run as `__main__`; `python -m overturemaps --help` exits 0.

### 12.3 Integration test coverage requirements

- `list_releases()`: non-empty, sorted newest-first.
- `get_latest_release()`: non-empty, contains `-` and `.`.
- `release_exists()`: True for latest; False for `"invalid-release"`.
- `get_next_release()`: None for latest; returns latest for second-to-latest.
- GERS query: known UUID returns 1-row batch with `id`, `geometry`, `bbox`.
- GERS query: non-existent UUID returns None.
- `query_changelog_ids()`: small bbox returns dict of string sets.
- `query_changelog_ids()`: ocean bbox returns all-empty sets.
- `summarize_changelog()`: theme query returns nested dict with int counts.

### 12.4 Benchmark baselines

- 10k points GeoJSONSeq write: < 100ms (M-series target: ~31ms).
- 10k polygons GeoJSONSeq write: < 150ms (M-series target: ~44ms).
- Benchmarks must not require network access.

---

## 13. CI/CD Requirements

### 13.1 Test workflow

- Triggers: push to `main`, PR to `main`.
- Matrix: Python 3.10, 3.11, 3.12, 3.13.
- Steps: checkout → setup-uv (with cache) → `uv sync --dev` → `pytest tests/ -v` → CLI smoke test.
- Smoke test: Boston buildings download to file, format `geojson`.
- Rollup job `are-we-good` must always run; blocks merge on any matrix failure.
- Concurrency: cancel-in-progress for same workflow+ref.

### 13.2 Publish workflow

- Production trigger: `release: published` GitHub event.
- Test trigger: `workflow_dispatch`.
- Build: `uv build` → artifact `dist/`.
- Publish: `pypa/gh-action-pypi-publish` with OIDC (no stored API tokens).
- `skip-existing: true` for test publishes; `attestations: true` always.
- GitHub environments: `pypi` (production), `test-pypi`.

### 13.3 Binary build workflow

- Trigger: `release: published` GitHub event; `workflow_dispatch` for dry-run (uploads artifacts, does not attach to release).
- Matrix: `ubuntu-latest` (linux-x86_64), `macos-latest` (macos-arm64), `macos-13` (macos-x86_64), `windows-latest` (windows-x86_64).
- Tool: PyInstaller `--onefile` via `uv run pyinstaller`; `--collect-all pyarrow`, `--collect-all shapely`, `--collect-data pyfiglet`.
- Entry point: `overturemaps/__main__.py`.
- Output naming: `overturemaps-{version}-{target}[.exe]`.
- Post-build smoke test: binary invoked with `--help`; must exit 0.
- Release attachment: `gh release upload` with `contents: write` scoped to job level.

### 13.4 Action pinning

- All GitHub Actions pinned to full commit SHAs.
- Dependabot monitors `github-actions` ecosystem weekly.
- `actions/*` updates grouped into single PR.
- PRs labeled `bot`; commit prefix `[CHORE](deps)`.

---

## 14. Code Quality and Best Practices

### 14.1 What to enforce

- **No linter config currently exists** (no ruff, black, mypy, pre-commit). Adding any of these is in-scope for quality improvement.
- **Type annotations**: all public API functions should have full type annotations. Internal helpers may be unannotated where obvious.
- **Docstrings**: not required by convention. Add only where behavior is non-obvious (e.g. STAC fallback design, binary search invariants).
- **No mutable default arguments**.
- **StrEnum shim** must be tested for Python 3.10 behavior.

### 14.2 Error handling principles

- **Validate at CLI boundary**: bbox, release string, GERS UUID, format choice. Library functions trust their callers.
- **Fail fast on invalid type**: `type` not in `type_theme_map` should raise `KeyError` immediately, not silently produce an empty result.
- **No silent swallowing** except: `load_state` (missing/corrupt state is non-fatal by design), `release_exists` (network errors = False), STAC fallback (fallback is the contract).
- **`RuntimeError` for unrecoverable catalog failures** (e.g. no releases found).

### 14.3 Output discipline

- **stdout is data**: GeoJSON/GeoJSONSeq/GeoParquet content goes to stdout when no `-o`. No diagnostic output to stdout ever.
- **stderr is user**: warnings, progress bars, GERS info, error messages all go to stderr.
- **Exit codes**: 0 = success/true/up-to-date; 1 = failure/false/stale.

### 14.4 Backwards compatibility

- `ALL_RELEASES` lazy proxy retained for backwards compatibility. Do not remove.
- `record_batch_reader` and `get_all_overture_types` are the public API contract. Signature changes require a version bump.
- New optional parameters may be added with defaults without bumping major version.

---

## 15. Build and Release

- **Build backend**: `hatchling`
- **Package manager / task runner**: `uv` + `just`
- **Version source**: `pyproject.toml` `version` field (not dynamic)
- **Release cadence**: Tracks Overture Maps data releases (approximately monthly)
- **PyPI package name**: `overturemaps`
- **Standalone binaries**: Built via PyInstaller on release; attached to GitHub Release as assets for linux-x86_64, macos-arm64, macos-x86_64, windows-x86_64. Local build: `just build-binary`.
- **`python -m overturemaps`**: Supported via `overturemaps/__main__.py`; same entry point used by PyInstaller.

Version bump checklist:
1. Update `version` in `pyproject.toml`.
2. Create GitHub release with matching tag.
3. Publish workflow auto-triggers on release event.

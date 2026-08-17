# Performance notes

Benchmarks use synthetic data on Apple M-series hardware.

| Output format | Geometry | Rows   | Time               |
| ------------- | -------- | ------ | ------------------ |
| GeoJSON       | Points   | 10 000 | 31 ms              |
| GeoJSON       | Polygons | 10 000 | 44 ms              |
| GeoParquet    | n/a      | n/a    | network/disk bound |

Run the benchmarks locally with:

```bash
uv sync --group dev
pytest benchmarks/ -v
```

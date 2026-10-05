# Zenodo JSON Dump Structural Inference

This project ingests a large local [dump of Zenodo record JSON files](https://zenodo.org/api/exporter/records-json.tar.gz) (millions
of individual `.json` files in one directory) into a local Iceberg table, and
provides a Marimo notebook for exploring the data and inferring the structure
of the JSON — including nested and highly variable fields like `metadata` and
`custom_fields` — without needing a fixed schema up front.

Each record is landed with its full original JSON preserved (`raw_json`),
plus a handful of flat, commonly-used fields extracted for fast filtering and
aggregation (id, DOI, title, dates, resource type, view/download counts,
etc.).

## Setup

1. **Point the ingest script at your data**

   Open `ingest.py` and adjust `JSON_DIR_PATH` to the directory containing
   your Zenodo JSON dump:

   ```python
   JSON_DIR_PATH = "./json-data"
   ```

2. **Install dependencies**

   ```bash
   uv sync
   ```

3. **Run the ingest**

   ```bash
   uv run python ingest.py
   ```

   This reads the JSON files in chunks, lands each record's raw JSON plus
   extracted top-level fields into a local Iceberg table
   (`default.zenodo_raw`), stored under `./iceberg_warehouse`.

## Exploring the data

Launch the analysis notebook:

```bash
uv run marimo edit query_nb.py
```

This opens an interactive Marimo notebook connected to the Iceberg table,
with cells for:

- Row counts and sample records
- Aggregations over the flat columns (resource type, year, downloads, etc.)
- Pretty-printing full raw JSON for individual records
- Inferring the structure of nested fields (e.g. `metadata`,
  `custom_fields`) across filtered subsets of records, using streamed
  Iceberg scans to stay memory-safe at multi-million-row scale
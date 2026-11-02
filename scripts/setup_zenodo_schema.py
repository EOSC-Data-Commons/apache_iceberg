import marimo

__generated_with = "0.25.2"
app = marimo.App()


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
 
    """)
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Setup Apache Iceberg schemas for Zenodo

    Planned setup:

    Table 1: all files  = **aggregator_records**
    Columns:
    * file
    * id
    * source (Zenodo)
    * resource_type
    * created (date)
    * updated (date)

    Table 2: eligible datasets with full JSON = **records_to_harvest**
    -> includes transformation into Datacite JSON

    Columns:
    * file
    * id
    * source (Zenodo)
    * resource_type
    * created (date)
    * updated (date)
    * raw_json
    * datacite_json
    * ?additional_metadata_json (file metadata)
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    **FOR TESTING**
    Working on the 20k records dump instead of the full, 8-million.
    """)
    return


@app.cell
def _():
    import os, json
    from datetime import date
    from itertools import islice
    from concurrent.futures import ThreadPoolExecutor
    import boto3
    from botocore.config import Config
    import pyarrow as pa
    from pyiceberg.catalog import load_catalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField, StringType, DateType

    return (
        DateType,
        NestedField,
        Schema,
        StringType,
        ThreadPoolExecutor,
        date,
        islice,
        json,
        load_catalog,
        os,
        pa,
    )


@app.cell
def _(load_catalog, os):
    ICEBERG_URL = os.environ["ICEBERG_REST_CATALOG"]

    # S3 info for the actual data
    S3_ENDPOINT = os.environ.get("S3_ENDPOINT")
    S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
    S3_SECRET_KEY = os.environ["S3_SECRET_KEY"]

    catalog = load_catalog("remote", **{
        "type": "rest",
        "uri": ICEBERG_URL,
        "rest.client.max-retries": 5,
        "rest.client.connection-timeout-ms": 30000,  # 30 s to establish the connection
        "s3.endpoint": S3_ENDPOINT,
        "s3.access-key-id": S3_ACCESS_KEY,
        "s3.secret-access-key": S3_SECRET_KEY,
        "py-io-impl" : "pyiceberg.io.fsspec.FsspecFileIO"
    })
    return S3_ACCESS_KEY, S3_ENDPOINT, S3_SECRET_KEY, catalog


@app.cell
def _(DateType, NestedField, Schema, StringType):
    # Table 1 - for all records

    NAMESPACE = "test_harvest_ice"
    TABLE_IDENTIFIER_T1 = f"{NAMESPACE}.aggregator_records"

    schema_T1 = Schema(
        NestedField(field_id=1, name="file", field_type=StringType(), required=True),
        NestedField(field_id=2, name="id", field_type=StringType(), required=True),
        NestedField(field_id=3, name="source", field_type=StringType(), required=True),
        NestedField(field_id=4, name="resource_type", field_type=StringType(), required=False),
        NestedField(field_id=5, name="created", field_type=DateType(), required=False),
        NestedField(field_id=6, name="updated", field_type=DateType(), required=False),
    )
    return NAMESPACE, TABLE_IDENTIFIER_T1, schema_T1


@app.cell
def _(DateType, NAMESPACE, NestedField, Schema, StringType):
    # Table 2 - for records we want to turn into harvest_events

    TABLE_IDENTIFIER_T2 = f"{NAMESPACE}.records_to_harvest"

    schema_T2 = Schema(
        NestedField(field_id=1, name="file", field_type=StringType(), required=True),
        NestedField(field_id=2, name="id", field_type=StringType(), required=True),
        NestedField(field_id=3, name="source", field_type=StringType(), required=True),
        NestedField(field_id=4, name="resource_type", field_type=StringType(), required=False),
        NestedField(field_id=5, name="created", field_type=DateType(), required=False),
        NestedField(field_id=6, name="updated", field_type=DateType(), required=False),
        NestedField(field_id=7, name="raw_json", field_type=StringType(), required=False),
        # raw -> DataCite JSON
        NestedField(field_id=8, name="datacite_json", field_type=StringType(), required=False),
        # raw -> zenodo files schema JSON
        NestedField(field_id=9, name="additional_metadata_json", field_type=StringType(), required=False),
    )
    return TABLE_IDENTIFIER_T2, schema_T2


@app.cell
def _(NAMESPACE, catalog):
    catalog.create_namespace_if_not_exists(NAMESPACE)
    return


@app.cell
def _(TABLE_IDENTIFIER_T1, catalog):
    # Always start from stratch!
    if catalog.table_exists(TABLE_IDENTIFIER_T1):
        catalog.purge_table(TABLE_IDENTIFIER_T1)
    return


@app.cell
def _(TABLE_IDENTIFIER_T1, catalog, schema_T1):
    iceberg_table = catalog.create_table_if_not_exists(TABLE_IDENTIFIER_T1, schema=schema_T1)
    return


@app.cell
def _(TABLE_IDENTIFIER_T2, catalog):
    # Always start from stratch for second table!
    if catalog.table_exists(TABLE_IDENTIFIER_T2):
        catalog.purge_table(TABLE_IDENTIFIER_T2)
    return


@app.cell
def _(TABLE_IDENTIFIER_T2, catalog, schema_T2):
    iceberg_table_2 = catalog.create_table_if_not_exists(TABLE_IDENTIFIER_T2, schema=schema_T2)
    return


@app.cell
def _(NAMESPACE, catalog):
    catalog.list_tables(NAMESPACE)
    return


@app.cell
def _(TABLE_IDENTIFIER_T1, catalog):
    catalog.load_table(TABLE_IDENTIFIER_T1)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Filling tables from S3
    Use data in buckets to fill the slimmer table with all records, then filter and fill the larger table with all JSONs.
    """)
    return


@app.cell
def _():
    from s3_upload import connect_to_s3, iter_s3_keys
    from json_manipulation.json_transform import transform_json_zenodo_to_datacite

    return connect_to_s3, iter_s3_keys, transform_json_zenodo_to_datacite


@app.cell
def _(S3_ACCESS_KEY, S3_ENDPOINT, S3_SECRET_KEY, connect_to_s3):
    s3 = connect_to_s3(
        S3_ACCESS_KEY,
        S3_SECRET_KEY,
        S3_ENDPOINT
    )
    return (s3,)


@app.cell
def _():
    SOURCE_BUCKET = "zenodo20k-2026-10-03" # TESTING!
    SOURCE_PREFIX = "zenodo_dump"
    return SOURCE_BUCKET, SOURCE_PREFIX


@app.cell
def _(schema_T1):
    arrow_schema_T1 = schema_T1.as_arrow()
    return (arrow_schema_T1,)


@app.cell
def _(SOURCE_BUCKET, date, json, s3):
    def to_date(s):
        return date.fromisoformat(s[:10]) if s else None

    def slim_row(key):
        d = json.loads(s3.get_object(Bucket=SOURCE_BUCKET, Key=key)["Body"].read())
        if d.get("id") is None:
            return None  # required column, so skip records without an id
        return {
            "file": key,  # full key, used for resume
            "id": str(d["id"]),
            "source": "Zenodo",
            "resource_type": (d.get("metadata", {}).get("resource_type") or {}).get("id"),
            "created": to_date(d.get("created")),
            "updated": to_date(d.get("updated")),
        }

    return (slim_row,)


@app.cell
def _(SOURCE_BUCKET, SOURCE_PREFIX, islice, iter_s3_keys, s3, slim_row):
    # Test run on 10 files before real operation
    test_keys = list(islice(iter_s3_keys(s3, SOURCE_BUCKET, SOURCE_PREFIX), 10))
    test_rows = [slim_row(k) for k in test_keys]
    print(test_rows[0])
    print(sum(r is None for r in test_rows), "skipped")
    return


@app.cell
def _(
    SOURCE_BUCKET,
    SOURCE_PREFIX,
    TABLE_IDENTIFIER_T1,
    ThreadPoolExecutor,
    arrow_schema_T1,
    catalog,
    islice,
    iter_s3_keys,
    os,
    pa,
    s3,
    slim_row,
):
    CHECKPOINT_PATH = "t1_checkpoint.txt"
    SLICE = 10_000

    def load_checkpoint():
        try:
            return open(CHECKPOINT_PATH, encoding="utf-8").read().strip()
        except FileNotFoundError:
            return ""

    def save_checkpoint(key):
        with open(CHECKPOINT_PATH + ".tmp", "w", encoding="utf-8") as f:
            f.write(key)
        os.replace(CHECKPOINT_PATH + ".tmp", CHECKPOINT_PATH)

    last_key = load_checkpoint()
    print(f"Resuming after {last_key!r}" if last_key else "Starting fresh")

    keys = iter_s3_keys(s3, SOURCE_BUCKET, SOURCE_PREFIX, start_after=last_key)
    total = 0
    skipped = 0

    table_T1 = catalog.load_table(TABLE_IDENTIFIER_T1)

    with ThreadPoolExecutor(max_workers=32) as ex:
        while True:
            batch = list(islice(keys, SLICE))
            if not batch:
                break
            results = list(ex.map(slim_row, batch))
            rows = [r for r in results if r is not None]
            skipped += len(results) - len(rows)
            if rows:
                table_T1.append(pa.Table.from_pylist(rows, schema=arrow_schema_T1))
            save_checkpoint(batch[-1])  # only after a successful commit
            total += len(rows)
            print(f"Committed {total:,} rows ({skipped:,} skipped), last key {batch[-1]}")

    # Reached only if every slice committed
    if os.path.exists(CHECKPOINT_PATH):
        os.remove(CHECKPOINT_PATH)

    upload_total = total  # rows committed in this run
    return (upload_total,)


@app.cell
def _(TABLE_IDENTIFIER_T1, catalog, upload_total):
    print(upload_total)
    table_T1_test = catalog.load_table(TABLE_IDENTIFIER_T1)
    print(table_T1_test.scan(selected_fields=("file",)).to_arrow().num_rows)
    return (table_T1_test,)


@app.cell
def _(table_T1_test):
    print(table_T1_test.scan(selected_fields=("file",)).to_arrow())
    return


@app.cell
def _(table_T1_test):
    print(table_T1_test.scan(limit = 10).to_arrow())
    return


@app.cell
def _(TABLE_IDENTIFIER_T1, catalog):
    counts_T1 = catalog.load_table(TABLE_IDENTIFIER_T1).scan(
        selected_fields=("resource_type",)
    ).to_arrow()

    resource_type_counts = (
        counts_T1.group_by("resource_type")
        .aggregate([([], "count_all")])
        .sort_by([("count_all", "descending")])
    )

    for row in resource_type_counts.to_pylist():
        print(row)
    return


@app.cell
def _(TABLE_IDENTIFIER_T1, catalog):
    # 2nd table
    # Filter 1st table rows to only dataset

    src_T1 = catalog.load_table(TABLE_IDENTIFIER_T1)
    t1_records = src_T1.scan(
        row_filter="resource_type = 'dataset'",
        selected_fields=("file", "id", "source", "resource_type", "created", "updated"),
    ).to_arrow()

    dataset_files = sorted(t1_records["file"].to_pylist())   # sorted, so progress is orderly
    print(f"{len(dataset_files):,} dataset records to copy")
    return (dataset_files,)


@app.cell
def _(SOURCE_BUCKET, json, s3, slim_row, transform_json_zenodo_to_datacite):
    def full_row(key):
        raw_text = s3.get_object(Bucket=SOURCE_BUCKET, Key=key)["Body"].read().decode("utf-8")
        base = slim_row(key)   # reuses Table 1's parsing, so the columns match exactly
        if base is None:
            return None
        base["raw_json"] = raw_text   # exact file contents
        base["datacite_json"] = json.dumps(
            transform_json_zenodo_to_datacite(json.loads(raw_text)), ensure_ascii=False
        )
        base["additional_metadata_json"] = None   # left empty
        return base

    return (full_row,)


@app.cell
def _(
    TABLE_IDENTIFIER_T2,
    ThreadPoolExecutor,
    catalog,
    dataset_files,
    full_row,
    pa,
    schema_T2,
    time,
):
    T2_SLICE = 5_000   # smaller than T1, rows are much bigger now

    def append_with_retry(table, arrow_tbl, attempts=4):
        before = len(table.metadata.snapshots)
        for i in range(attempts):
            try:
                table.append(arrow_tbl)
                return
            except Exception as e:
                if i == attempts - 1:
                    raise
                print(f"append failed ({e!r}), retrying in {2 ** i}s")
                time.sleep(2 ** i)
                table.refresh()
                if len(table.metadata.snapshots) > before:
                    print("commit had actually landed, not retrying")
                    return


    table_T2 = catalog.load_table(TABLE_IDENTIFIER_T2)

    copied2 = failed2 = 0
    with ThreadPoolExecutor(max_workers=16) as ex2:
        for i in range(0, len(dataset_files), T2_SLICE):
            batch2 = dataset_files[i:i + T2_SLICE]
            results2 = list(ex2.map(full_row, batch2))
            rows2 = [r for r in results2 if r is not None]
            failed2 += len(results2) - len(rows2)
            if rows2:
                append_with_retry(table_T2, pa.Table.from_pylist(rows2, schema=schema_T2.as_arrow()))
            copied2 += len(rows2)
            print(f"Committed {copied2:,} / {len(dataset_files):,}")

    upload_total_T2 = copied2
    return failed2, upload_total_T2


@app.cell
def _(TABLE_IDENTIFIER_T2, catalog, dataset_files, failed2, upload_total_T2):
    upload_total_T2  # run after the fill

    verify_T2 = catalog.load_table(TABLE_IDENTIFIER_T2)
    n = verify_T2.scan(selected_fields=("file",)).to_arrow().num_rows
    print(n, "rows in T2;", len(dataset_files), "dataset records in T1;", failed2, "skipped")
    return


if __name__ == "__main__":
    app.run()

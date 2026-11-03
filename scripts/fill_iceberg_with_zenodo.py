# Create and fill the Zenodo Iceberg tables from an S3 bucket.

# Table 1 (aggregator_records):  one slim row per JSON file in the bucket.
# Table 2 (records_to_harvest):  only resource_type = 'dataset', with full JSON
#                               and the DataCite transform.

# Both tables are dropped and rebuilt on every run.

# Required env vars: ICEBERG_REST_CATALOG, S3_ACCESS_KEY, S3_SECRET_KEY

# Example:
#    uv run python scripts/fill_iceberg_with_zenodo.py --bucket zenodo20k-2026-10-03

from dotenv import load_dotenv

import os

# These variables need to be set so s3 > iceberg requests pass
os.environ.setdefault("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
os.environ.setdefault("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")

import argparse
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from itertools import islice

import pyarrow as pa
from pyiceberg.catalog import load_catalog

from s3_upload import connect_to_s3, iter_s3_keys
from json_manipulation.json_transform import transform_json_zenodo_to_datacite
from apache_iceberg.iceberg_schemas import SCHEMA_AGGREGATOR_RECORDS, SCHEMA_RECORDS_TO_HARVEST
from apache_iceberg.zenodo_from_s3 import append_with_retry, to_date, read_object, slim_row_from_dict

logger = logging.getLogger(__name__)
# Setup secrets, target bucket, local file to download dump, number of workers
load_dotenv(".env", override=True)

def setup_logging() -> None:
    """
    Log messages from this project at INFO level and above, to the console.

    HTTP and AWS client libraries are kept at WARNING so their messages don't
    drown out our own.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s [%(threadName)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    for noisy in ("boto3", "botocore", "s3transfer", "urllib3", "pyiceberg.io"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

# --------------------------------------------------------------------------
# Setup helpers
# --------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bucket", required=True, help="Source S3 bucket containing the Zenodo JSON files")
    p.add_argument("--prefix", default="zenodo_dump", help="Key prefix inside the bucket (default: %(default)s)")
    p.add_argument("--namespace", default="test_harvest_ice", help="Iceberg namespace (default: %(default)s)")
    p.add_argument("--t1-slice", type=int, default=10_000, help="Files per commit for table 1 (default: %(default)s)")
    p.add_argument("--t2-slice", type=int, default=5_000, help="Records per commit for table 2 (default: %(default)s)")
    p.add_argument("--workers", type=int, default=32, help="Parallel S3 reads (default: %(default)s)")
    return p.parse_args()

def build_catalog():
    props = {
        "type": "rest",
        "uri": os.environ["ICEBERG_REST_CATALOG"],
        "rest.client.max-retries": 5,
        "rest.client.connection-timeout-ms": 30000,
        "s3.endpoint": os.environ["S3_ENDPOINT"],
        "s3.access-key-id": os.environ["S3_ACCESS_KEY"],
        "s3.secret-access-key": os.environ["S3_SECRET_KEY"],
        "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
    }
    return load_catalog("remote", **props)


def recreate_table(catalog, identifier, schema):
    """Drop (with data) and recreate, so the table always starts empty."""
    if catalog.table_exists(identifier):
        catalog.purge_table(identifier)
    return catalog.create_table(identifier, schema=schema)

# Table 1 - all records

def fill_t1(catalog, s3, identifier, bucket, prefix, slice_size, workers):
    arrow_schema = SCHEMA_AGGREGATOR_RECORDS.as_arrow()
    table = catalog.load_table(identifier)

    # Turn JSON from S3 into a dict for appending,
    # To be done in parallel with multiple workers
    def build(key):
        return slim_row_from_dict(key, json.loads(read_object(s3, bucket, key)))

    keys = iter_s3_keys(s3, bucket, prefix)

    listed = total = skipped = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        while True:
            batch = list(islice(keys, slice_size))
            if not batch:
                break
            listed += len(batch)
            results = list(ex.map(build, batch))
            rows = [r for r in results if r is not None]
            skipped += len(results) - len(rows)
            if rows:
                append_with_retry(table, pa.Table.from_pylist(rows, schema=arrow_schema))
            total += len(rows)
            logger.info("T1: committed %s rows (%s skipped), last key %s", f"{total:,}", f"{skipped:,}", batch[-1])

    assert listed == total + skipped, "some listed files were not accounted for"

    n = catalog.load_table(identifier).scan(selected_fields=("file",)).to_arrow().num_rows
    logger.info("T1 done: %s listed, %s uploaded, %s skipped; table has %s rows", f"{listed:,}", f"{total:,}", f"{skipped:,}", f"{n:,}")
    if n != total:
        logger.warning("T1 row count (%s) differs from rows uploaded this run (%s)", n, total)
    return total


# --------------------------------------------------------------------------
# Table 2
# --------------------------------------------------------------------------

def select_dataset_files(catalog, identifier):
    src = catalog.load_table(identifier)  # get latest info, after the initial fill is complete
    t = src.scan(
        row_filter="resource_type = 'dataset'",
        selected_fields=("file",),
    ).to_arrow()
    files = sorted(t["file"].to_pylist())
    logger.info("%s dataset records to copy", f"{len(files):,}")
    return files


def fill_t2(catalog, s3, identifier, bucket, dataset_files, slice_size, workers):
    arrow_schema = SCHEMA_RECORDS_TO_HARVEST.as_arrow()
    table = catalog.load_table(identifier)

    def build(key):
        try:
            raw_text = read_object(s3, bucket, key)
            d = json.loads(raw_text)
            row = slim_row_from_dict(key, d)
            if row is None:
                return None
            row["raw_json"] = raw_text  # exact file contents
            row["datacite_json"] = transform_json_zenodo_to_datacite(d)
            row["additional_metadata_json"] = None  # left empty
            return row
        except Exception:
            logger.exception("T2: failed to build row for %s, skipping", key)
            return None

    copied = failed = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for i in range(0, len(dataset_files), slice_size):
            batch = dataset_files[i:i + slice_size]
            results = list(ex.map(build, batch))
            rows = [r for r in results if r is not None]
            failed += len(results) - len(rows)
            if rows:
                append_with_retry(table, pa.Table.from_pylist(rows, schema=arrow_schema))
            copied += len(rows)
            logger.info("T2: committed %s / %s", f"{copied:,}", f"{len(dataset_files):,}")

    n = catalog.load_table(identifier).scan(selected_fields=("file",)).to_arrow().num_rows
    logger.info("T2 done: %s rows in table; %s dataset records in table aggregator_records; %s skipped", f"{n:,}", f"{len(dataset_files):,}", f"{failed:,}")
    if n != copied:
        logger.warning("T2 row count (%s) differs from rows uploaded this run (%s)", n, copied)
    return copied, failed

def main():
    setup_logging()
    args = parse_args()

    t1_id = f"{args.namespace}.aggregator_records"
    t2_id = f"{args.namespace}.records_to_harvest"

    catalog = build_catalog()
    s3 = connect_to_s3(
        os.environ["S3_ACCESS_KEY"],
        os.environ["S3_SECRET_KEY"],
        os.environ["S3_ENDPOINT"],
        max_pool_connections=args.workers
    )

    logger.info("Source: s3://%s/%s", args.bucket, args.prefix)
    catalog.create_namespace_if_not_exists(args.namespace)

    # Table 1: aggregator_records -- all records
    recreate_table(catalog, t1_id, SCHEMA_AGGREGATOR_RECORDS)
    fill_t1(
        catalog, s3, t1_id,
        bucket=args.bucket,
        prefix=args.prefix,
        slice_size=args.t1_slice,
        workers=args.workers,
    )

    # Table 2: records_to_harvest -- datasets only, with full JSON
    recreate_table(catalog, t2_id, SCHEMA_RECORDS_TO_HARVEST)
    dataset_files = select_dataset_files(catalog, t1_id)
    fill_t2(
        catalog, s3, t2_id,
        bucket=args.bucket,
        dataset_files=dataset_files,
        slice_size=args.t2_slice,
        workers=args.workers,
    )


if __name__ == "__main__":
    main()

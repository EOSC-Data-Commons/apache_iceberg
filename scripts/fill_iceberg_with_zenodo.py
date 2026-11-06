"""Create and fill the Zenodo Iceberg tables from an S3 bucket.

Table 1 (``aggregator_records``): one row per JSON file in the bucket with id, 
resource type, dates from the record.
Table 2 (``records_to_harvest``): subset of only ``resource_type = 'dataset'`` records,
with the full JSON and its DataCite transform.

Both tables are dropped and rebuilt on every run.

The tables are created in the provided --namespace.

Example:
    uv run python scripts/fill_iceberg_with_zenodo.py --bucket zenodo20k-2026-10-03 --namespace test_harvest_ice
"""

import os

from dotenv import load_dotenv

# These variables need to be set so s3 > iceberg requests pass
os.environ.setdefault("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
os.environ.setdefault("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")

import argparse
import logging

from apache_iceberg.iceberg_schemas import (
    SCHEMA_AGGREGATOR_RECORDS,
    SCHEMA_RECORDS_TO_HARVEST,
)
from apache_iceberg.zenodo_from_s3 import (
    build_catalog,
    fill_aggregator_records,
    fill_records_to_harvest,
    recreate_table,
    select_dataset_files,
)
from s3_upload import connect_to_s3

logger = logging.getLogger(__name__)

# Load env vars (S3/Iceberg credentials) before connecting to anything
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

def parse_args() -> argparse.Namespace:
    """Define command-line arguments.
    
    --bucket and --namespace are mandatory.
    """
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bucket", required=True, help="Source S3 bucket containing the Zenodo JSON files")
    p.add_argument("--namespace", required=True, help="Iceberg namespace where the tables will live")
    p.add_argument("--prefix", default="zenodo_dump", help="Key prefix inside the bucket (default: %(default)s)")
    p.add_argument("--t1-slice", type=int, default=10_000, help="Files per commit for table 1 (default: %(default)s)")
    p.add_argument("--t2-slice", type=int, default=5_000, help="Records per commit for table 2 (default: %(default)s)")
    p.add_argument("--workers", type=int, default=32, help="Parallel S3 reads (default: %(default)s)")
    return p.parse_args()

def main():
    """Connect to S3 and the catalog, then rebuild and fill both tables."""
    setup_logging()
    args = parse_args()

    # Full table names, including namespace
    t1_id = f"{args.namespace}.aggregator_records"
    t2_id = f"{args.namespace}.records_to_harvest"

    catalog = build_catalog(
        rest_uri=os.environ["ICEBERG_REST_CATALOG"],
        s3_endpoint=os.environ["S3_ENDPOINT"],
        access_key=os.environ["S3_ACCESS_KEY"],
        secret_key=os.environ["S3_SECRET_KEY"],
    )
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
    fill_aggregator_records(
        catalog, s3, t1_id,
        bucket=args.bucket,
        prefix=args.prefix,
        slice_size=args.t1_slice,
        workers=args.workers,
    )

    # Table 2: records_to_harvest -- datasets only, with full JSON
    recreate_table(catalog, t2_id, SCHEMA_RECORDS_TO_HARVEST)
    fill_records_to_harvest(
        catalog, s3, t2_id,
        bucket=args.bucket,
        dataset_files=select_dataset_files(catalog, t1_id),
        slice_size=args.t2_slice,
        workers=args.workers,
    )


if __name__ == "__main__":
    main()

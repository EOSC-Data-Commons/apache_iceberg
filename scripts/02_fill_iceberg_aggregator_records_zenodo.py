"""Create and fill the aggregator_records Iceberg table from an S3 bucket.

One slim row (id, source, resource type and dates) per ``*.json`` object
under ``s3://{bucket}/{prefix}``, so later steps can select which records
to harvest -- script 03 fills ``records_to_harvest`` from the dataset
records selected here.

The table is dropped and rebuilt on every run.

The table is created in the provided --namespace.

Example:
    uv run python scripts/02_fill_iceberg_aggregator_records_zenodo.py --bucket zenodo20k-2026-10-03 --namespace test_harvest_ice
"""

import os

from dotenv import load_dotenv

# These variables need to be set so s3 > iceberg requests pass
os.environ.setdefault("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
os.environ.setdefault("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")

import argparse
import logging

from apache_iceberg.iceberg_schemas import SCHEMA_AGGREGATOR_RECORDS
from apache_iceberg.zenodo_from_s3 import (
    build_catalog,
    fill_aggregator_records,
    recreate_table,
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
    p.add_argument("--namespace", required=True, help="Iceberg namespace where the table will live")
    p.add_argument("--prefix", default="zenodo_dump", help="Key prefix inside the bucket (default: %(default)s)")
    p.add_argument("--slice", type=int, default=10000, help="Rows per commit (default: %(default)s)")
    p.add_argument("--workers", type=int, default=32, help="Parallel S3 reads (default: %(default)s)")
    return p.parse_args()

def main():
    """Connect to S3 and the catalog, then rebuild and fill the aggregator records table."""
    setup_logging()
    args = parse_args()

    # Full table name, including namespace
    identifier = f"{args.namespace}.aggregator_records"

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

    recreate_table(catalog, identifier, SCHEMA_AGGREGATOR_RECORDS)
    fill_aggregator_records(
        catalog, s3, identifier,
        bucket=args.bucket,
        prefix=args.prefix,
        slice_size=args.slice,
        workers=args.workers,
    )


if __name__ == "__main__":
    main()
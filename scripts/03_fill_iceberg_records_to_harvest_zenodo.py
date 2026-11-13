"""Create and fill the records_to_harvest Iceberg table from the dataset records.

The ``resource_type = 'dataset'`` records are selected from the
``aggregator_records`` table (filled by script 02), read from S3 again and
stored as full rows: the slim columns, the exact record text and its
DataCite and additional_metadata transforms.

The table is dropped and rebuilt on every run.

Script 02 must run first -- the S3 keys to copy are the ``file`` column
of the filled ``aggregator_records`` table in the same --namespace.

Example:
    uv run python scripts/03_fill_iceberg_records_to_harvest_zenodo.py --bucket zenodo20k-2026-10-03 --namespace test_harvest_ice
"""

import os

from dotenv import load_dotenv

# These variables need to be set so s3 > iceberg requests pass
os.environ.setdefault("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
os.environ.setdefault("AWS_RESPONSE_CHECKSUM_VALIDATION", "when_required")

import argparse
import logging

from apache_iceberg.iceberg_schemas import SCHEMA_RECORDS_TO_HARVEST
from apache_iceberg.zenodo_from_s3 import (
    build_catalog,
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
    p.add_argument("--namespace", required=True, help="Iceberg namespace where the tables live")
    p.add_argument("--slice", type=int, default=5000, help="Rows per commit (default: %(default)s)")
    p.add_argument("--workers", type=int, default=32, help="Parallel S3 reads (default: %(default)s)")
    return p.parse_args()

def main():
    """Connect to S3 and the catalog, then rebuild and fill the table."""
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

    logger.info("Source: s3://%s", args.bucket)

    # Select the dataset records before recreating table
    # So a possible error doesn't empty the table.
    dataset_files = select_dataset_files(catalog, t1_id)

    recreate_table(catalog, t2_id, SCHEMA_RECORDS_TO_HARVEST)
    fill_records_to_harvest(
        catalog, s3, t2_id,
        bucket=args.bucket,
        dataset_files=dataset_files,
        slice_size=args.slice,
        workers=args.workers,
    )


if __name__ == "__main__":
    main()
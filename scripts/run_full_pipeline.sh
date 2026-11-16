# Run the whole pipeline, downloading the latest Zenodo dump and recreating the Iceberg tables

# S3 bucket to be created with the files from the dump. The records will live in <bucket>/zenodo_dump
BUCKET="zenodo_dump"
# Iceberg namespace where the tables will be recreated and filled.
# If the tables already exist, they will be dropped!
NAMESPACE="harvest-iceberg"

uv run scripts/01_zenodo_dump_to_s3.py --bucket "$BUCKET"
uv run scripts/02_fill_iceberg_aggregator_records_zenodo.py --bucket "$BUCKET" --namespace "$NAMESPACE"
uv run scripts/03_fill_iceberg_records_to_harvest_zenodo.py --bucket "$BUCKET" --namespace "$NAMESPACE"

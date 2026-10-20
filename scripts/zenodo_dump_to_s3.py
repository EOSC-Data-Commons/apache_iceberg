# Download latest Zenodo dump and push it to S3 bucket

# The Zenodo download takes ~13 GB on disk! The file is needed for streaming records
# to S3, after which it can be deleted. The download can last a few minutes.

# Uploading is done in parallel with multiple workers - can take multiple hours locally
# to process all records!

# --bucket Mandatory! S3 bucket to upload the dump to.
# --local-file defines the filepath of the .tar.gz to download. Start at project root.
# --cleanup-local-dump can be set to "yes" to delete the downloaded .tar.gz after the script finishes


import argparse
import os
from dotenv import load_dotenv
import tarfile
from concurrent.futures import ThreadPoolExecutor
import threading
from botocore.exceptions import ClientError
import requests
from datetime import datetime

from s3_upload import UploadStats, connect_to_s3, download_resumable, md5_of, upload

# Setup secrets, target bucket, local file to download dump, number of workers

load_dotenv("../.env", override=True)

ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
SECRET_KEY = os.getenv("S3_SECRET_KEY")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")


def parse_args() -> argparse.Namespace:
    """
    Define command-line arguments.
    The only mandatory is --bucket, the S3 bucket to create/upload to.
    """
    parser = argparse.ArgumentParser(
        description="Download the newest Zenodo dump and upload it to S3."
    )
    parser.add_argument("--bucket", required=True, help="destination S3 bucket")
    parser.add_argument(
        "--local-file",
        default="records-json.tar.gz",
        help="file path where the downloaded dump will be stored",
    )
    parser.add_argument(
        "--cleanup-local-dump",
        choices=["yes", "no"],
        default="no",
        help="yes/no: delete the local dump after a successful upload",
    )
    return parser.parse_args()


def main(bucket: str, local_file: str, cleanup_local_dump: str):
    print("Script " + __file__ + " started.")

    BUCKET = bucket
    PREFIX = "zenodo_dump/"

    # We'll first download the file locally, then stream it to S3. Otherwise direct streaming fails
    LOCAL_FILE = local_file

    # Stream to s3 in parallel
    WORKERS = 16

    # Headers identifying us as the requester
    HEADERS = {
        "User-Agent": f"zenodo-iceberg-loader(v0.1) / {os.environ['REQUESTER_EMAIL']}",
    }

    s3_client = connect_to_s3(ACCESS_KEY, SECRET_KEY, S3_ENDPOINT)

    # Create bucket if it doesn't already exist

    try:
        s3_client.head_bucket(Bucket=BUCKET)
        print(f"Bucket {BUCKET} already exists.")
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchBucket"):
            s3_client.create_bucket(Bucket=BUCKET)
            print(f"Bucket {BUCKET} created.")
        else:
            raise

    # Zenodo dump ---
    # Download the latest version of the JSON metadata dump

    resp = requests.get("https://zenodo.org/api/exporter", timeout=30, headers=HEADERS)
    resp.raise_for_status()

    # The JSON version of full metadata dump
    versions = resp.json()["records-json.tar.gz"]
    newest = max(versions, key=lambda v: datetime.fromisoformat(v["created"]))

    zenodo_metadata_url = newest["links"]["self"]  # URL to download the dump
    zenodo_metadata_date = newest["created"]

    # Expected hash for validation, as well as expectedfilesize
    EXPECTED_MD5 = newest["checksum"].removeprefix("md5:")
    EXPECTED_SIZE = int(newest["size"])

    print("Latest metadata dump: ")
    print(zenodo_metadata_url, zenodo_metadata_date)

    # Download file locally
    # Lengthy!
    download_resumable(zenodo_metadata_url, LOCAL_FILE, EXPECTED_SIZE, HEADERS)
    actual_md5 = md5_of(LOCAL_FILE)

    # Checksum matches expected?
    # if corrupt file: remove it so the next run starts clean
    if actual_md5 != EXPECTED_MD5:
        os.remove(LOCAL_FILE)
        raise RuntimeError(f"checksum mismatch: {actual_md5} != {EXPECTED_MD5}")
    print("Download complete and verified.")

    # Setup threading pool to upload to s3 in parallel

    slots = threading.BoundedSemaphore(WORKERS * 4)
    stats = UploadStats()

    print("Starting upload to S3.")

    with ThreadPoolExecutor(WORKERS) as pool:
        with tarfile.open(LOCAL_FILE, mode="r|gz") as tar:
            for member in tar:
                if not member.isfile():
                    continue
                data = tar.extractfile(member).read()
                slots.acquire()
                pool.submit(
                    upload, s3_client, BUCKET, PREFIX + member.name, data, stats, slots
                )
                # Testing - stop at 20000 files
                current = stats.uploaded
                if current >= 20000:
                    break

    print(f"done: {stats.uploaded} uploaded, {len(stats.errors)} failed")

    # Write down any errors and the files they occurred at in a log
    log_name = BUCKET + "_errors.log"

    stats.log_errors(log_name)

    # Optionally: if indicated, delete the local dump file to free up space
    if cleanup_local_dump == "yes":
        os.remove(local_file)
        print(f"Deleted {local_file}.")

    print("Script " + __file__ + " complete.")


if __name__ == "__main__":
    args = parse_args()
    main(args.bucket, args.local_file, args.cleanup_local_dump)

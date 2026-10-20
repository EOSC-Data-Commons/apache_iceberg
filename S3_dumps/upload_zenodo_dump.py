import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import os
    from dotenv import load_dotenv
    import tarfile
    import hashlib
    from concurrent.futures import ThreadPoolExecutor
    import threading

    from s3_upload import UploadStats, connect_to_s3, download_resumable, md5_of, upload

    return (
        ThreadPoolExecutor,
        UploadStats,
        connect_to_s3,
        download_resumable,
        load_dotenv,
        md5_of,
        os,
        tarfile,
        threading,
        upload,
    )


@app.cell
def _(load_dotenv, os):
    load_dotenv("../.env", override=True)

    ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
    SECRET_KEY = os.getenv("S3_SECRET_KEY")
    S3_ENDPOINT = os.getenv("S3_ENDPOINT")

    BUCKET = "test_zenodo_dump2"
    PREFIX = "zenodo_dump/"

    # We'll first download the file locally, then stream it to S3. Otherwise direct streaming fails
    LOCAL_FILE = "records-json.tar.gz"

    # Stream to s3 in parallel
    WORKERS = 16
    return (
        ACCESS_KEY,
        BUCKET,
        LOCAL_FILE,
        PREFIX,
        S3_ENDPOINT,
        SECRET_KEY,
        WORKERS,
    )


@app.cell
def _(os):
    # Headers identifying us as the requester
    HEADERS = {
        "Authorization": f"Bearer {os.environ['ZENODO_TOKEN']}",
        "User-Agent": f"zenodo-iceberg-loader(v0.1) / {os.environ['REQUESTER_EMAIL']}",
    }
    return (HEADERS,)


@app.cell
def _(ACCESS_KEY, S3_ENDPOINT, SECRET_KEY, connect_to_s3):
    s3_client = connect_to_s3(ACCESS_KEY, SECRET_KEY, S3_ENDPOINT)
    return (s3_client,)


@app.cell
def _(BUCKET, s3_client):
    # Create the bucket if it doesn't already exist
    from botocore.exceptions import ClientError

    try:
        s3_client.head_bucket(Bucket=BUCKET)
        print("Bucket already exists.")
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchBucket"):
            s3_client.create_bucket(Bucket=BUCKET)
        else:
            raise 
    return


@app.cell
def _(HEADERS):
    # Get URL and time of latest Zenodo JSON metadata dump

    import requests
    from datetime import datetime

    resp = requests.get("https://zenodo.org/api/exporter", timeout=30,
                       headers = HEADERS)
    resp.raise_for_status()

    versions = resp.json()["records-json.tar.gz"] # The JSON version of full metadata dump
    newest = max(versions, key=lambda v: datetime.fromisoformat(v["created"]))

    zenodo_metadata_url = newest["links"]["self"] # URL to download the dump
    zenodo_metadata_date = newest["created"]

    # Expected hash for validation, as well as expectedfilesize
    EXPECTED_MD5 = newest["checksum"].removeprefix("md5:")
    EXPECTED_SIZE = int(newest["size"])

    print(zenodo_metadata_url, zenodo_metadata_date)
    return EXPECTED_MD5, EXPECTED_SIZE, zenodo_metadata_url


@app.cell
def _(
    EXPECTED_MD5,
    EXPECTED_SIZE,
    HEADERS,
    LOCAL_FILE,
    download_resumable,
    md5_of,
    os,
    zenodo_metadata_url,
):
    # Download file locally
    download_resumable(zenodo_metadata_url, LOCAL_FILE, EXPECTED_SIZE, HEADERS)
    actual = md5_of(LOCAL_FILE)
    if actual != EXPECTED_MD5:
        os.remove(LOCAL_FILE)   # corrupt: remove so the next run starts clean
        raise RuntimeError(f"checksum mismatch: {actual} != {EXPECTED_MD5}")
    print("download complete and verified")
    return


@app.cell
def _(WORKERS, threading):
    slots = threading.BoundedSemaphore(WORKERS * 4)
    errors, uploaded, lock = [], 0, threading.Lock()
    return (slots,)


@app.cell
def _(UploadStats):
    stats = UploadStats()
    return (stats,)


@app.cell
def _(
    BUCKET,
    LOCAL_FILE,
    PREFIX,
    ThreadPoolExecutor,
    WORKERS,
    s3_client,
    slots,
    stats,
    tarfile,
    upload,
):
    with ThreadPoolExecutor(WORKERS) as pool:
        with tarfile.open(LOCAL_FILE, mode="r|gz") as tar:
            for member in tar:
                if not member.isfile():
                    continue
                data = tar.extractfile(member).read()
                slots.acquire()
                pool.submit(upload, s3_client, BUCKET, PREFIX + member.name, data, stats, slots)
                # Testing - stop at 20000 files
                current = stats.uploaded
                if current >= 20000:
                    break

    print(f"done: {stats.uploaded} uploaded, {len(stats.errors)} failed")
    return


@app.cell
def _(BUCKET, stats):
    log_name = BUCKET + "_errors.log"

    stats.log_errors(log_name)
    return


if __name__ == "__main__":
    app.run()

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

    load_dotenv("../.env", override=True)

    ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
    SECRET_KEY = os.getenv("S3_SECRET_KEY")
    S3_ENDPOINT = os.getenv("S3_ENDPOINT")

    BUCKET = "test_zenodo_dump"
    PREFIX = "zenodo_dump/"

    # We'll first download the file locally, then stream it to S3. Otherwise direct streaming fails
    LOCAL_FILE = "records-json.tar.gz"

    # Stream to s3 in parallel
    WORKERS = 32
    return (
        ACCESS_KEY,
        BUCKET,
        LOCAL_FILE,
        PREFIX,
        S3_ENDPOINT,
        SECRET_KEY,
        ThreadPoolExecutor,
        WORKERS,
        hashlib,
        os,
        tarfile,
        threading,
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
def _():
    """Connect to s3"""

    from functools import cache

    import boto3
    from boto3.session import Session
    from botocore.config import Config


    @cache
    def connect_to_s3(access_key: str, secret_key: str, endpoint: str) -> boto3.client:
        """Connect to S3 with a singleton per credentials"""
        session = Session()
        return session.client(
            service_name="s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=str(endpoint),
            # Don't add checksums unless required,
            # Leaving this out results in errors uploading .tar records
            config=Config(
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required",
            ),
        )

    return (connect_to_s3,)


@app.cell
def _(ACCESS_KEY, S3_ENDPOINT, SECRET_KEY, connect_to_s3):
    s3_client = connect_to_s3(ACCESS_KEY, SECRET_KEY, S3_ENDPOINT)
    return (s3_client,)


@app.cell
def _(s3_client):
    # Create the bucket if it doesn't already exist
    from botocore.exceptions import ClientError

    bucket = "test_zenodo_dump"

    try:
        s3_client.head_bucket(Bucket=bucket)
        print("bucket exists")
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchBucket"):
            s3_client.create_bucket(Bucket=bucket)
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
    return EXPECTED_MD5, EXPECTED_SIZE, requests, zenodo_metadata_url


@app.cell
def _(os, requests, time):
    def download_resumable(url, path, total, headers, chunk=4 * 1024 * 1024):
        """
        Continue from specific byte-number if error occurs
        """
        attempt = 0
        while True:
            have = os.path.getsize(path) if os.path.exists(path) else 0
            if have >= total:
                return
            req_headers = dict(headers)
            if have:
                req_headers["Range"] = f"bytes={have}-"
            try:
                with requests.get(url, headers=req_headers, stream=True, timeout=(10, 120)) as r:
                    r.raise_for_status()
                    if have and r.status_code != 206:
                        print("server ignored Range; restarting from zero")
                        have = 0
                    with open(path, "ab" if have else "wb") as f:
                        # raw stream, no decoding: the file is already a .gz and must stay byte-identical
                        for block in r.raw.stream(chunk, decode_content=False):
                            f.write(block)
                            have += len(block)
                            if have % (256 * 1024 * 1024) < chunk:
                                print(f"{have / 1e9:.2f} / {total / 1e9:.2f} GB")
            except (requests.exceptions.RequestException, OSError) as e:
                attempt += 1
                now = os.path.getsize(path) if os.path.exists(path) else 0
                print(f"interrupted at {now / 1e6:.0f} MB (retry {attempt}): {type(e).__name__}")
                time.sleep(min(60, 2 ** min(attempt, 6)))
                continue
            attempt = 0   # a clean pass resets the counter; loop re-checks size



    return (download_resumable,)


@app.cell
def _(hashlib):
    def md5_of(path):
        h = hashlib.md5()
        with open(path, "rb") as f:
            for block in iter(lambda: f.read(8 * 1024 * 1024), b""):
                h.update(block)
        return h.hexdigest()

    return (md5_of,)


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
def _(threading):
    class UploadStats:
        def __init__(self):
            self.lock = threading.Lock()
            self.uploaded = 0
            self.errors = []

        def ok(self):
            with self.lock:
                self.uploaded += 1
                n = self.uploaded
            if n % 10_000 == 0:
                print(f"{n} files uploaded")

        def fail(self, key, exc):
            with self.lock:
                self.errors.append((key, exc))

    return (UploadStats,)


@app.cell
def _(UploadStats):
    stats = UploadStats()
    return (stats,)


@app.function
def upload(s3_client, bucket, key, data, stats, slots):
    try:
        s3_client.put_object(Bucket=bucket, Key=key, Body=data)
        stats.ok()
    except Exception as e:
        stats.fail(key, e)
    finally:
        slots.release()


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
):
    with ThreadPoolExecutor(WORKERS) as pool:
        with tarfile.open(LOCAL_FILE, mode="r|gz") as tar:
            for member in tar:
                if not member.isfile():
                    continue
                data = tar.extractfile(member).read()
                slots.acquire()
                pool.submit(upload, s3_client, BUCKET, PREFIX + member.name, data, stats, slots)

    print(f"done: {stats.uploaded} uploaded, {len(stats.errors)} failed")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()

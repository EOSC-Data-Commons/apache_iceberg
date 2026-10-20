import threading
from functools import cache
import boto3
from boto3.session import Session
from botocore.config import Config
import os
import requests
import hashlib
import time

# A class to keep tally of successful and unsuccesful uploads,
# with a way to log all errors to a file
class UploadStats:
    def __init__(self):
        self.lock = threading.Lock()
        self.uploaded = 0
        self.errors = []

    def ok(self):
        # Count a record as successfully uploaded
        with self.lock:
            self.uploaded += 1
            n = self.uploaded
        if n % 10_000 == 0:
            print(f"{n} files uploaded")

    def fail(self, key, exc):
        # Log an error and what record it occured on
        with self.lock:
            self.errors.append((key, exc))

    def log_errors(self, file):
        """
        Log all errors into a single file.

        After the upload is complete, this allows for an easy overview of potential errors.
        """
        with self.lock:
            errors = list(self.errors)          # snapshot so it can't change mid-write
        with open(file, "w", encoding="utf-8") as f:
            for key, exc in errors:
                msg = " ".join(str(exc).split())  # collapse newlines so each error is one line
                f.write(f"{key} - {msg}\n")
        print(f"wrote {len(errors)} errors to {file}")

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

def md5_of(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()

def upload(
    s3_client: boto3.client,
    bucket: str,
    key: str,
    data: bytes,
    stats: UploadStats,
    slots: threading.BoundedSemaphore) -> None:
    """Upload one object to S3 and record the outcome.

    Can be run in a worker thread. It never raises exceptions: success or failure is
    recorded on the ``stats`` object, and the ``slots`` semaphore is always released so
    the loop can keep going.

    Args:
        s3_client: boto3 S3 client, shared across workers.
        bucket (str): Name of the destination bucket.
        key (str): Object key to write, e.g. ``"zenodo_dump/1003105.json"``.
        data (bytes): Full object content, already read into memory.
        stats (UploadStats): Results tally ; ``ok()`` is called on
            success and ``fail(key, exc)`` on any exception.
        slots (threading.BoundedSemaphore): Semaphore acquired
            by the caller before submitting this task; released at the end.

    Returns:
        None
    """
    try:
        s3_client.put_object(Bucket=bucket, Key=key, Body=data)
        stats.ok()
    except Exception as e:
        stats.fail(key, e)
    finally:
        slots.release()

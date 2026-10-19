import marimo

__generated_with = "0.24.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import os
    from dotenv import load_dotenv

    load_dotenv("../.env", override=True)

    ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
    SECRET_KEY = os.getenv("S3_SECRET_KEY")
    S3_ENDPOINT = os.getenv("S3_ENDPOINT")
    return ACCESS_KEY, S3_ENDPOINT, SECRET_KEY, os


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
def _(ACCESS_KEY):
    print(ACCESS_KEY)
    return


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
    return (bucket,)


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

    print(zenodo_metadata_url, zenodo_metadata_date)
    return requests, zenodo_metadata_url


@app.cell
def _(HEADERS, bucket, requests, s3_client, zenodo_metadata_url):
    # Upload the Zenodo dump to s3
    # Streaming the contents of the tar.gz record-by-record

    import tarfile

    BUCKET = bucket
    PREFIX = "zenodo_dump/"          # folder-like prefix inside the bucket
    URL = zenodo_metadata_url

    with requests.get(URL, stream=True, timeout=60,
                       headers = HEADERS) as r:
        r.raise_for_status()
        # "r|gz" = read one file at a time
        r.raw.decode_content = True
        with tarfile.open(fileobj=r.raw, mode="r|gz") as tar:
            uploaded = 0
            for member in tar:
                data = tar.extractfile(member).read()
                s3_client.put_object(Bucket=BUCKET, Key=PREFIX + member.name, Body=data)
                uploaded += 1
                if uploaded % 1000 == 0:
                    print(f"{uploaded} files uploaded")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()

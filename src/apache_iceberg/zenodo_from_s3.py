import logging
import time
from datetime import date

logger = logging.getLogger(__name__)

def append_with_retry(table, arrow_tbl, attempts=4):
    """Append, retrying on dropped connections. A commit is a POST, so
    pyiceberg's own retries don't cover it. Checks whether a 'failed' commit
    actually landed before retrying, to avoid appending twice.
    
    table: pyiceberg.table Table()
    arrow_tbl: pa.Table with rows to append and table schema info
    attemps: int, number of retries to attempt
    """
    before = len(table.metadata.snapshots)
    for i in range(attempts):
        try:
            table.append(arrow_tbl)
            return
        except Exception as e:
            if i == attempts - 1:
                raise
            logger.warning("append failed (%r), retrying in %ds", e, 2 ** i)
            time.sleep(2 ** i)
            table.refresh()
            if len(table.metadata.snapshots) > before:
                logger.warning("commit had actually landed, not retrying")
                return

def to_date(s):
    return date.fromisoformat(s[:10]) if s else None

def read_object(s3, bucket, key):
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")         

# Build table rows:

# Table SCHEMA_AGGREGATOR_RECORDS, with no JSON
def slim_row_from_dict(key, d):
    """Table 1 columns from a parsed record, or None if there is no id."""
    if d.get("id") is None:
        return None  # id is a required column
    return {
        "file": key,  # full S3 key
        "id": str(d["id"]),
        "source": "Zenodo",
        "resource_type": (d.get("metadata", {}).get("resource_type") or {}).get("id"),
        "created": to_date(d.get("created")),
        "updated": to_date(d.get("updated")),
    }

   
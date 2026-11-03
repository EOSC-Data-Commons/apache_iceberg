import logging
import time

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
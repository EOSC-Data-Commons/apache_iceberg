import os, glob
import duckdb
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from tqdm import tqdm

JSON_DIR_PATH = "/tmp/zenodo_records"
WAREHOUSE_PATH = os.path.abspath("./iceberg_warehouse")
FILES_PER_CHUNK = 5000
CHUNK_SIZE = 50000

def main():
    os.makedirs(WAREHOUSE_PATH, exist_ok=True)
    catalog = load_catalog("local", **{
        "type": "sql",
        "uri": f"sqlite:///{WAREHOUSE_PATH}/pyiceberg_catalog.db",
        "warehouse": f"file://{WAREHOUSE_PATH}",
    })
    table_identifier = "default.zenodo_raw"
    catalog.create_namespace_if_not_exists("default")
    try:
        catalog.drop_table(table_identifier)
    except Exception:
        pass

    all_files = sorted(glob.glob(f"{JSON_DIR_PATH}/*.json"))
    print(f"Found {len(all_files)} files")

    iceberg_table = None
    con = duckdb.connect()

    file_chunks = [all_files[i:i+FILES_PER_CHUNK] for i in range(0, len(all_files), FILES_PER_CHUNK)]

    for chunk_files in tqdm(file_chunks, desc="File chunks"):
        file_list_sql = "[" + ",".join(f"'{f}'" for f in chunk_files) + "]"
        # read_text: one row per file, columns = filename, content (both VARCHAR)
        # schema is ALWAYS the same shape -> no union_by_name needed, no struct inference
        cursor = con.execute(f"""
            SELECT
                filename,
                content AS raw_json,
                json_extract_string(content, '$.id')                              AS zenodo_id,
                json_extract_string(content, '$.pids.doi.identifier')             AS doi,
                json_extract_string(content, '$.created')                         AS created,
                json_extract_string(content, '$.updated')                        AS updated,
                json_extract_string(content, '$.metadata.title')                  AS title,
                json_extract_string(content, '$.metadata.publication_date')       AS publication_date,
                json_extract_string(content, '$.metadata.resource_type.id')       AS resource_type,
                json_extract_string(content, '$.metadata.description')            AS description,
                json_extract_string(content, '$.parent.id')                      AS parent_id,
                try_cast(json_extract_string(content, '$.files.count') AS INTEGER)      AS file_count,
                try_cast(json_extract_string(content, '$.files.total_bytes') AS BIGINT) AS total_bytes,
                try_cast(json_extract_string(content, '$.stats.this_version.views') AS INTEGER)     AS views,
                try_cast(json_extract_string(content, '$.stats.this_version.downloads') AS INTEGER) AS downloads,
                -- keep whole nested/variable blocks as raw JSON strings for a later pass
                json_extract(content, '$.metadata.creators')::VARCHAR             AS creators_json,
                json_extract(content, '$.metadata.subjects')::VARCHAR             AS subjects_json,
                json_extract(content, '$.metadata.related_identifiers')::VARCHAR  AS related_identifiers_json,
                json_extract(content, '$.custom_fields')::VARCHAR                 AS custom_fields_json,
                json_extract(content, '$.parent.communities')::VARCHAR            AS communities_json
            FROM read_text({file_list_sql})
        """)
        reader = cursor.fetch_record_batch(CHUNK_SIZE)

        for batch in reader:
            arrow_chunk = pa.Table.from_batches([batch])

            if iceberg_table is None:
                iceberg_table = catalog.create_table(
                    identifier=table_identifier,
                    schema=arrow_chunk.schema
                )

            iceberg_table.append(arrow_chunk)

    print("\n🎉 Done. Raw landing table built.")

if __name__ == "__main__":
    main()
import marimo

__generated_with = "0.24.0"
app = marimo.App()


@app.cell
def _():
    import duckdb
    import pyarrow as pa
    from pyiceberg.catalog import load_catalog

    # 1. Connect to your local PyIceberg SQLite catalog
    catalog = load_catalog(
        "local",
        **{
            "type": "sql",
            "uri": "sqlite:///./iceberg_warehouse/pyiceberg_catalog.db",
            "warehouse": "file://./iceberg_warehouse",
        }
    )

    # 2. Load the target table reference
    iceberg_table = catalog.load_table("default.zenodo_raw")

    con = duckdb.connect()
    return con, iceberg_table


@app.cell
def _(iceberg_table):
    # Lean scan for exploratory analysis: skip raw_json / *_json columns to
    # keep the in-memory Arrow table small and fast. Add columns back as needed.
    LEAN_COLUMNS = [
        "filename",
        "zenodo_id",
        "doi",
        "created",
        "updated",
        "title",
        "publication_date",
        "resource_type",
        "parent_id",
        "file_count",
        "total_bytes",
        "views",
        "downloads",
        #"raw_json"
    ]

    dataset = iceberg_table.scan(selected_fields=LEAN_COLUMNS).to_arrow()
    return


@app.cell
def _(con):
    print("--- Row count ---")
    con.sql("SELECT COUNT(*) AS total_records FROM dataset;").show()
    return


@app.cell
def _(con):
    print("--- Sample rows ---")
    con.sql("SELECT * FROM dataset LIMIT 5;").show()
    return


@app.cell
def _(con):
    print("--- Records by resource_type ---")
    con.sql("""
        SELECT resource_type, COUNT(*) AS total_records
        FROM dataset
        GROUP BY resource_type
        ORDER BY total_records DESC;
    """).show()
    return


@app.cell
def _():
    from pyiceberg.expressions import EqualTo

    return (EqualTo,)


@app.cell
def _(EqualTo, con, iceberg_table):


    count_reader = iceberg_table.scan(
        row_filter=EqualTo("resource_type", "dataset"),
        selected_fields=["zenodo_id"],
    ).to_arrow_batch_reader()

    con.register("datasets_count_only", count_reader)
    con.sql("SELECT COUNT(*) FROM datasets_count_only;").show()
    return


@app.cell
def _(EqualTo, con, iceberg_table):


    def get_datasets_reader():
        return iceberg_table.scan(
            row_filter=EqualTo("resource_type", "dataset"),
            selected_fields=["zenodo_id", "raw_json"],
        ).to_arrow_batch_reader()

    con.register("datasets_sample_src", get_datasets_reader())

    result = con.sql("""
        SELECT json_structure(json_group_array(json_extract(raw_json, '$.metadata')))
        FROM (SELECT raw_json FROM datasets_sample_src LIMIT 5000000)
    """).fetchone()

    print(result[0])
    return


if __name__ == "__main__":
    app.run()

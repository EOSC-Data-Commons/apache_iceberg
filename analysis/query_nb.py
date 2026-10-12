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
        "raw_json"
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
    # Main result:
    # Fields present in the Zenodo JSON

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


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Transforming Zenodo JSON into uniform harvest_event JSON

    We'll write a script to reformat the JSON we get from Zenodo into a format that's like datacite_json in the metadata warehouse. This will be the main column in the apache iceberg table.
    The same process could be repeated for other aggregator dumps into the same apache table, with different transformation format (handled by different .jq files)
    """)
    return


@app.cell
def _():
    import json
    from jsonschema.validators import validate
    from jsonschema.exceptions import ValidationError
    import jq

    return ValidationError, jq, json, validate


@app.cell
def _(jq):
    # Try a simple transformation that keeps only the title, resource type and creators
    from pathlib import Path
    simple_jq_transform = jq.compile(Path("simple_jq_filter.jq").read_text())
 
    return (simple_jq_transform,)


@app.cell
def _(con):
    zenodo_json = con.sql("""
        SELECT raw_json
        FROM dataset LIMIT 1
    """).fetchone()[0]

    zenodo_json
    return (zenodo_json,)


@app.cell
def _(simple_jq_transform, zenodo_json):
    simple_jq_transform.input_text(zenodo_json).text()
    return


@app.cell
def _(con):
    all_zenodo_jsons = con.sql("""
        SELECT raw_json
        FROM dataset
    """).fetchall()

    all_zenodo_jsons = [item[0] for item in all_zenodo_jsons]

    all_zenodo_jsons[:5]
    return (all_zenodo_jsons,)


@app.cell
def _(all_zenodo_jsons, simple_jq_transform):
    simplified_jsons = []

    for raw_json in all_zenodo_jsons:
        simplified_jsons.append(
            simple_jq_transform.input_text(raw_json).text()
        )
    return (simplified_jsons,)


@app.cell
def _(simplified_jsons):
    simplified_jsons[:5]
    return


@app.cell
def _(json):
    # Load the GOAL schema - datacite format

    with open('../resources/datacite_schema.json') as f:
        datacite_schema = json.load(f)

    print(datacite_schema)
    return (datacite_schema,)


@app.cell
def _(ValidationError, datacite_schema, simplified_jsons, validate):
    try: 
        validate(simplified_jsons[0], datacite_schema)
    except ValidationError:
        print("The simple JSON does not conform to Datacite schema.")
    return


@app.cell
def _(json):
    # Load the Zenodo schema
    # We inferred this via the analysis module

    with open('../resources/zenodo_schema.json') as f2:
        zenodo_schema = json.load(f2)

    print(zenodo_schema)
    return (zenodo_schema,)


@app.cell
def _(all_zenodo_jsons, json):
    example_zenodo = json.loads(all_zenodo_jsons[0])
    example_zenodo['metadata']
    return (example_zenodo,)


@app.cell
def _(example_zenodo, validate, zenodo_schema):
    validate(example_zenodo['metadata'], zenodo_schema)
    print("Validation succesful!")
    return


@app.cell
def _(ValidationError, all_zenodo_jsons, json, validate, zenodo_schema):
    for ix, zenodo_json_el in enumerate(all_zenodo_jsons):
        try:
            current_json = json.loads(zenodo_json_el)
            validate(current_json['metadata'], zenodo_schema)
        except ValidationError as e:
            print(f"element {ix} fails validation!")
            raise e

    print("All zenodo json passed validation!")
    return


if __name__ == "__main__":
    app.run()

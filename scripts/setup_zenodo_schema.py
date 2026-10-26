import marimo

__generated_with = "0.25.2"
app = marimo.App()


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
 
    """)
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Setup Apache Iceberg schemas for Zenodo

    Planned setup:

    Table 1: all files  = **aggregator_records**
    Columns:
    * file
    * id
    * source (Zenodo)
    * resource_type
    * created (date)
    * updated (date)

    Table 2: eligible datasets with full JSON = **records_to_harvest**
    -> includes transformation into Datacite JSON

    Columns:
    * file
    * id
    * source (Zenodo)
    * resource_type
    * created (date)
    * updated (date)
    * raw_json
    * datacite_json
    * ?additional_metadata_json (file metadata)
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    **FOR TESTING**
    Working on the 20k records dump instead of the full, 8-million.
    """)
    return


@app.cell
def _():
    import duckdb
    import os
    import pyarrow as pa
    from pyiceberg.catalog import load_catalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField, StringType, DateType

    return DateType, NestedField, Schema, StringType, load_catalog, os


@app.cell
def _(load_catalog, os):
    ICEBERG_URL = os.environ["ICEBERG_REST_CATALOG"]

    catalog = load_catalog("remote", **{
        "type": "rest",
        "uri": ICEBERG_URL,
    })
    return (catalog,)


@app.cell
def _(DateType, NestedField, Schema, StringType):
    # Table 1 - for all records

    NAMESPACE = "test_harvest_ice"
    TABLE_IDENTIFIER_T1 = f"{NAMESPACE}.aggregator_records"

    schema_T1 = Schema(
        NestedField(field_id=1, name="file", field_type=StringType(), required=True),
        NestedField(field_id=2, name="id", field_type=StringType(), required=True),
        NestedField(field_id=3, name="source", field_type=StringType(), required=True),
        NestedField(field_id=4, name="resource_type", field_type=StringType(), required=False),
        NestedField(field_id=5, name="created", field_type=DateType(), required=False),
        NestedField(field_id=6, name="updated", field_type=DateType(), required=False),
    )
    return NAMESPACE, TABLE_IDENTIFIER_T1, schema_T1


@app.cell
def _(DateType, NAMESPACE, NestedField, Schema, StringType):
    # Table 2 - for records we want to turn into harvest_events

    TABLE_IDENTIFIER_T2 = f"{NAMESPACE}.records_to_harvest"

    schema_T2 = Schema(
        NestedField(field_id=1, name="file", field_type=StringType(), required=True),
        NestedField(field_id=2, name="id", field_type=StringType(), required=True),
        NestedField(field_id=3, name="source", field_type=StringType(), required=True),
        NestedField(field_id=4, name="resource_type", field_type=StringType(), required=False),
        NestedField(field_id=5, name="created", field_type=DateType(), required=False),
        NestedField(field_id=6, name="updated", field_type=DateType(), required=False),
        NestedField(field_id=7, name="raw_json", field_type=StringType(), required=False),
        NestedField(field_id=8, name="datacite_json", field_type=StringType(), required=False),
        NestedField(field_id=9, name="additional_metadata_json", field_type=StringType(), required=False),
    )
    return


@app.cell
def _(NAMESPACE, catalog):
    catalog.create_namespace_if_not_exists(NAMESPACE)
    return


@app.cell
def _(TABLE_IDENTIFIER_T1, catalog, schema_T1):
    iceberg_table = catalog.create_table_if_not_exists(TABLE_IDENTIFIER_T1, schema=schema_T1)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()

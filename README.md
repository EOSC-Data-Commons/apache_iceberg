# Apache Iceberg configuration and scripts

Currently used to setup an initial harvest of Zenodo metadata.

Run `uv sync` to recreate the needed virtual env.  

## Exploratory modules  

### Zenodo JSON Dump Structural Inference
More info in the [module's README](analysis/README.md).

## Scripts  

### Push Zenodo metadata dump to S3
A script fills a S3 bucket with the contents of the latest Zenodo metadata dump.

To run the script, execute the following:  
`uv run scripts/zenodo_dump_to_s3.py --bucket test_zenodo_dump`  
You can add `--cleanup-local-dump yes` to delete the ~13GB dump that gets downloaded after the script finishes.

It goes through the following steps:
* creates the `--bucket` bucket if it doesn't already exist on the S3
* finds the latest JSON metadata dump from Zenodo
* downloads the dump to local drive
* uses parallel workers to upload the records from the dump to the specified bucket, with the "zenodo_dump/" prefix
* if any records raised an error during upload, the `<bucket>_errors.log` file will be created listing the record ID and the specific error raised

Note: Downloading the .tar.gz dump requires about 13GB of space and might take minutes. Uploading the millions of records may take **hours**!

### Fill Iceberg tables from an S3 bucket of Zenodo records
A script drops and rebuilds the two Iceberg tables defined in `src/apache_iceberg/iceberg_schemas.py`, creating them in the given `--namespace`: `aggregator_records` (one row for each record) and `records_to_harvest` (only *dataset* records, with the raw and DataCite JSON).

To run the script, execute the following:
`uv run scripts/fill_iceberg_with_zenodo.py --bucket zenodo20k-2026-10-03 --namespace test_harvest_ice`

It goes through the following steps:
* connects to the Iceberg REST catalog and the S3 endpoint, with credentials from `.env`
* rebuilds `aggregator_records` from every `*.json` object under the `--prefix`, committing in slices of `--t1-slice` rows
* rebuilds `records_to_harvest` from the `resource_type = 'dataset'` records, adding the raw and DataCite JSON
* records that fail to read or parse are logged and skipped so the run doesn't fail in the middle  


### Use Iceberg records as "harvest_events" in ***Metadata Warehouse***
TODO  
? In which repository should this script live?

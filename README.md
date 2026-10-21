# Apache Iceberg configuration and scripts

Currently used to setup an initial harvest of Zenodo metadata.

## Push Zenodo metadata dump to S3
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

## Define Apache Iceberg tables
TODO Table definitions, fill tables based on S3 contents

## Use Iceberg records as "harvest_events" in ***Metadata Warehouse***
TODO
? In which repository should this script live?
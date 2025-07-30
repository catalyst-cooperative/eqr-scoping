# FERC EQR scoping
## Background
This repo exists for prototyping and scoping the integration of FERC EQR data
into PUDL. We've been interested in this data for years, but due to the large
scale of EQR, and lack of dedicated funding, we've been unable to take on this
project. This constrained scoping project will focus on better understanding the
challenge of a full integration of EQR data while we continue to search for
a funding source.

## Goals
* Archive all EQR data from 2013-present
* Extract and Transform 5 quarters of data
* Develop a consistent schema that can be applied to all years of data
* Understand the infrastructural requirements to handle the large scale of data

## Usage
This repo uses `uv` for dependency management, and `dagster` to orchestrate the
prototype ETL. Once you have [installed uv](https://docs.astral.sh/uv/getting-started/installation/),
you can launch `dagster` with the following command:

```
uv run dg dev
```

Assets are [partitioned](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets)
by year-quarter, and a specific partition(s) can be run by following these steps:

1. Click on the `Assets` tab in the `dagster UI`
2. Click on the `default` asset group
3. Select the desired asset, then click `Materialize selected`
4. Select desired partition, then click `Launch run`

## Design
### Extract
Raw data is archived at `gs://archives.catalyst.coop/eqr/`. There is one zipfile
per quarter, and within each top level zipfile there are nested zipfiles for each
EQR filing. Within each of these filings there CSV files for the 4 tables, `contracts`,
`ident`, `transactions`, and `indexPub`.

The extract asset, `extract_eqr`, will load the raw zipfile for a single quarter,
then loop through the nested filing zipfiles and extract one at a time. It creates a
single parquet file per table per filing. The output parquet files will be saved in
a directory called `extracted_eqr` in the base directory by default. This can be
configured by selecting `Open Launchpad` in the `dagster UI` before executing the
asset and changing the directory specified in the `config` section. Within this
directory, there will be a sub-directory for each of the 4 tables containing one
parquet file per filing.

In order to parse the CSVs, the asset will first extract them from each nested
zipfile to a temporary directory where `duckdb` can copy the data from each CSV to a
parquet file. Given that CSV has no concept of datatypes, and we have no way to be
sure that fields are consistently formatted in such a way that we can easily infer
datatypes, the extraction will keep all columns as strings. This means that the
extraction is not attempting any modification of the underlying data, and is simply
converting to a format that is easier to access in bulk.

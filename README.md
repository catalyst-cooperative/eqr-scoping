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

Additionally there is a master list of FERC Company Identifiers (CIDs) checked into this
repo under the `docs/` directory, which provides some canonical information associated
with each CID.

The extract asset in Dagster `extract_eqr`, will load the raw zipfile for a single quarter,
then loop through the nested filing zipfiles and extract one at a time. It creates a
single parquet file per table per filing. If you are on a residential internet connection,
this may be very slow, and every time you run the asset it will re-download the raw zipfile
for the quarter again. To conserve local disk, the raw zipfiles are not saved. The whole
raw dataset is roughly 85GB.

The output parquet files will be saved in a directory called `extracted_eqr` in the base
directory by default. This can be configured by selecting `Open Launchpad` in the
`dagster UI` before executing the asset and changing the directory specified in the
`config` section. Within this directory, there will be a sub-directory for each of the 4
tables, and in each of those table-level dirctories there is a sub-directory for each
quarter of data named by year_quarter, containing one parquet file per filing:

```text
extracted_eqr
├── contracts
│   ├── 2013q3
│   │   ├── 201309_3C_Solar_LLC_ident.parquet
│   │   ├── 201309_511_Plaza_Energy-_LLC_ident.parquet
│   │   ├── 201309_ABN_Energy-_LLC_ident.parquet
│   │   ├── 201309_Accent_Energy_Midwest_II_LLC_ident.parquet
│   │   ├── 201309_AEP_Energy_Partners-_Inc._ident.parquet
│   │   ├── 201309_AEP_Energy-_Inc._ident.parquet
│   │   ├── 201309_AEP_Generating_Company_ident.parquet
```

In order to parse the CSVs, the asset will first extract them from each nested zipfile
to a temporary directory where `duckdb` can copy the data from each CSV to a parquet
file. Given that CSV has no concept of datatypes, and we have no way to be sure that
fields are consistently formatted in such a way that we can easily infer datatypes, the
extraction will keep all columns as strings. This means that the extraction is not
attempting any modification of the underlying data, and is simply converting to a format
that is easier to access in bulk.

In a small number of cases, `duckdb` will fail to parse a CSV file, producing errors.
In that case the file will be skipped and a warning will be logged. The logging output
is written to a per-quarter parquet under `extracted_eqr/extraction_metadata/`.

The extracted parquet files are pretty big. Almost everything is in the `transactions`
table.

```bash
du -sh extracted_eqr/*
```

```pre
509M     extracted_eqr/contracts/
376M     extracted_eqr/extraction_metadata/
241M     extracted_eqr/ident/
237M     extracted_eqr/indexPub/
46G      extracted_eqr/transactions/
```

### Transform

For our initial simple transforms, we are just casting all columns to reasonable
datatypes, standardizing the case in categorical columns, and combining the data
from many individual filings into larger parquet files organized by quarter.

For each of the EQR tables, there is a corresponding SQL script in the `sql/` directory.
These scripts can be run individually with the `duckdb` command line tool, e.g.:

```bash
duckdb < sql/ident.sql
```

Or they can all be run at once with the `transform_all.sh` script:

```bash
./transform_all.sh
```

The results are written to per-table, per-quarter parquet files in the `parquet/`
directory. For now we are using `duckdb`'s native partitioning support which
generates "hive" partitioned paths because it was simple to implement:

```text
parquet
├── contracts
│   ├── year_quarter=2013q3
│   │   └── data_0.parquet
│   ├── year_quarter=2013q4
│   │   └── data_0.parquet
│   ├── year_quarter=2014q1
│   │   └── data_0.parquet
│   ├── year_quarter=2014q2
│   │   └── data_0.parquet
```

Tools like `duckdb`, `polars` and `pandas` can all read data from partitioned files that
share the same schema as a single table. Beware though, the `transactions` table is huge
and can easily take up all your memory if you're not careful.

```bash
du -sh parquet/*
```

```pre
116M    parquet/contracts/
364K    parquet/ferc_cid.parquet
6.7M    parquet/ident/
140K    parquet/index_pub/
40G     parquet/transactions/
```

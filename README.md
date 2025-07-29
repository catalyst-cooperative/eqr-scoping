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
prototype ETL. In order to launch `dagster`, run:

```
uv run dg dev
```

Assets are [partitioned](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets)
by year-quarter, and a specific partition(s) can be run by following these steps:

1. Click on the `Assets` tab in the `dagster UI`
2. Click on the `default` asset group
3. Select the desired asset, then click `Materialize selected`
4. Select desired partition, then click `Launch run`

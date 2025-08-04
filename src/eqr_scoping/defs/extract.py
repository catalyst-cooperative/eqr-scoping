import io
import tempfile
import zipfile
from collections import Counter
from pathlib import Path

import duckdb
import dagster as dg
from upath import UPath

from eqr_scoping.settings import year_quarters

logger = dg.get_dagster_logger(f"catalystcoop.{__name__}")


class ExtractSettings(dg.ConfigurableResource):
    """Dagster resource which defines which EQR data to extract and configuration for raw archive."""

    archive: str = "gs://archives.catalyst.coop/eqr/"
    output: str = "./extracted_eqr"

    @property
    def base_path(self) -> UPath:
        return UPath(self.archive)

    @property
    def output_path(self) -> UPath:
        return UPath(self.output)


def _get_output_name(parquet_path: Path, csv_path: Path) -> str:
    return str(parquet_path / (csv_path.stem + ".parquet"))


def _clean_csv_name(csv_path: Path) -> str:
    new_path = csv_path
    if "'" in csv_path.name:
        new_path = csv_path.rename(csv_path.parent / csv_path.name.replace("'", ""))
    return new_path


def _csvs_to_parquet(csv_path: Path, output_path: UPath, year_quarter: str):
    """Mirror CSVs in filing to a parquet file.

    Each filing contains a CSV for 4 EQR tables. These will each be extracted
    to a separate parquet file.
    """
    for file in csv_path.iterdir():
        # Detect which table type CSV is and prep output directory
        [table_type] = [
            key
            for key in ["contracts", "ident", "transactions", "indexPub"]
            if file.stem.endswith(key)
        ]
        file = _clean_csv_name(file)
        parquet_path = output_path / table_type
        parquet_path.mkdir(parents=True, exist_ok=True)

        # Use duckdb to read CSV and write as parquet
        duckdb.execute(
            f"COPY (SELECT *, '{year_quarter}' AS year_quarter FROM read_csv('{str(file)}', all_varchar=true, ignore_errors=true))"
            f"    TO '{_get_output_name(parquet_path, file)}';"
        )


@dg.asset(partitions_def=year_quarters, deps=["custom_sql_types"])
def extract_eqr(
    context: dg.AssetExecutionContext,
    extract_settings: ExtractSettings = ExtractSettings(),
):
    """Extract year quarter from CSVs and load to parquet files."""
    # Get year/quarter from selected partition
    year_quarter = context.partition_key
    year, quarter = year_quarter.split("q")
    quarter_zip_path = extract_settings.base_path / f"ferceqr-{year}-Q{quarter}.zip"

    # Open top level zipfile
    with zipfile.ZipFile(
        io.BytesIO(quarter_zip_path.open(mode="rb").read())
    ) as quarter_archive:
        # Check for duplicate filenames
        if len(
            dupes := [
                filing
                for filing, count in Counter(quarter_archive.namelist()).items()
                if count > 1
            ]
        ):
            raise RuntimeError(f"Detected duplicate filings: {dupes}")

        # Loop through all nested zipfiles (one for each filing in the quarter)
        for filing in quarter_archive.namelist():
            # Extract CSVs from filing to a temporary directory so duckdb can be used
            # to parse CSVs and mirror to parquet
            try:
                with zipfile.ZipFile(
                    io.BytesIO(quarter_archive.read(filing))
                ) as filing_archive:
                    logger.info(f"Extracting CSVs from {filing}.")
                    with tempfile.TemporaryDirectory() as tmp_dir:
                        filing_archive.extractall(path=tmp_dir)
                        _csvs_to_parquet(
                            Path(tmp_dir),
                            extract_settings.output_path,
                            year_quarter,
                        )
            except zipfile.BadZipfile:
                logger.warning(f"Could not open filing: {filing}.")

import io
import tempfile
import zipfile
from pathlib import Path

import dagster as dg
from dagster_duckdb import DuckDBResource
from duckdb.duckdb import DuckDBPyConnection
from upath import UPath

from eqr_scoping.settings import year_quarters

logger = dg.get_dagster_logger(f"catalystcoop.{__name__}")


class ExtractSettings(dg.ConfigurableResource):
    """Dagster resource which defines which EQR data to extract and configuration for raw archive."""

    archive: str = "gs://archives.catalyst.coop/ferceqr/"
    output: str = "./extracted"

    @property
    def base_path(self) -> UPath:
        return UPath(self.archive)

    @property
    def output_path(self) -> UPath:
        return UPath(self.output)


def _get_output_name(parquet_path: Path, csv_path: Path) -> Path:
    return parquet_path / (csv_path.stem + ".parquet")


def _clean_csv_name(csv_path: Path) -> Path:
    new_path = csv_path
    if "'" in csv_path.name:
        new_path = csv_path.rename(csv_path.parent / csv_path.name.replace("'", ""))
    if '"' in csv_path.name:
        new_path = csv_path.rename(csv_path.parent / csv_path.name.replace('"', ""))
    return new_path


def _extract_ident(
    ident_csv: str,
    parquet_path: Path,
    year_quarter: str,
    duckdb_connection: DuckDBPyConnection,
) -> str:
    """Extract data from ident csv and write to parquet, returning CID from table."""
    # Use duckdb to read CSV and write as parquet
    duckdb_connection.execute(
        f"COPY (SELECT *, '{year_quarter}' AS year_quarter FROM read_csv('{ident_csv}', all_varchar=true, store_rejects=true))"
        f"    TO '{parquet_path}';"
    )
    (cid,) = duckdb_connection.execute(
        f"SELECT company_identifier FROM '{parquet_path}' LIMIT 1;"
    ).fetchone()
    return cid


def _extract_other_table(
    csv_path: Path,
    parquet_path: Path,
    year_quarter: str,
    cid: str,
    duckdb_connection: DuckDBPyConnection,
):
    """Extract data from ident csv and write to parquet, returning CID from table."""
    # Use duckdb to read CSV and write as parquet
    duckdb_connection.execute(
        f"COPY (SELECT *, '{year_quarter}' AS year_quarter, '{cid}' as company_identifier FROM read_csv('{csv_path}', all_varchar=true, store_rejects=true))"
        f"    TO '{parquet_path}';"
    )


def _csvs_to_parquet(
    csv_path: Path,
    output_path: UPath,
    year_quarter: str,
    duckdb_connection: DuckDBPyConnection,
):
    """Mirror CSVs in filing to a parquet file.

    Each filing contains a CSV for 4 EQR tables. These will each be extracted
    to a separate parquet file.
    """
    # Clean csv filenames for duckdb compatibility, then get ident table path
    csv_paths = [_clean_csv_name(csv_file) for csv_file in csv_path.iterdir()]
    [ident_path] = [
        csv_file for csv_file in csv_paths if csv_file.stem.endswith("ident")
    ]
    csv_paths.remove(ident_path)

    try:
        # Create a subdirectory for the quarter to allow easy selection
        parquet_dir = output_path / "ident" / year_quarter
        parquet_dir.mkdir(parents=True, exist_ok=True)
        # Extract ident table and return CID
        cid = _extract_ident(
            ident_csv=str(ident_path),
            parquet_path=_get_output_name(parquet_dir, ident_path),
            year_quarter=year_quarter,
            duckdb_connection=duckdb_connection,
        )
    except TypeError:
        logger.warning(
            f"Failed to parse ident table from {ident_path.name}."
            " Skipping remaining tables for filing."
        )
        return

    # Loop through remaining CSVs and extract, adding extracted CID as a column in each table
    for file in csv_paths:
        # Detect which table type CSV is and prep output directory
        [table_type] = [
            key
            for key in ["contracts", "transactions", "indexPub"]
            if file.stem.endswith(key)
        ]

        # Create a subdirectory for the quarter to allow easy selection
        parquet_dir = output_path / table_type / year_quarter
        parquet_dir.mkdir(parents=True, exist_ok=True)
        # Use duckdb to read CSV and write as parquet
        _extract_other_table(
            csv_path=file,
            parquet_path=_get_output_name(parquet_dir, ident_path),
            year_quarter=year_quarter,
            cid=cid,
            duckdb_connection=duckdb_connection,
        )


def _create_output_paths(base_path: Path):
    """Create paths for all output parquet files."""
    for table_type in [
        "ident",
        "contracts",
        "transactions",
        "indexPub",
        "extraction_metadata",
    ]:
        (base_path / table_type).mkdir(parents=True, exist_ok=True)


def _save_extract_metadata(
    base_path: Path, year_quarter: str, duckdb_connection: DuckDBPyConnection
):
    """Create parquet file with metadata on any CSV parsing errors."""
    output_dir = base_path / "extraction_metadata" / year_quarter
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"{year_quarter}_extract_metadata.parquet"
    duckdb_connection.execute(
        "COPY ("
        f"    SELECT reject_errors.*, parse_filename(reject_scans.file_path), '{year_quarter}' as year_quarter"
        "    FROM reject_errors"
        "    JOIN reject_scans"
        "    ON reject_errors.scan_id=reject_scans.scan_id AND reject_errors.file_id=reject_scans.file_id"
        f") TO '{str(output_file)}';"
    )


@dg.multi_asset(
    partitions_def=year_quarters,
    specs=[
        dg.AssetSpec("extract_ident"),
        dg.AssetSpec("extract_contracts"),
        dg.AssetSpec("extract_transactions", deps=["custom_sql_types"]),
        dg.AssetSpec("extract_indexPub"),
    ],
)
def extract_eqr(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
    extract_settings: ExtractSettings = ExtractSettings(),
):
    """Extract year quarter from CSVs and load to parquet files."""
    # Get year/quarter from selected partition
    year_quarter = context.partition_key
    quarter_zip_path = extract_settings.base_path / f"ferceqr-{year_quarter}.zip"

    # Prepare output paths for parquet files
    _create_output_paths(extract_settings.output_path)

    # Open top level zipfile
    with (
        zipfile.ZipFile(
            io.BytesIO(quarter_zip_path.open(mode="rb").read())
        ) as quarter_archive,
        duckdb.get_connection() as conn,
    ):
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
                            csv_path=Path(tmp_dir),
                            output_path=extract_settings.output_path,
                            year_quarter=year_quarter,
                            duckdb_connection=conn,
                        )
            except zipfile.BadZipfile:
                logger.warning(f"Could not open filing: {filing}.")
        _save_extract_metadata(extract_settings.output_path, year_quarter, conn)
    yield dg.MaterializeResult(asset_key="extract_ident")
    yield dg.MaterializeResult(asset_key="extract_contracts")
    yield dg.MaterializeResult(asset_key="extract_transactions")
    yield dg.MaterializeResult(asset_key="extract_indexPub")

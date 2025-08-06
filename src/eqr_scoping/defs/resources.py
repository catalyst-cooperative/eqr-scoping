import dagster as dg
from dagster_duckdb import DuckDBResource

from eqr_scoping.defs.extract import ExtractSettings
from eqr_scoping.utils import project_root_dir


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "extract_settings": ExtractSettings(),
            "duckdb": DuckDBResource(database=str(project_root_dir / "eqr.duckdb")),
        }
    )

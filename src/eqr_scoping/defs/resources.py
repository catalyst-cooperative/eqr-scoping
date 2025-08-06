import dagster as dg
from dagster_duckdb import DuckDBResource

from eqr_scoping.defs.extract import ExtractSettings


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "extract_settings": ExtractSettings(),
            "duckdb": DuckDBResource(database=":memory:"),
        }
    )

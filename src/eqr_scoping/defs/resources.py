import dagster as dg
from dagster_dbt import DbtCliResource

from eqr_scoping.defs.extract import ExtractSettings
from eqr_scoping.defs.dbt import dbt_project


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "extract_settings": ExtractSettings(),
            "dbt": DbtCliResource(project_dir=dbt_project),
        }
    )

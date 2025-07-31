import importlib
from pathlib import Path

import dagster as dg
import duckdb
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets


dbt_project_directory = Path(__file__).absolute().parent.parent.parent.parent
dbt_project = DbtProject(project_dir=dbt_project_directory)
dbt_project.prepare_if_dev()


# Yields Dagster events streamed from the dbt CLI
@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


@dg.asset
def custom_sql_types():
    transaction_type_script = (
        importlib.resources.files("eqr_scoping.sql") / "custom_transaction_types.sql"
    )
    with duckdb.connect("eqr.duckdb") as conn, transaction_type_script.open() as f:
        conn.execute(f.read())

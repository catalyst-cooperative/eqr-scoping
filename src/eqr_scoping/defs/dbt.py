import importlib

import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets
from dagster_duckdb import DuckDBResource

from eqr_scoping.utils import project_root_dir


dbt_project = DbtProject(project_dir=project_root_dir)
dbt_project.prepare_if_dev()


# Yields Dagster events streamed from the dbt CLI
@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """Creates dagster assets for each dbt model."""
    yield from dbt.cli(["build"], context=context).stream()


@dg.asset
def custom_sql_types(duckdb: DuckDBResource):
    """Execute SQL script to create custom types in the duckdb database.

    It's can be useful to create custom SQL ENUM types when performing data cleaning,
    but this is not easy to do in dbt directly. As a workaround, we've created a
    dedicated SQL script to create datatypes, and this asset executes that script in
    the duckdb database. This asset is made an upstream dependency of the extract
    asset to make sure that this script gets executed prior to performing transformations.
    """
    transaction_type_script = (
        importlib.resources.files("eqr_scoping.sql") / "custom_transaction_types.sql"
    )
    with duckdb.get_connection() as conn, transaction_type_script.open() as f:
        conn.execute(f.read())

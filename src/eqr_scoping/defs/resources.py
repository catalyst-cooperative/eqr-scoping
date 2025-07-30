import dagster as dg

from eqr_scoping.defs.extract import ExtractSettings


@dg.definitions
def resources():
    return dg.Definitions(resources={"extract_settings": ExtractSettings()})

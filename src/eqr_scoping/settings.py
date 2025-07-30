import dagster as dg


year_quarters: dg.StaticPartitionsDefinition = dg.StaticPartitionsDefinition(
    [
        f"{year}q{quarter}"
        for year in range(2013, 2025)
        for quarter in range(1, 5)
        if not (year == 2013 and quarter < 3)
    ]
)

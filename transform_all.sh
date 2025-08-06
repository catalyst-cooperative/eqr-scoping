#!/bin/sh
# Run all the draft transforms.
# Assumes Dagster extraction has run already.
for table in "ferc_cid" "index_pub" "ident" "contracts" "transactions"; do
    echo "Transforming $table..."
    duckdb <"sql/$table.sql"
done

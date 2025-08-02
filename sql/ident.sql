COPY (
    SELECT *
    FROM 'extracted_eqr/ident/*.parquet'
) TO 'parquet/ident.parquet' (
    FORMAT PARQUET,
    COMPRESSION SNAPPY,
    ROW_GROUP_SIZE 100_000
);

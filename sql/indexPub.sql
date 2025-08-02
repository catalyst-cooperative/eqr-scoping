COPY (
    SELECT *
    FROM 'extracted_eqr/indexPub/*.parquet'
) TO 'parquet/indexPub.parquet' (
    FORMAT PARQUET,
    COMPRESSION SNAPPY,
    ROW_GROUP_SIZE 100_000
);

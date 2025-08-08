COPY (
    SELECT
        filer_unique_id::VARCHAR AS filer_unique_id,
        company_identifier::VARCHAR AS company_identifier,
        Seller_Company_Name::VARCHAR AS seller_company_name,
        CASE
            WHEN UPPER(
                Index_Price_Publishers_To_Which_Sales_Transactions_Have_Been_Reported
            ) = 'N/A' THEN NULL
            ELSE UPPER(
                Index_Price_Publishers_To_Which_Sales_Transactions_Have_Been_Reported
            )::VARCHAR
        END AS index_price_publisher_name,
        Transactions_Reported::VARCHAR AS transactions_reported,
        year_quarter::VARCHAR AS year_quarter
    FROM 'extracted_eqr/indexPub/*/*.parquet'
) TO 'parquet/index_pub' (
    FORMAT PARQUET,
    COMPRESSION SNAPPY,
    ROW_GROUP_SIZE 100_000,
    PARTITION_BY year_quarter,
    WRITE_PARTITION_COLUMNS,
    OVERWRITE_OR_IGNORE
);

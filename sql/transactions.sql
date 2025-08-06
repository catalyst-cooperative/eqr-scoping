-- Simple SQL script to concatenate EQR data and apply real dtypes
-- duckdb < sql/transform.sql
-- This has only been tested on: [2013q3, 2016q2, 2018q1, 2020q1, 2022q4]
-- on a 10 core Apple M1 it takes <90 seconds to run over 371M rows.
COPY (
    SELECT year_quarter::VARCHAR as year_quarter,
        transaction_unique_id::VARCHAR AS transaction_unique_id,
        seller_company_name::VARCHAR AS seller_company_name,
        customer_company_name::VARCHAR AS customer_company_name,
        ferc_tariff_reference::VARCHAR AS ferc_tariff_reference,
        contract_service_agreement::VARCHAR AS contract_service_agreement,
        transaction_unique_identifier::VARCHAR AS transaction_unique_identifier,
        -- Convert date & datetime strings to appropriate types
        TRY_STRPTIME(transaction_begin_date, '%Y%m%d%H%M') AS transaction_begin_date,
        TRY_STRPTIME(transaction_end_date, '%Y%m%d%H%M') AS transaction_end_date,
        TRY_CAST(TRY_STRPTIME(trade_date, '%Y%m%d') AS DATE) AS trade_date,
        -- Convert to uppercase and cast ENUMs, adding true NULL values
        CASE
            WHEN UPPER(exchange_brokerage_service) = 'N/A' THEN NULL
            ELSE UPPER(exchange_brokerage_service)::VARCHAR
        END AS exchange_brokerage_service,
        CASE
            WHEN UPPER(type_of_rate) = 'N/A' THEN NULL
            ELSE UPPER(type_of_rate)::VARCHAR
        END AS type_of_rate,
        CASE
            WHEN UPPER(time_zone) = 'N/A' THEN NULL
            ELSE UPPER(time_zone)::VARCHAR
        END AS time_zone,
        CASE
            WHEN UPPER(class_name) = 'N/A' THEN NULL
            ELSE UPPER(class_name)::VARCHAR
        END AS class_name,
        CASE
            WHEN UPPER(term_name) = 'N/A' THEN NULL
            ELSE UPPER(term_name)::VARCHAR
        END AS term_name,
        CASE
            WHEN UPPER(increment_name) = 'N/A' THEN NULL
            ELSE UPPER(increment_name)::VARCHAR
        END AS increment_name,
        CASE
            WHEN UPPER(increment_peaking_name) = 'N/A' THEN NULL
            ELSE UPPER(increment_peaking_name)::VARCHAR
        END AS increment_peaking_name,
        CASE
            WHEN UPPER(product_name) = 'N/A' THEN NULL
            ELSE UPPER(product_name)::VARCHAR
        END AS product_name,
        CASE
            WHEN UPPER(rate_units) = 'N/A' THEN NULL
            ELSE UPPER(rate_units)::VARCHAR
        END AS rate_units,
        -- There are 100+ BA codes in here, link to EIA
        UPPER(point_of_delivery_balancing_authority)::VARCHAR AS point_of_delivery_balancing_authority,
        -- There are ~9000 of these. Low cardinality relative to 200M rows
        UPPER(point_of_delivery_specific_location)::VARCHAR AS point_of_delivery_specific_location,
        TRY_CAST(transaction_quantity AS FLOAT) AS transaction_quantity,
        TRY_CAST(price AS FLOAT) AS price,
        TRY_CAST(standardized_quantity AS FLOAT) AS standardized_quantity,
        TRY_CAST(standardized_price AS DECIMAL(12, 2)) AS standardized_price,
        TRY_CAST(total_transmission_charge AS DECIMAL(12, 2)) AS total_transmission_charge,
        TRY_CAST(total_transaction_charge AS DECIMAL(12, 2)) AS total_transaction_charge
    FROM 'extracted_eqr/transactions/*/*.parquet'
) TO 'parquet/transactions' (
    FORMAT PARQUET,
    COMPRESSION SNAPPY,
    ROW_GROUP_SIZE 100_000,
    PARTITION_BY year_quarter,
    WRITE_PARTITION_COLUMNS,
    OVERWRITE_OR_IGNORE
);

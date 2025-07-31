SELECT
    year_quarter::VARCHAR AS year_quarter,
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
        WHEN exchange_brokerage_service = 'N/A' THEN NULL
        ELSE TRY_CAST(UPPER(exchange_brokerage_service) AS exchange_brokerage_service_enum)
    END AS exchange_brokerage_service,
    CASE
        WHEN type_of_rate = 'N/A' THEN NULL
        ELSE TRY_CAST(UPPER(type_of_rate) AS type_of_rate_enum)
    END AS type_of_rate,
    CASE
        WHEN time_zone = 'N/A' THEN NULL
        ELSE TRY_CAST(UPPER(time_zone) AS time_zone_enum)
    END AS time_zone,
    CASE
        WHEN class_name = 'N/A' THEN NULL
        ELSE TRY_CAST(UPPER(class_name) AS class_name_enum)
    END AS class_name,
    CASE
        WHEN term_name = 'N/A' THEN NULL
        ELSE TRY_CAST(UPPER(term_name) AS term_name_enum)
    END AS term_name,
    CASE
        WHEN increment_name = 'N/A' THEN NULL
        ELSE TRY_CAST(UPPER(increment_name) AS increment_name_enum)
    END AS increment_name,
    CASE
        WHEN increment_peaking_name = 'N/A' THEN NULL
        ELSE TRY_CAST(UPPER(increment_peaking_name) AS increment_peaking_name_enum)
    END AS increment_peaking_name,
    CASE
        WHEN product_name = 'N/A' THEN NULL
        ELSE TRY_CAST(UPPER(product_name) AS product_name_enum)
    END AS product_name,
    CASE
        WHEN rate_units = 'N/A' THEN NULL
        ELSE TRY_CAST(UPPER(rate_units) AS rate_units_enum)
    END AS rate_units,

    -- There are 100+ BA codes in here, which we would probably want to link to EIA
    UPPER(point_of_delivery_balancing_authority)::VARCHAR as point_of_delivery_balancing_authority,
    -- There are ~9000 of these. Low cardinality relative to 200M rows, but it's a
    UPPER(point_of_delivery_specific_location)::VARCHAR as point_of_delivery_specific_location,

    TRY_CAST(transaction_quantity AS FLOAT) AS transaction_quantity,
    TRY_CAST(price AS FLOAT) AS price,
    TRY_CAST(standardized_quantity AS FLOAT) AS standardized_quantity,
    TRY_CAST(standardized_price AS DECIMAL(12,2)) AS standardized_price,
    TRY_CAST(total_transmission_charge AS DECIMAL(12,2)) AS total_transmission_charge,
    TRY_CAST(total_transaction_charge AS DECIMAL(12,2)) AS total_transaction_charge
FROM {{ source('raw_eqr', 'transactions') }}

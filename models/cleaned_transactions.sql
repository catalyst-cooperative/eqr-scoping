SELECT CAST(year_quarter AS VARCHAR) AS year_quarter,
    CAST(transaction_unique_id AS VARCHAR) AS transaction_unique_id,
    CAST(seller_company_name AS VARCHAR) AS seller_company_name,
    CAST(customer_company_name AS VARCHAR) AS customer_company_name,
    CAST(ferc_tariff_reference AS VARCHAR) AS ferc_tariff_reference,
    CAST(contract_service_agreement AS VARCHAR) AS contract_service_agreement,
    CAST(transaction_unique_identifier AS VARCHAR) AS transaction_unique_identifier,
    -- Convert date & datetime strings to appropriate types
    TRY_STRPTIME(transaction_begin_date, '%Y%m%d%H%M') AS transaction_begin_date,
    TRY_STRPTIME(transaction_end_date, '%Y%m%d%H%M') AS transaction_end_date,
    TRY_CAST(TRY_STRPTIME(trade_date, '%Y%m%d') AS DATE) AS trade_date,
    -- Convert to uppercase and cast ENUMs, adding true NULL values
    CASE
        WHEN UPPER(exchange_brokerage_service) = 'N/A' THEN NULL
        ELSE TRY_CAST(
            UPPER(exchange_brokerage_service) AS exchange_brokerage_service_enum
        )
    END AS exchange_brokerage_service,
    CASE
        WHEN UPPER(type_of_rate) = 'N/A' THEN NULL
        ELSE TRY_CAST(UPPER(type_of_rate) AS type_of_rate_enum)
    END AS type_of_rate,
    CASE
        WHEN UPPER(time_zone) = 'N/A' THEN NULL
        ELSE TRY_CAST(UPPER(time_zone) AS time_zone_enum)
    END AS time_zone,
    CASE
        WHEN UPPER(class_name) = 'N/A' THEN NULL
        ELSE TRY_CAST(UPPER(class_name) AS class_name_enum)
    END AS class_name,
    CASE
        WHEN UPPER(term_name) = 'N/A' THEN NULL
        ELSE TRY_CAST(UPPER(term_name) AS term_name_enum)
    END AS term_name,
    CASE
        WHEN UPPER(increment_name) = 'N/A' THEN NULL
        ELSE TRY_CAST(UPPER(increment_name) AS increment_name_enum)
    END AS increment_name,
    CASE
        WHEN UPPER(increment_peaking_name) = 'N/A' THEN NULL
        ELSE TRY_CAST(
            UPPER(increment_peaking_name) AS increment_peaking_name_enum
        )
    END AS increment_peaking_name,
    CASE
        WHEN UPPER(product_name) = 'N/A' THEN NULL
        ELSE TRY_CAST(UPPER(product_name) AS product_name_enum)
    END AS product_name,
    CASE
        WHEN UPPER(rate_units) = 'N/A' THEN NULL
        ELSE TRY_CAST(UPPER(rate_units) AS rate_units_enum)
    END AS rate_units,
    -- There are 100+ BA codes in here, which we would probably want to link to EIA
    CAST(
        UPPER(point_of_delivery_balancing_authority) AS VARCHAR
    ) AS point_of_delivery_balancing_authority,
    -- There are ~9000 of these. Low cardinality relative to 200M rows, but it's a
    CAST(
        UPPER(point_of_delivery_specific_location) AS VARCHAR
    ) AS point_of_delivery_specific_location,
    TRY_CAST(transaction_quantity AS FLOAT) AS transaction_quantity,
    TRY_CAST(price AS FLOAT) AS price,
    TRY_CAST(standardized_quantity AS FLOAT) AS standardized_quantity,
    TRY_CAST(standardized_price AS DECIMAL(12, 2)) AS standardized_price,
    TRY_CAST(total_transmission_charge AS DECIMAL(12, 2)) AS total_transmission_charge,
    TRY_CAST(total_transaction_charge AS DECIMAL(12, 2)) AS total_transaction_charge
FROM {{ source('raw_eqr', 'transactions') }}

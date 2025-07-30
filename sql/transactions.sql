-- Simple SQL script to concatenate EQR transactions and apply real dtypes
-- However, the ENUMs for low-cardinality strings are not reducing the file size.
-- Not sure if they're actually being dictionary encoded in the Parquet output.
-- Ideally we would identify the universe of values dynamically rather than manually.
-- This has only been tested on the 2022 Q4 data.
-- It takes only a few seconds to run on 200M rows.
-- To run the conversion you can just do:
-- duckdb < transactions.sql

-- Create ENUM types for all categorical columns
CREATE TYPE exchange_brokerage_service_enum AS ENUM (
    'BROKER',
    'ICE',
    'NODAL'
);

CREATE TYPE type_of_rate_enum AS ENUM (
    'ELECTRIC INDEX',
    'FIXED',
    'FORMULA',
    'RTO/ISO'
);

CREATE TYPE time_zone_enum AS ENUM (
    'CD',
    'CP',
    'CS',
    'ED',
    'EP',
    'EPT',
    'ES',
    'EST',
    'MD',
    'MP',
    'MS',
    'PD',
    'PP',
    'PS'
);

CREATE TYPE class_name_enum AS ENUM (
    'BA',
    'F',
    'NF',
    'UP'
);

CREATE TYPE term_name_enum AS ENUM (
    'LT',
    'ST'
);

CREATE TYPE increment_name_enum AS ENUM (
    '15',
    '5',
    'D',
    'H',
    'M',
    'W',
    'Y'
);

CREATE TYPE increment_peaking_name_enum AS ENUM (
    'FP',
    'OP',
    'P'
);

CREATE TYPE product_name_enum AS ENUM (
    'BLACK START SERVICE',
    'BOOKED OUT POWER',
    'CAPACITY',
    'CUSTOMER CHARGE',
    'ENERGY',
    'ENERGY IMBALANCE',
    'EXCHANGE',
    'FUEL CHARGE',
    'GENERATOR IMBALANCE',
    'GRANDFATHERED BUNDLED',
    'NEGOTIATED RATE TRANSMISSION',
    'OTHER',
    'PRIMARY FREQUENCY RESPONSE',
    'REACTIVE SUPPLY & VOLTAGE CONTROL',
    'REAL POWER TRANSMISSION LOSS',
    'REGULATION & FREQUENCY RESPONSE',
    'REQUIREMENTS SERVICE',
    'SCHEDULE SYSTEM CONTROL & DISPATCH',
    'SPINNING RESERVE',
    'SUPPLEMENTAL RESERVE',
    'TOLLING ENERGY',
    'UPLIFT'
);

CREATE TYPE rate_units_enum AS ENUM (
    'KVA',
    'KVR',
    'KW',
    'KW-DAY',
    'KW-MO',
    'KW-YR',
    'KWH',
    'MVAR-YR',
    '$/MW',
    '$/MW-DAY',
    '$/MW-MO',
    '$/MW-YR',
    '$/MWH',
    '$/RKVA',
    'FLAT RATE'
);

COPY (
    SELECT
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

        -- More work is required to make these into clean ENUMs
        UPPER(point_of_delivery_balancing_authority)::VARCHAR as point_of_delivery_balancing_authority,
        UPPER(point_of_delivery_specific_location)::VARCHAR as point_of_delivery_specific_location,

        TRY_CAST(transaction_quantity AS FLOAT) AS transaction_quantity,
        TRY_CAST(price AS FLOAT) AS price,
        TRY_CAST(standardized_quantity AS FLOAT) AS standardized_quantity,
        TRY_CAST(standardized_price AS DECIMAL(12,2)) AS standardized_price,
        TRY_CAST(total_transmission_charge AS DECIMAL(12,2)) AS total_transmission_charge,
        TRY_CAST(total_transaction_charge AS DECIMAL(12,2)) AS total_transaction_charge
    FROM 'extracted_eqr/transactions/*.parquet'
) TO 'transactions.parquet' (
    FORMAT PARQUET,
    COMPRESSION SNAPPY,
    ROW_GROUP_SIZE 100_000
);

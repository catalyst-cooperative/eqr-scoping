-- Simple SQL script to concatenate EQR data and apply real dtypes
-- duckdb < sql/contracts.sql
-- This has only been tested on: [2013q3, 2016q2, 2018q1, 2020q1, 2022q4]
COPY (
    SELECT year_quarter::VARCHAR AS year_quarter,
        contract_unique_id::VARCHAR AS contract_unique_id,
        seller_company_name::VARCHAR AS seller_company_name,
        customer_company_name::VARCHAR AS customer_company_name,
        CASE
            WHEN UPPER(contract_affiliate) = 'N/A' THEN NULL
            ELSE UPPER(contract_affiliate)::VARCHAR
        END AS contract_affiliate,
        ferc_tariff_reference::VARCHAR AS ferc_tariff_reference,
        contract_service_agreement_id::VARCHAR AS contract_service_agreement_id,
        TRY_CAST(
            TRY_STRPTIME(contract_execution_date, '%Y%m%d') AS DATE
        ) AS contract_execution_date,
        TRY_CAST(
            TRY_STRPTIME(commencement_date_of_contract_term, '%Y%m%d') AS DATE
        ) AS commencement_date_of_contract_term,
        TRY_CAST(
            TRY_STRPTIME(contract_termination_date, '%Y%m%d') AS DATE
        ) AS contract_termination_date,
        TRY_CAST(
            TRY_STRPTIME(actual_termination_date, '%Y%m%d') AS DATE
        ) AS actual_termination_date,
        extension_provision_description::VARCHAR AS extension_provision_description,
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
            WHEN UPPER(product_type_name) = 'N/A' THEN NULL
            ELSE UPPER(product_type_name)::VARCHAR
        END AS product_type_name,
        CASE
            WHEN UPPER(product_name) = 'N/A' THEN NULL
            ELSE UPPER(product_name)::VARCHAR
        END AS product_name,
        TRY_CAST(quantity AS FLOAT) AS quantity,
        CASE
            WHEN UPPER(units) = 'N/A' THEN NULL
            ELSE UPPER(units)::VARCHAR
        END AS units,
        TRY_CAST(rate AS FLOAT) AS rate,
        TRY_CAST(rate_minimum AS FLOAT) AS rate_minimum,
        TRY_CAST(rate_maximum AS FLOAT) AS rate_maximum,
        rate_description::VARCHAR AS rate_description,
        CASE
            WHEN UPPER(rate_units) = 'N/A' THEN NULL
            ELSE UPPER(rate_units)::VARCHAR
        END AS rate_units,
        point_of_receipt_balancing_authority::VARCHAR AS point_of_receipt_balancing_authority,
        point_of_receipt_specific_location::VARCHAR AS point_of_receipt_specific_location,
        point_of_delivery_balancing_authority::VARCHAR AS point_of_delivery_balancing_authority,
        point_of_delivery_specific_location::VARCHAR AS point_of_delivery_specific_location,
        TRY_STRPTIME(begin_date, '%Y%m%d%H%M') AS begin_date,
        TRY_STRPTIME(end_date, '%Y%m%d%H%M') AS end_date
    FROM 'extracted_eqr/contracts/*.parquet'
) TO 'parquet/contracts.parquet' (
    FORMAT PARQUET,
    COMPRESSION SNAPPY,
    ROW_GROUP_SIZE 100_000
);

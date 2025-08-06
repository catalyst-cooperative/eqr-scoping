COPY (
    SELECT filer_unique_id::VARCHAR AS filer_unique_id,
        company_identifier::VARCHAR AS company_identifier,
        company_name::VARCHAR AS company_name,
        company_identifier::VARCHAR AS company_identifier,
        contact_name::VARCHAR AS contact_name,
        contact_title::VARCHAR AS contact_title,
        contact_address::VARCHAR AS contact_address,
        contact_city::VARCHAR AS contact_city,
        contact_state::VARCHAR AS contact_state,
        contact_zip::VARCHAR AS contact_zip,
        contact_country_name::VARCHAR AS contact_country_name,
        contact_phone::VARCHAR AS contact_phone,
        contact_email::VARCHAR AS contact_email,
        CASE
            WHEN transactions_reported_to_index_price_publishers = 'Y' THEN TRUE
            WHEN transactions_reported_to_index_price_publishers = 'N' THEN FALSE
            ELSE NULL
        END AS transactions_reported_to_index_price_publishers,
        filing_quarter::VARCHAR AS filing_quarter,
        year_quarter::VARCHAR AS year_quarter
    FROM 'extracted_eqr/ident/*/*.parquet'
) TO 'parquet/ident' (
    FORMAT PARQUET,
    COMPRESSION SNAPPY,
    ROW_GROUP_SIZE 100_000,
    PARTITION_BY year_quarter,
    WRITE_PARTITION_COLUMNS,
    OVERWRITE_OR_IGNORE
);

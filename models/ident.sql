SELECT filer_unique_id::VARCHAR as filer_unique_id,
    company_name::VARCHAR as company_name,
    company_identifier::VARCHAR as company_identifier,
    contact_name::VARCHAR as contact_name,
    contact_title::VARCHAR as contact_title,
    contact_address::VARCHAR as contact_address,
    contact_city::VARCHAR as contact_city,
    contact_state::VARCHAR as contact_state,
    contact_zip::VARCHAR as contact_zip,
    contact_country_name::VARCHAR as contact_country_name,
    contact_phone::VARCHAR as contact_phone,
    contact_email::VARCHAR as contact_email,
    CASE
        WHEN transactions_reported_to_index_price_publishers = 'Y' THEN TRUE
        WHEN transactions_reported_to_index_price_publishers = 'N' THEN FALSE
        ELSE NULL
    END AS transactions_reported_to_index_price_publishers,
    filing_quarter::VARCHAR as filing_quarter,
    year_quarter::VARCHAR as year_quarter
FROM {{ source('raw_eqr', 'ident') }}

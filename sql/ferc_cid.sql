COPY (
    SELECT
        organization_name,
        cid::VARCHAR AS company_identifier,
        column1::VARCHAR AS address_full,
        _program::VARCHAR AS ferc_program,
        region::VARCHAR AS region,
        company_website::VARCHAR AS company_website,
        address::VARCHAR AS address1,
        address2::VARCHAR AS address2,
        address3::VARCHAR AS address3,
        city::VARCHAR AS city,
        state::VARCHAR AS state_or_province,
        zip::VARCHAR AS postal_code
    FROM READ_CSV(
        'docs/ferc-cid-listing-2025-06-10.csv',
        HEADER=TRUE,
        NORMALIZE_NAMES=TRUE
    )
) TO 'parquet/ferc_cid.parquet' (
    FORMAT PARQUET,
    COMPRESSION SNAPPY
)

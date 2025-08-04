SELECT filer_unique_id::VARCHAR AS filer_unique_id,
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
FROM {{ source('raw_eqr', 'indexPub') }}

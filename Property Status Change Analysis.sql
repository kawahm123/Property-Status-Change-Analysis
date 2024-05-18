WITH ranked_changes AS (
    -- Create a ranked list of records status changes ordered by the most recent validation dates
    SELECT
        record_key,
        valid_to,
        valid_from,
        property_id,
        category,
        address_line,
        locality,
        region,
        postal_code,
        verified_by,
        created_date,
        listing_id,
        listing_type,
        status,
        usage,
        available_units,
        -- Get the previous status of the listing
        LAG(status, 1) OVER (PARTITION BY record_key ORDER BY valid_to DESC) AS prev_status,
        -- Rank the records for each property by validation date
        ROW_NUMBER() OVER (PARTITION BY record_key ORDER BY valid_from DESC) AS row_num
    FROM main_table
    LEFT JOIN property_table
    ON record_key = property_key
    WHERE valid_from BETWEEN '2024-03-01' AND '2024-03-31'
),

filtered_changes AS (
    -- Filter the ranked status changes to include only significant changes
    SELECT *,
        CASE
            WHEN status = 'WITHDRAWN' AND prev_status IS NOT NULL THEN 1
            ELSE 0
        END AS status_change_count,
        CAST('status' AS TEXT) AS column_change_name
    FROM ranked_changes
    WHERE row_num = 1
    AND (
        CASE
            WHEN available_units ~ '^[0-9]+$' THEN CAST(available_units AS INTEGER)
            ELSE NULL
        END
    ) > 10000
    AND status = 'WITHDRAWN'
)

-- Final query to construct property and listing URLs and select required fields
SELECT
    COALESCE(TO_CHAR(created_date::TIMESTAMP, 'MM/DD/YYYY'), 'N/A') AS created_date,
    COALESCE(TO_CHAR(valid_from::TIMESTAMP, 'MM/DD/YYYY'), 'N/A') AS modified_date,
    'https://example.com/property/' || property_id AS property_url,
    'https://example.com/listing/' || listing_id AS listing_url,
    listing_type AS space_type,
    status AS space_status,
    category AS category,
    usage AS primary_use,
    address_line AS address,
    locality AS city,
    region AS state,
    postal_code AS zip,
    available_units AS space_size
FROM filtered_changes
LEFT JOIN property_table
ON record_key = property_key
GROUP BY
    created_date,
    valid_from,
    property_id,
    listing_id,
    listing_type,
    status,
    category,
    usage,
    address,
    city,
    state,
    postal_code,
    space_size,
    verified_by
ORDER BY
    valid_from DESC,
    address;
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='customer_sk',
    on_schema_change='sync_all_columns'
) }}

WITH source_data AS (
    SELECT 
        CustomerID, 
        CustomerName, 
        ContactPerson, 
        Address, 
        City, 
        PostalCode, 
        Country,
        CURRENT_TIMESTAMP AS Effective_Date
    FROM {{ ref('customers_stage') }}
)

{% if is_incremental() %}
    , existing_data AS (
        SELECT * FROM {{ this }}
    )
{% endif %}

, scd2_Data AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['CustomerID']) }} AS customer_sk,
        s.CustomerID,
        s.CustomerName,
        s.ContactPerson,
        s.Address,
        s.City,
        s.PostalCode,
        s.Country,
        s.Effective_Date,
        NULL AS End_Date,  
        1 AS Is_Active
    FROM source_data s
    {% if is_incremental() %}
        LEFT JOIN existing_data e 
        ON s.CustomerID = e.CustomerID 
        AND e.Is_Active = 1
        WHERE 
            e.CustomerID IS NULL
            OR e.CustomerName != s.CustomerName
            OR e.Address != s.Address
            OR e.City != s.City
            OR e.PostalCode != s.PostalCode
            OR e.Country != s.Country
    {% endif %}
)

{% if is_incremental() %}
    UPDATE {{ this }}
    SET End_Date = CURRENT_TIMESTAMP, Is_Active = 0
    WHERE CustomerID IN (SELECT CustomerID FROM scd2_Data)
    AND Is_Active = 1;

{% endif %}

SELECT * FROM scd2_Data
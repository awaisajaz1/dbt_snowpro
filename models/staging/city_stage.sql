{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='CityID'
) }}

SELECT 
    CityID, PostalCode, Country
FROM
{{ source('orders_system', 'city') }}

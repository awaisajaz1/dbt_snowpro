-- {{ config(
--     materialized='view',
--     snowflake_warehouse='compute_wh',
--     snowflake_database='dbt_dwh',
--     schema='staging'
-- ) }}


SELECT 
*
FROM
{{ source('orders_system', 'customers') }}

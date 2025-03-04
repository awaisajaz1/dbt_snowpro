{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='ProductID'
) }}


SELECT 
    products.ProductID,
    products.ProductName, 
    products.SUPPLIER, 
    products.CATEGORY,
    products.PRICE,
    products.STOCK
FROM
{{ source('orders_system', 'products') }} as products
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='CustomerID'
) }}


SELECT 
    customers.CustomerID,
    customers.CustomerName, 
    ContactPerson, 
    customers.Address, 
    customers.City,
    city.PostalCode,
    city.Country
FROM
{{ref('customers_stage')}} as customers
left JOIN
{{ref('city_stage')}} as city
on city.CityID = customers.City
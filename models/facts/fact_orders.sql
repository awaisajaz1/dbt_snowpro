{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['OrderID', 'orderdetailid']
) }}

SELECT
    orders.OrderID,
    orderdetails.orderdetailid,
    orderdetails.productID,
    products.productName,
    orders.customerID,
    customers.customerName,
    orders.orderDate,
    orders.status,
    orderdetails.quantity,
    orderdetails.price AS unitprice,
    orders.totalamount
FROM
    {{ source('orders_system', 'orders') }} AS orders
JOIN
    {{ source('orders_system', 'orderdetails') }} AS orderdetails  ON orders.OrderID = orderdetails.OrderID
LEFT JOIN
    {{ ref('dim_customers') }} AS customers  ON orders.CustomerID = customers.CustomerID
LEFT JOIN
    {{ ref('dim_products') }} AS products  ON orderdetails.ProductID = products.ProductID

{% if is_incremental() %}
WHERE orders.orderDate >= (SELECT MAX(orderDate) FROM {{ this }})
{% endif %}

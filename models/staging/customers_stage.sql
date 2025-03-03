SELECT 
    customers.CustomerID,
    customers.CustomerName, 
    customers.ContactName as ContactPerson, 
    customers.Address, 
    customers.City,
    city.PostalCode,
    city.Country
FROM
{{ source('orders_system', 'customers') }} as customers
left JOIN
{{ ref('city_stage') }} as city
on customers.City = city.CityID

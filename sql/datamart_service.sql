CREATE TABLE datamart_service AS
SELECT
    DATE_FORMAT(a.service_date, '%Y') AS periode,
    a.vin,
    c.name AS customer_name,
    ca.address,
    
    COUNT(*) AS count_service,
    CASE
        WHEN COUNT(*) > 10 THEN 'HIGH'
        WHEN COUNT(*) BETWEEN 5 AND 10 THEN 'MED'
        ELSE 'LOW'
    END AS priority
FROM after_sales_clean a
LEFT JOIN customers_clean c ON a.customer_id = c.id
LEFT JOIN customer_addresses_clean ca ON a.customer_id = ca.customer_id
GROUP BY 1,2,3,4;
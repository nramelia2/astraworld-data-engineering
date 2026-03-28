CREATE TABLE datamart_sales AS
SELECT
    DATE_FORMAT(invoice_date, '%Y-%m') AS periode,
    CASE
        WHEN price BETWEEN 100000000 AND 250000000 THEN 'LOW'
        WHEN price BETWEEN 250000001 AND 400000000 THEN 'MEDIUM'
        WHEN price > 400000000 THEN 'HIGH'
        ELSE 'UNKNOWN'
    END AS class,
    model,
    SUM(price) AS total
FROM sales_clean
GROUP BY 1,2,3;
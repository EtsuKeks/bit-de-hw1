SELECT c.customer_id, COUNT(t.transaction_id) AS approved_transactions
FROM customer_parquet c
JOIN transaction_partitioned t ON c.customer_id = t.customer_id
WHERE t.order_status = 'Approved'
GROUP BY c.customer_id;

SELECT 
    SUBSTR(t.transaction_date, 6, 2) AS month,
    c.job_industry_category,
    COUNT(*) AS transactions
FROM transaction_partitioned t
JOIN customer_parquet c ON t.customer_id = c.customer_id
GROUP BY SUBSTR(t.transaction_date, 6, 2), c.job_industry_category;

SELECT c.first_name, c.last_name
FROM customer_parquet c
LEFT JOIN transaction_partitioned t ON c.customer_id = t.customer_id
WHERE t.transaction_id IS NULL;

SELECT first_name,
       last_name,
       total_spent
FROM (
    SELECT c.first_name,
           c.last_name,
           SUM(CAST(t.list_price AS DOUBLE)) AS total_spent,
           MIN(SUM(CAST(t.list_price AS DOUBLE))) OVER () AS min_total,
           MAX(SUM(CAST(t.list_price AS DOUBLE))) OVER () AS max_total
    FROM customer_parquet c
    JOIN transaction_partitioned t ON c.customer_id = t.customer_id
    WHERE t.list_price IS NOT NULL
    GROUP BY c.first_name, c.last_name
) x
WHERE total_spent = min_total OR total_spent = max_total;

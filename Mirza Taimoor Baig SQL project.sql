Select * from customer_nodes;
Select * from regions;
Select * from transaction;

SELECT COUNT(DISTINCT node_id) AS unique_node_count
FROM customer_nodes;

select region_id, count(node_id) as num_of_nodes from customer_nodes
group by region_id
order by count(node_id) asc;

select region_id, count(distinct customer_id) as Total_customer 
 from customer_nodes
 group by region_id
 order by count(distinct customer_id);
 
SELECT
AVG(DATEDIFF(end_date, start_id)) AS average_days_reallocation
FROM
customer
WHERE
end_date IS NOT NULL AND YEAR(end_date) <> 9999;

SELECT 
  r.region_id,
  AVG(CASE WHEN rn = FLOOR(0.5 * cnt + 0.5) THEN ReallocationDays END) AS 'Median',
  AVG(CASE WHEN rn = FLOOR(0.8 * cnt + 0.5) THEN ReallocationDays END) AS 'P80',
  AVG(CASE WHEN rn = FLOOR(0.95 * cnt + 0.5) THEN ReallocationDays END) AS 'P95'
FROM (
  SELECT 
    cn.region_id,
    DATEDIFF(cn.end_date, cn.start_date) AS ReallocationDays,
    @rn := IF(@prev_region = cn.region_id, @rn + 1, 1) AS rn,
    @cnt := IF(@prev_region = cn.region_id, @cnt, (SELECT COUNT(*) FROM customer_nodes WHERE region_id = cn.region_id)) AS cnt,
    @prev_region := cn.region_id
  FROM 
    customer_nodes cn,
    (SELECT @rn := 0, @cnt := 0, @prev_region := NULL) r
  ORDER BY 
    cn.region_id, DATEDIFF(cn.end_date, cn.start_date)
) AS ranked
JOIN regions r ON ranked.region_id = r.region_id
GROUP BY region_id;


Select txn_type,
Count(txn_type), Sum(txn_amount)
From transaction
group by txn_type;

SELECT
  AVG(Deposit_Count) AS Avg_Deposit_Count,
  AVG(Deposit_Amount) AS Avg_Deposit_Amount
FROM (
  SELECT
    customer_id,
    COUNT(*) AS Deposit_Count,
    SUM(txn_amount) AS Deposit_Amount
  FROM transaction
  WHERE txn_type = 'deposit'
  GROUP BY customer_id
) AS Customer_Deposits;

Select * from transaction;
SELECT
    YEAR(txn_date) AS year,
    MONTH(txn_date) AS month,
    customer_id,
    COUNT(CASE WHEN txn_type = 'deposit' THEN 1 END) AS deposit_count,
    COUNT(CASE WHEN txn_type = 'purchase' THEN 1 END) AS purchase_count,
    COUNT(CASE WHEN txn_type = 'withdrawal' THEN 1 END) AS withdrawal_count
FROM
    transaction
GROUP BY
    YEAR(txn_date),
    MONTH(txn_date),
    customer_id
HAVING
    deposit_count > 1 AND (purchase_count = 1 OR withdrawal_count = 1);
Select * from transaction;

SELECT
  customer_id,
  EXTRACT(YEAR FROM txn_date) AS year,
  EXTRACT(MONTH FROM txn_date) AS month,
  SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE 0 END) - 
  SUM(CASE WHEN txn_type IN ('purchase', 'withdrawal') THEN txn_amount ELSE 0 END) AS closing_balance
FROM
  transaction
GROUP BY
  customer_id,
  EXTRACT(YEAR FROM txn_date),
  EXTRACT(MONTH FROM txn_date)
ORDER BY
  customer_id,
  year,
  month;

WITH CustomerBalances AS (
    SELECT
        customer_id,
        SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount ELSE 0 END) AS TotalDeposits,
        SUM(CASE WHEN txn_type = 'purchase' THEN txn_amount ELSE 0 END) AS TotalPurchases
    FROM
        transaction
    GROUP BY
        customer_id
),
BalanceChanges AS (
    SELECT
        customer_id,
        TotalDeposits,
        TotalPurchases,
        (TotalDeposits - TotalPurchases) AS NetBalanceChange,
        ((TotalDeposits - TotalPurchases) / NULLIF(TotalDeposits, 0)) * 100 AS PercentageIncrease
    FROM
        CustomerBalances
)

SELECT
    COUNT(*) AS TotalCustomers,
    SUM(CASE WHEN PercentageIncrease > 5 THEN 1 ELSE 0 END) AS CustomersIncreasedMoreThan5Percent,
    (SUM(CASE WHEN PercentageIncrease > 5 THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100 AS PercentageOfCustomersIncreasedMoreThan5Percent
FROM
    BalanceChanges;
    
    -- Option 1: Data is allocated based off the amount of money at the end of the previous month?

SET SQL_mode = '';

WITH adjusted_amount AS (
SELECT customer_id, txn_type, 
EXTRACT(MONTH FROM (txn_date)) AS month_number, 
MONTHNAME(txn_date) AS month,
CASE 
WHEN  txn_type = 'deposit' THEN txn_amount
ELSE -txn_amount
END AS amount
FROM customer_transactions
),
balance AS (
SELECT customer_id, month_number, month,
SUM(amount) OVER(PARTITION BY customer_id, month_number ORDER BY month_number ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
AS running_balance
FROM adjusted_amount
),
allocation AS (
SELECT customer_id, month_number,month,
LAG(running_balance,1) OVER(PARTITION BY customer_id, month_number ORDER BY month_number) AS monthly_allocation
FROM balance
)
SELECT month_number,month,
SUM(CASE WHEN monthly_allocation < 0 THEN 0 ELSE monthly_allocation END) AS total_allocation
FROM allocation
GROUP BY 1,2
ORDER BY 1,2; 
 
-- Option 2: Data is allocated on the average amount of money kept in the
-- account in the previous 30 days

WITH updated_transactions AS (
SELECT customer_id, txn_type, 
EXTRACT(MONTH FROM(txn_date)) AS Month_number,
MONTHNAME(txn_date) AS month,
CASE
WHEN txn_type = 'deposit' THEN txn_amount
ELSE -txn_amount
END AS amount
FROM customer_transactions
),
balance AS (
SELECT customer_id, month, month_number,
SUM(amount) OVER(PARTITION BY customer_id, month_number ORDER BY customer_id, month_number 
ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance
FROM updated_transactions
),

avg_running AS(
SELECT customer_id, month,month_number,
AVG(running_balance) AS avg_balance
FROM balance
GROUP BY 1,2,3
ORDER BY 1

)
SELECT month_number,month, 
SUM(CASE WHEN avg_balance < 0 THEN 0 ELSE avg_balance END) AS allocation_balance
FROM avg_running
GROUP BY 1,2
ORDER by 1,2;


-- Option 3: Data is updated real-time
WITH updated_transactions AS (
SELECT customer_id, txn_type,
EXTRACT(MONTH FROM(txn_date)) AS month_number,
MONTHNAME(txn_date) AS month,
CASE
WHEN txn_type = 'deposit' THEN txn_amount
ELSE -txn_amount
END AS amount
FROM customer_transactions
),
balance AS (
SELECT customer_id, month_number, month, 
SUM(amount) OVER(PARTITION BY customer_id, month_number ORDER BY customer_id, month_number ASC 
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance
FROM updated_transactions
)
SELECT month_number, month,
SUM(CASE WHEN running_balance < 0 THEN 0 ELSE running_balance END) AS total_allocation
FROM balance
GROUP BY 1,2
ORDER BY 1;


    WITH adjusted_amount AS (
SELECT customer_id, 
EXTRACT(MONTH FROM(txn_date)) AS month_number,
MONTHNAME(txn_date) AS month,
SUM(CASE 
WHEN txn_type = 'deposit' THEN txn_amount
ELSE -txn_amount
END) AS monthly_amount
FROM customer_transactions
GROUP BY 1,2,3
ORDER BY 1
),
interest AS (
SELECT customer_id, month_number,month, monthly_amount,
ROUND(((monthly_amount * 6 * 1)/(100 * 12)),2) AS interest
FROM adjusted_amount
GROUP BY 1,2,3,4
ORDER BY 1,2,3
),
total_earnings AS (
SELECT customer_id, month_number, month,
(monthly_amount + interest) as earnings
FROM  interest
GROUP BY 1,2,3,4
ORDER BY 1,2,3
)
SELECT month_number,month,
SUM(CASE WHEN earnings < 0 THEN 0 ELSE earnings END) AS allocation
FROM total_earnings
GROUP BY 1,2
ORDER BY 1,2;
/* Change-Over-Time =====================================
Analyze how a measure evolves over time.
Helps track trends and identify seasonality in your data.
>> Summation[MEASURE] By[DATE DIMENSION]
*/

-- Analyze Sales Performance Over Time. (day level)
SELECT
	order_date,
	SUM(sales_amount) AS total_sales
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY order_date
ORDER BY order_date

-- Analyze (Sales Performance + customers + quantity) Over Time. (year level)
SELECT
	YEAR(order_date) AS order_year,
	SUM(sales_amount) AS total_sales,
	COUNT(DISTINCT customer_key) AS total_customers,
	SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date)
ORDER BY order_year

-- Analyze (Sales Performance + customers + quantity) Over Time. (month level)
-- Aggregating all the data from all years
SELECT
	MONTH(order_date) AS order_month,
	SUM(sales_amount) AS total_sales,
	COUNT(DISTINCT customer_key) AS total_customers,
	SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY MONTH(order_date)
ORDER BY order_month

-- Analyze (Sales Performance + customers + quantity) Over Time. (month level)
-- Aggregating all the data for each years
SELECT
	YEAR(order_date) AS order_year,
	MONTH(order_date) AS order_month,
	SUM(sales_amount) AS total_sales,
	COUNT(DISTINCT customer_key) AS total_customers,
	SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY order_year, order_month

-- OR (best way)
SELECT
	DATETRUNC(MONTH, order_date) AS order_date,
	SUM(sales_amount) AS total_sales,
	COUNT(DISTINCT customer_key) AS total_customers,
	SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(MONTH, order_date)
ORDER BY order_date

-- OR (using FORMAT, it outputs STRING hence unsorted result)
SELECT
	FORMAT(order_date, 'yyyy-MMM') AS order_date,
	SUM(sales_amount) AS total_sales,
	COUNT(DISTINCT customer_key) AS total_customers,
	SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY FORMAT(order_date, 'yyyy-MMM')
ORDER BY order_date

-- How many new customers were added each year
SELECT
	DATETRUNC(YEAR, create_date) AS create_year,
	COUNT(customer_key) AS total_customer
FROM gold.dim_customers
GROUP BY DATETRUNC(YEAR, create_date)
ORDER BY create_year

/* Cumulative Analysis =====================================
Aggregate the data progressively over time.
Helps to understand whether our business is growing or declining.
>> Summation[CUMULATIVE MEASURE] By[DATE DIMENSION]
*/

-- Calculate the total sales per month
-- and the running total of sales over time
SELECT
	order_month,
	total_sales,
	SUM(total_sales) OVER(ORDER BY order_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total_sales
FROM 
(
	SELECT
		DATETRUNC(MONTH, order_date) AS order_month,
		SUM(sales_amount) AS total_sales
	FROM gold.fact_sales
	WHERE order_date IS NOT NULL
	GROUP BY DATETRUNC(MONTH, order_date)
)t

-- Calculate the total sales per month but for each years
-- and the running total of sales over time
SELECT
	order_month,
	total_sales,
	SUM(total_sales) OVER(PARTITION BY DATETRUNC(YEAR, order_month) ORDER BY order_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total_sales
FROM 
(
	SELECT
		DATETRUNC(MONTH, order_date) AS order_month,
		SUM(sales_amount) AS total_sales
	FROM gold.fact_sales
	WHERE order_date IS NOT NULL
	GROUP BY DATETRUNC(MONTH, order_date)
)t

-- Calculate the AVG price per month
-- and the moving average of price over time
SELECT
	order_month,
	average_price,
	AVG(average_price) OVER(ORDER BY order_month) AS moving_average_price
FROM 
(
	SELECT
		DATETRUNC(MONTH, order_date) AS order_month,
		AVG(price) AS average_price
	FROM gold.fact_sales
	WHERE order_date IS NOT NULL
	GROUP BY DATETRUNC(MONTH, order_date)
)t

/* Performance Analysis =====================================
Comparing the current value to a target value.
Helps measure success and compare performance.
>> current[MEASURE] - target[MEASURE]
*/

-- Analyze the yearly performance of products by comparing
-- each product's sales to both its average sales 
-- performance and the previous year's sales.
WITH yearly_product_sales AS (
	SELECT
		YEAR(f.order_date) AS order_year,
		p.product_name,
		SUM(f.sales_amount) AS current_sales
	FROM gold.fact_sales AS f
	LEFT JOIN gold.dim_products AS p
	ON f.product_key = p.product_key
	WHERE f.order_date IS NOT NULL
	GROUP BY YEAR(f.order_date), p.product_name
)
SELECT
	order_year,
	product_name,
	current_sales,
	AVG(current_sales) OVER(PARTITION BY product_name) AS average_sales,
	current_sales - AVG(current_sales) OVER(PARTITION BY product_name) AS diff_avg,
	CASE WHEN current_sales - AVG(current_sales) OVER(PARTITION BY product_name) > 0 THEN 'Above Avg'
		 WHEN current_sales - AVG(current_sales) OVER(PARTITION BY product_name) < 0 THEN 'Below Avg'
		 ELSE 'Avg'
	END AS avg_change,
	-- Year-over-year Analysis
	LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) AS previous_year_sales,
	current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) AS diff_previous_year,
	CASE WHEN LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase'
		 WHEN LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease'
		 ELSE 'No Change'
	END AS previous_year_change
FROM yearly_product_sales
ORDER BY product_name, order_year

/* Part-To-Whole (Proportional Analysis) ==================
Proportion of the part relative to whole.
Analyze how an individual part is performing compared to the overall,
allowing us to understand which category has the greatest impact on the business.
>> ([MEASURE] / total[MEASURE]) * 100 by [DIMENSION]
*/

-- Which categories contribute the most to overall sales?
WITH category_sales AS (
SELECT
	p.category AS category,
	SUM(f.sales_amount) AS total_sales
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_products AS p
ON f.product_key = p.product_key
GROUP BY p.category
)
SELECT 
	category,
	total_sales,
	SUM(total_sales) OVER() AS overall_sales,
	CONCAT(ROUND((CAST(total_sales AS FLOAT) / SUM(total_sales) OVER())*100, 2), '%')  AS percentage_of_total
FROM category_sales
ORDER BY total_sales DESC

/* Data Segmentation =====================================
Group the data based on a specific range.
Helps understand the correlation between two measures.
>> [MEASURE] by [MEASURE]
*/

-- Segment products into cost ranges and 
-- count how many products fall into each segment
WITH product_segments AS (
	SELECT
		product_key,
		product_name,
		cost,
		CASE WHEN cost < 100 THEN 'Below 100'
			 WHEN cost BETWEEN 100 AND 500 THEN '100-500'
			 WHEN cost BETWEEN 501 AND 1000 THEN '501-1000'
			 ELSE 'Above 1000'
		END AS cost_range
	FROM gold.dim_products
)
SELECT
	cost_range,
	COUNT(product_key) AS total_products
FROM product_segments
GROUP BY cost_range
ORDER BY total_products DESC

-- Group customers into three segments based on their spending behaviour:
-- VIP: Customers with at least 12 months of history and spending more than 5,000.
-- Regular: Customers with at least 12 months of history but spending 5,000 or less.
-- New: Customers with a lifespan less than 12 months.
-- And find the total number of customers by each group
WITH customer_spending AS (
SELECT
	c.customer_key,
	SUM(f.sales_amount) AS total_spending,
	MIN(order_date) AS first_order,
	MAX(order_date) AS last_order,
	DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_customers AS c
ON f.customer_key = c.customer_key
GROUP BY c.customer_key
)
SELECT 
	customer_segment,
	COUNT(customer_key) AS total_customers
FROM (
	SELECT
		customer_key,
		total_spending,
		lifespan,
		CASE WHEN lifespan >= 12 AND total_spending > 5000 THEN 'VIP'
			 WHEN lifespan >= 12 AND total_spending <= 5000 THEN 'Regular'
			 ELSE 'New'
		END AS customer_segment
	FROM customer_spending
)t
GROUP BY customer_segment
ORDER BY total_customers DESC

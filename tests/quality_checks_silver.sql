/*
===========================================================================
Quality Checks
===========================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy,
    and standardization across the 'silver' schemas. It includes checks for:
     - Null or duplicates primary keys.
     - Unwanted spaces in string fields.
     - Data standardization and consistency.
     - Invalid date ranges and orders.
     - Data consistency between related fields.

Usage Notes:
     - Run these checks after data loading Silver Layer.
     - Investigate and resolve any discrepancies found during the checks.
===========================================================================
*/

--=======================================================
-- CHECKING 'silver.crm_cust_info'
--=======================================================

--Check For Nulls or Duplicates in Primary Key
--Expectation: No Result
SELECT
	cst_id,
	Count(*) AS flag
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

-- Check for unwanted spaces in (cst_firstname, cst_lastname, cst_gndr)
--Expectation: No Result
SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

-- Also check for unwanted spaces for cst_key
-- Expectation: No Results
SELECT cst_key
FROM bronze.crm_cust_info
WHERE cst_key != TRIM(cst_key)

-- Data Standardization & Consistency
-- Expectation: No abbreviations but Full-Form
-- For cst_gndr col 
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info
-- For cst_marital_status col
SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info

--=======================================================
-- CHECKING 'silver.crm_prd_info'
--=======================================================

-- Check For Nulls or Duplicates in Primary key
-- Expectation: No Result
SELECT
	prd_id,
	COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- Check for unwanted spaces
-- Expectation: No Results
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for NULLs or Negative Numbers
--Expectation: No Results
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info

-- Check for Invalid Date Orders
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt

--=======================================================
-- CHECKING 'silver.crm_sales_details'
--=======================================================

-- Check for Invalid Dates
SELECT
	NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20500101
OR sls_order_dt < 19000101

-- Check for Invalid Date Orders
SELECT 
*
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

--Check Data Consistency: Between Sales, Quantity, and Price
-- >> Sales = Qunatity * Price
-- >> Values must not be NULL, zero and negative.
SELECT DISTINCT
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0 
ORDER BY sls_sales, sls_quantity, sls_price

--=======================================================
-- CHECKING 'silver.erp_cust_az12'
--=======================================================

-- Identify Out-Of-Range Dates
SELECT DISTINCT
	bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- Data Standardization & Consistency
SELECT DISTINCT
gen
FROM bronze.erp_cust_az12

--=======================================================
-- CHECKING 'silver.erp_loc_a101'
--=======================================================

-- Data Standardization & Consistency
SELECT DISTINCT
	cntry
FROM bronze.erp_loc_a101
ORDER BY cntry

--=======================================================
-- CHECKING 'silver.erp_px_cat_g1v2'
--=======================================================

-- Check for unwanted spaces
SELECT
	*
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-- Data Standardization & Consistency for (cat, subcat, maintenance)
SELECT DISTINCT
	subcat
FROM bronze.erp_px_cat_g1v2

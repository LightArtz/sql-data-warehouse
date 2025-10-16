-- EXPLORING THE ISSUES
-- silver.crm_cust_info
-- 1. Check for Nulls or Duplicates in Primary Key
-- Expectation: No Result
SELECT
	cst_id,
	count(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING count(*) > 1 OR cst_id IS NULL

-- 2. Check for unwanted spaces in string values
-- Expectation: No Results
SELECT cst_gndr
FROM bronze.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr)

-- 3. Check the consistency of values in low cardinality columns (a.k.a limited options)
-- In our data warehouse, we aim to store clear and meaningful values rather than using abbreviated terms (F -> Female)
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;


-- silver.crm_prd_info
SELECT
	prd_id,
	count(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING count(*) > 1 OR prd_id IS NULL

SELECT prd_nm
FROM silver.crm_prd_info
WHERE TRIM(prd_nm) != prd_nm

SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

SELECT DISTINCT prd_line
FROM silver.crm_prd_info

SELECT * FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

SELECT 
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')

select * from silver.crm_prd_info


-- silver.crm_sales_details
SELECT
NULLIF(sls_due_dt, 0)
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
OR LEN(sls_due_dt) != 8
OR sls_due_dt > 20500101
OR sls_due_dt < 19000101

SELECT * FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

SELECT DISTINCT sls_sales, sls_quantity, sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price


-- silver.erp_cust_az12
SELECT DISTINCT bdate FROM silver.erp_cust_az12 WHERE bdate < '1924-01-01' OR bdate > GETDATE()

SELECT DISTINCT gen FROM silver.erp_cust_az12


-- silver.erp_loc_a101
SELECT
	REPLACE(cid, '-', '') AS cid,
	cntry
FROM bronze.erp_loc_a101

SELECT DISTINCT cntry as old_cntry, 
	CASE
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END AS cntry
FROM bronze.erp_loc_a101
ORDER BY cntry

SELECT DISTINCT cntry FROM silver.erp_loc_a101 ORDER BY cntry

SELECT * FROM silver.erp_loc_a101


-- silver.px_cat_g1v2
select * from bronze.erp_px_cat_g1v2 ORDER BY id

select cat_id FROM silver.crm_prd_info ORDER BY cat_id

SELECT * FROM bronze.erp_px_cat_g1v2
WHERE maintenance != TRIM(maintenance)

SELECT DISTINCT
maintenance
FROM bronze.erp_px_cat_g1v2

SELECT * FROM silver.erp_px_cat_g1v2

SELECT COUNT(*) FROM silver.erp_px_cat_g1v2

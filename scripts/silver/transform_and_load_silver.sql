INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END AS cst_marital_status,
    CASE
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        ELSE 'n/a'
    END AS cst_gndr,
    cst_create_date::DATE
FROM (
    SELECT *,
           DENSE_RANK() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS last_flag
    FROM bronze.crm_cust_info
) AS ranked
WHERE last_flag = 1;
-- select prd_id,count(*) from bronze.crm_prd_info group by prd_id having count(*)>1 or prd_id is NULL;
-- select substring(prd_key,1,5) from bronze.crm_prd_info;
-- select distinct prd_line from bronze.crm_prd_info;
-- select * from bronze.crm_prd_info where prd_start_dt > prd_end_dt;

INSERT INTO silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
SELECT 
	prd_id,
	REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
	SUBSTRING(prd_key,7,LENGTH(prd_key)) AS prd_key,
	prd_nm,
	COALESCE(prd_cost,0) as prd_cost,
	CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'n/a'
	END AS prd_line,
	prd_start_dt,
	LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 as prd_end_dt
FROM bronze.crm_prd_info;


INSERT INTO silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
)
SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE
		WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT)<8 THEN NULL
		ELSE TO_DATE(sls_order_dt::TEXT,'YYYYMMDD')
	END AS sls_order_dt,
	CASE
		WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT)<8 THEN NULL
		ELSE TO_DATE(sls_ship_dt::TEXT,'YYYYMMDD')
	END AS sls_ship_dt,
	CASE
		WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT)<8 THEN NULL
		ELSE TO_DATE(sls_due_dt::TEXT,'YYYYMMDD')
	END AS sls_due_dt,
	CASE
		WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales <> sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	CASE
		WHEN sls_price IS NULL OR sls_price <=0 THEN ABS(sls_sales/COALESCE(sls_quantity,0))
		ELSE sls_price
	END AS sls_price
FROM bronze.crm_sales_details;

-- select * from bronze.crm_sales_details
-- where sls_sales <> sls_quantity * sls_price
-- or sls_sales <0 or sls_sales is NULL;

INSERT INTO silver.erp_cust_az12(
	cid,
	bdate,
	gen
)
SELECT 
	CASE
		WHEN cid ILIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid::TEXT))
    	ELSE cid
	END AS cid,
	CASE
		WHEN bdate > CURRENT_DATE THEN NULL
		ELSE bdate
	END AS bdate,
    CASE 
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        ELSE 'n/a'
    END AS gen	
FROM bronze.erp_cust_az12;


INSERT INTO silver.erp_loc_a101(
	cid,
	cntry
)
SELECT 
	REPLACE(cid,'-','') as cid,
	CASE
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END AS cntry
FROM bronze.erp_loc_a101;


INSERT INTO silver.erp_px_cat_g1v2(
	id,
	cat,
	subcat,
	maintenance
)
SELECT 
	id,
	cat,
	subcat,
	maintenance
FROM bronze.erp_px_cat_g1v2;

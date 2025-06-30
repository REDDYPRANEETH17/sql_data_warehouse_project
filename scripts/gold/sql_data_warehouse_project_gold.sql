-- Surrogate key -> Created for the uniqueness of the row's in a table where it doesn't hold any value. Can be created using Window Functions etc.
-- to connect the internal schema we create them. it on ourselfs where we can create or not or else use the primary key
CREATE OR REPLACE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	er.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE 
		WHEN ci.cst_gndr!='n/a' THEN ci.cst_gndr
		ELSE COALESCE(ca.gen,'n/a')
	END AS gender,
	ca.bdate AS birth_date,
	ci.cst_create_date AS create_date	
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON	ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 er
ON	ci.cst_key = er.cid;


CREATE OR REPLACE VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER (ORDER BY pd.prd_start_dt,pd.prd_key) AS product_key,
	pd.prd_id AS product_id,
	pd.prd_key AS product_number,
	pd.prd_nm AS product_name,
	pd.cat_id AS category_id,
	pdcat.cat AS category,
	pdcat.subcat AS sub_category ,
	pdcat.maintenance AS category_maintenance,
	pd.prd_cost AS product_cost,
	pd.prd_line AS product_line,
	pd.prd_start_dt AS product_start_date
FROM silver.crm_prd_info pd
LEFT JOIN silver.erp_px_cat_g1v2 pdcat
ON	pd.cat_id = pdcat.id
WHERE pd.prd_end_dt IS NULL;


CREATE OR REPLACE VIEW gold.fact_sales AS
SELECT 
	sls.sls_ord_num AS order_number,
	dp.product_key,
	dc.customer_key,
	sls.sls_order_dt AS order_date,
	sls.sls_ship_dt AS shipping_date,
	sls.sls_due_dt AS due_date,
	sls.sls_sales AS product_sales,
	sls.sls_quantity AS quantity,
	sls.sls_price AS price
FROM silver.crm_sales_details sls
LEFT JOIN gold.dim_customers dc
ON	 sls.sls_cust_id = dc.customer_id
LEFT JOIN gold.dim_products dp
ON	sls.sls_prd_key=dp.product_number;



SELECT * FROM gold.dim_customers;
SELECT * FROM gold.dim_products;
SELECT * FROM gold.fact_sales;















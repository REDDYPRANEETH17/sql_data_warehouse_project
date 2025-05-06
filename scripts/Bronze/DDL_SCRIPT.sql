/*
=======================================================================================================
DDL SCRIPT: Creating Bronze Tables
=======================================================================================================
Script Purpose:
              The script creates tables in the "Bronze" Schema, by dropping tables if they
              already exists.
              Run this script to re-define the DDL structure of "Bronze" tables
========================================================================================================
*/

-- Drop and recreate the 'DataWarehouse' database
DO
$$
BEGIN
    IF EXISTS (
        SELECT FROM pg_database WHERE datname = 'datawarehouse'
    ) THEN
        -- Disconnect all active connections to the database
        PERFORM pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = 'datawarehouse'
        AND pid <> pg_backend_pid();

        -- Drop the database
        EXECUTE 'DROP DATABASE datawarehouse';
    END IF;
END;
$$;


-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;

-- Create Schemas
CREATE SCHEMA bronze;

CREATE SCHEMA silver;

CREATE SCHEMA gold;

DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             VARCHAR(100),
    cst_firstname       VARCHAR(100),
    cst_lastname        VARCHAR(100),
    cst_marital_status  VARCHAR(100),
    cst_gndr            VARCHAR(100),
    cst_create_date     DATE,
	row_hash      TEXT UNIQUE,
	cst_data_import_time	TIMESTAMP DEFAULT NOW()
);


DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      VARCHAR(50),
    prd_nm       VARCHAR(50),
    prd_cost     INT,
    prd_line     VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt   DATE,
	row_hash       TEXT UNIQUE,
	cst_data_import_time	TIMESTAMP DEFAULT NOW()
);

DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  VARCHAR(50),
    sls_prd_key  VARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT,
	row_hash      TEXT UNIQUE,
	cst_data_import_time	TIMESTAMP DEFAULT NOW()
);


DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
    cid    VARCHAR(50),
    cntry  VARCHAR(50),
	row_hash       TEXT UNIQUE,
	erp_data_import_time	TIMESTAMP DEFAULT NOW()
);


DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
    cid    VARCHAR(50),
    bdate  DATE,
    gen    VARCHAR(50),
	row_hash       TEXT UNIQUE,
	erp_data_import_time	TIMESTAMP DEFAULT NOW()
);


DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           VARCHAR(50),
    cat          VARCHAR(50),
    subcat       VARCHAR(50),
    maintenance  VARCHAR(50),
	row_hash       TEXT UNIQUE,
	erp_data_import_time	TIMESTAMP DEFAULT NOW()
);
-- Data Importing done through 
select * from bronze.crm_cust_info;
select * from bronze.crm_prd_info; 
select * from bronze.crm_sales_details;
select * from bronze.erp_cus

select * from bronze.erp_loc_a101;
select * from bronze.erp_px_cat_g1v2;

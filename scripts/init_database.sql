/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    After checking if it already exists, this script creates a new database named 'DataWarehouse'. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

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

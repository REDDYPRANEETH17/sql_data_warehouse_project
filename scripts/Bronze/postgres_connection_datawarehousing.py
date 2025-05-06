#importing necessary libraries
import psycopg2
from datetime import datetime
import hashlib
import csv

# Generating hash to avoid duplicate values
def generating_hash(row,column):
    concat_rows='|'.join('' if row[col] is None else str(row[col]) for col in column)
    return hashlib.md5(concat_rows.encode('utf-8')).hexdigest()

# Connecting to the database
conn = psycopg2.connect(
    dbname="datawarehouse",
    user= "postgres",
    password="Reddy@9949612315",
    host="localhost",
    port="5432"
)

#Creating a cursor
cur = conn.cursor()
tables = [
    {"table":"bronze.crm_cust_info","csv":r"C:\Users\krpra\OneDrive\Desktop\postgreSQL\Data Warehousing\sql-data-warehouse-project\datasets\source_crm\cust_info.csv"},
    {"table":"bronze.crm_prd_info","csv":r"C:\Users\krpra\OneDrive\Desktop\postgreSQL\Data Warehousing\sql-data-warehouse-project\datasets\source_crm\prd_info.csv"},
    {"table":"bronze.crm_sales_details","csv":r"C:\Users\krpra\OneDrive\Desktop\postgreSQL\Data Warehousing\sql-data-warehouse-project\datasets\source_crm\sales_details.csv"},
    {"table":"bronze.erp_cust_az12","csv":r"C:\Users\krpra\OneDrive\Desktop\postgreSQL\Data Warehousing\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv"},
    {"table":"bronze.erp_loc_a101","csv":r"C:\Users\krpra\OneDrive\Desktop\postgreSQL\Data Warehousing\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv"},
    {"table":"bronze.erp_px_cat_g1v2","csv":r"C:\Users\krpra\OneDrive\Desktop\postgreSQL\Data Warehousing\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv"}
]
for table in tables:
    print(f"Importing {table['csv']} into {table['table']}")
    with open(table["csv"],newline='',encoding='UTF-8') as f:
        reader = csv.DictReader(f)
        columns= reader.fieldnames
        for row in reader:
            row_hash=generating_hash(row,columns)
            values = [row[col] if row[col] != '' else None for col in columns] + [row_hash] #[row[col] for col in columns]+[row_hash]
            placeholders=",".join(['%s']*(len(columns)+1))
            sql=f"""
                INSERT INTO {table['table']}({','.join(columns)},row_hash)
                VALUES({placeholders})
                ON CONFLICT (row_hash) DO NOTHING
            """
            cur.execute(sql,values)
    print(f"Finished importing {table['table']}")
conn.commit()
#Close the cursor
cur.close()
#Close the connection
conn.close()
print("All Tables Imported Successfully")
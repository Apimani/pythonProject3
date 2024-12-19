import pandas as pd
from sqlalchemy import create_engine
# import cx_Oracle  # Uncomment if Oracle database is used
from Scripts.config import * # Ensure `Scripts` is a valid Python module with an `__init__.py` file

# Create MySQL engine
mysql_engine = create_engine('mysql+pymysql://root:Tiger@localhost:3306/etl_automation')

# Uncomment and configure for Oracle database if required
# oracle_engine = create_engine('oracle+cx_oracle://system:admin@localhost:1521/xe')
# oracle_engine = create_engine(f'oracle+cx_oracle://{config.ORACLE_USER}:{config.ORACLE_PASSWORD}@{config.ORACLE_HOST}:{config.ORACLE_PORT}/{config.ORACLE_SERVICE}')

def extract_sales_dataSRC_Load_STG():
    """Extract sales data from CSV and load it into the staging_sales table."""
    try:
        df = pd.read_csv("../data/sales_data.csv")
        df.to_sql("staging_sales", mysql_engine, if_exists='replace', index=False)
        print("Sales data loaded successfully.")
    except Exception as e:
        print(f"Error loading sales data: {e}")

def extract_product_dataSRC_Load_STG():
    """Extract product data from CSV and load it into the staging_product table."""
    try:
        df = pd.read_csv("../data/product_data.csv")
        df.to_sql("staging_product", mysql_engine, if_exists='replace', index=False)
        print("Product data loaded successfully.")
    except Exception as e:
        print(f"Error loading product data: {e}")

def extract_supplier_dataSRC_Load_STG():
    """Extract supplier data from JSON and load it into the staging_supplier table."""
    try:
        df = pd.read_json("../data/supplier_data.json")
        df.to_sql("staging_supplier", mysql_engine, if_exists='replace', index=False)
        print("Supplier data loaded successfully.")
    except Exception as e:
        print(f"Error loading supplier data: {e}")

def extract_inventory_dataSRC_Load_STG():
    """Extract inventory data from XML and load it into the staging_inventory table."""
    try:
        df = pd.read_xml("../data/inventory_data.xml", xpath=".//item")
        df.to_sql("staging_inventory", mysql_engine, if_exists='replace', index=False)
        print("Inventory data loaded successfully.")
    except Exception as e:
        print(f"Error loading inventory data: {e}")

# Uncomment if Oracle database functionality is required
'''
def extract_stores_data_OracleSRC_Load_STG():
    """Extract stores data from Oracle database and load it into the staging_stores table."""
    try:
        query = "SELECT * FROM stores"
        df = pd.read_sql(query, oracle_engine)
        df.to_sql("staging_stores", mysql_engine, if_exists='replace', index=False)
        print("Stores data loaded successfully.")
    except Exception as e:
        print(f"Error loading stores data: {e}")
'''

if __name__ == '__main__':
    print("Data extraction started ....")
    extract_sales_dataSRC_Load_STG()
    extract_product_dataSRC_Load_STG()
    extract_supplier_dataSRC_Load_STG()
    extract_inventory_dataSRC_Load_STG()
    # extract_stores_data_OracleSRC_Load_STG()  # Uncomment if required
    print("Data extraction completed ....")

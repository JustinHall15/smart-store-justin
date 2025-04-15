import pandas as pd
import sqlite3
import pathlib
import sys

# For local imports, temporarily add project root to sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from utils.logger import logger

# Constants
DW_DIR = pathlib.Path("data").joinpath("dw")
DB_PATH = DW_DIR.joinpath("smart_sales.db")
PREPARED_DATA_DIR = pathlib.Path("data").joinpath("prepared")

def create_schema(cursor: sqlite3.Cursor) -> None:
    """Create tables in the data warehouse if they don't exist."""
    with open("scripts/create_customer_table.sql", "r") as sql_file:
        sql_customer = sql_file.read()    
    cursor.execute(sql_customer)
    
    with open("scripts/create_product_table.sql", "r") as sql_file:
        sql_product = sql_file.read()
    cursor.execute(sql_product)
    
    with open("scripts/create_sale_table.sql", "r") as sql_file:
        sql_sale = sql_file.read()    
    cursor.execute(sql_sale)
    with open("scripts/create_campaign_table.sql", "r") as sql_file:
        sql_campaign = sql_file.read()
    cursor.execute(sql_campaign)

def delete_existing_records(cursor: sqlite3.Cursor) -> None:
    """Delete all existing records from the customer, product, and sale tables."""
    cursor.execute("DELETE FROM customer")
    cursor.execute("DELETE FROM product")
    cursor.execute("DELETE FROM sale")
    cursor.execute("DELETE FROM campaign")

def insert_campaigns(campaign_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert campaign data into the campaign table."""
    try:
        # Check required columns
        required_columns = {"campaignid", "campaignname", "startdate", "enddate"}
        if not required_columns.issubset(campaign_df.columns):
            logger.error(f"Missing columns in campaign DataFrame: {required_columns - set(campaign_df.columns)}")
            return

        # Map CSV columns to database table columns
        campaign_df = campaign_df.rename(
            columns={
                "campaignid": "campaign_id",  # Map CSV column -> DB column
                "campaignname": "campaign_name",
                "startdate": "start_date",
                "enddate": "end_date"
            }
        )
        campaign_df.to_sql("campaign", cursor.connection, if_exists="append", index=False)
        logger.info("Campaigns data inserted into the campaign table.")
    except sqlite3.Error as e:
        logger.error(f"Error inserting campaigns: {e}")
        raise

def insert_customers(customers_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert customer data into the customer table."""
    try:
        # Check required columns
        required_columns = {"customerid", "name", "region", "loyaltypoints", "gender", "joindate"}
        if not required_columns.issubset(customers_df.columns):
            logger.error(f"Missing columns in customer DataFrame: {required_columns - set(customers_df.columns)}")
            return

        # Map CSV columns to database table columns
        customers_df = customers_df.rename(
            columns={
                "customerid": "customer_id",  # Map CSV column -> DB column
                "loyaltypoints": "loyalty_points",
                "joindate": "join_date"
            }
        )
        customers_df.to_sql("customer", cursor.connection, if_exists="append", index=False)
        logger.info("Customers data inserted into the customer table.")
    except sqlite3.Error as e:
        logger.error(f"Error inserting customers: {e}")
        raise

def insert_products(products_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert product data into the product table."""
    try:
        # Check required columns
        required_columns = {"productid", "productname", "category", "unitprice", "stock", "supplier"}
        if not required_columns.issubset(products_df.columns):
            logger.error(f"Missing columns in products DataFrame: {required_columns - set(products_df.columns)}")
            return

        # Map CSV columns to database table columns
        products_df = products_df.rename(
            columns={
                "productid": "product_id",  # Map CSV column -> DB column
                "productname": "product_name",
                "unitprice": "unit_price"
            }
        )
        products_df.to_sql("product", cursor.connection, if_exists="append", index=False)
        logger.info("Products data inserted into the product table.")
    except sqlite3.Error as e:
        logger.error(f"Error inserting products: {e}")
        raise

def insert_sales(sales_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert sales data into the sales table."""
    try:
        # Check required columns
        required_columns = {"transactionid", "customerid", "productid", "storeid", "campaignid", "saleamount", "bonuspoints", "paymenttype", "saledate"}
        if not required_columns.issubset(sales_df.columns):
            logger.error(f"Missing columns in sales DataFrame: {required_columns - set(sales_df.columns)}")
            return

        # Map CSV columns to database table columns
        sales_df = sales_df.rename(
            columns={
                "transactionid": "sale_id",  # Map CSV column -> DB column
                "customerid": "customer_id",
                "productid": "product_id",
                "storeid": "store_id",
                "campaignid": "campaign_id",
                "saleamount": "sale_amount",
                "bonuspoints": "bonus_points",
                "paymenttype": "payment_type",
                "saledate": "sale_date"
            }
        )
        sales_df.to_sql("sale", cursor.connection, if_exists="append", index=False)
        logger.info("Sales data inserted into the sale table.")
    except sqlite3.Error as e:
        logger.error(f"Error inserting sales: {e}")
        raise

def load_data_to_db() -> None:
    try:
        # Connect to SQLite – will create the file if it doesn't exist
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Create schema and clear existing records
        create_schema(cursor)
        delete_existing_records(cursor)

        # Load prepared data using pandas
        campaign_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("campaign_data_prepared.csv"))
        customers_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("customers_data_prepared.csv"))
        products_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("products_data_prepared.csv"))
        sales_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("sales_data_prepared.csv"))

        # Insert data into the database
        insert_campaigns(campaign_df, cursor)
        insert_customers(customers_df, cursor)
        insert_products(products_df, cursor)
        insert_sales(sales_df, cursor)

        conn.commit()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    load_data_to_db()
"""
Module 2: Initial Script to Verify Project Setup
File: scripts/data_prep.py
"""

import pathlib
import sys
import pandas as pd

# For local imports, temporarily add project root to Python sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Now we can import local modules
from utils.logger import logger
from scripts.data_scrubber import DataScrubber

# Constants
DATA_DIR: pathlib.Path = PROJECT_ROOT.joinpath("data")
RAW_DATA_DIR: pathlib.Path = DATA_DIR.joinpath("raw")
PREPARED_DATA_DIR: pathlib.Path = DATA_DIR.joinpath("prepared")

def read_raw_data(file_name: str) -> pd.DataFrame:
    """Read raw data from CSV."""
    file_path: pathlib.Path = RAW_DATA_DIR.joinpath(file_name)
    try:
        logger.info(f"Reading raw data from {file_path}.")
        return pd.read_csv(file_path)
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return pd.DataFrame()  # Return an empty DataFrame if the file is not found
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        return pd.DataFrame()  # Return an empty DataFrame if any other error occurs
    
def save_prepared_data(df: pd.DataFrame, file_name: str) -> None:
    """
    Save cleaned data to CSV.
    """
    #logger.info(f"FUNCTION START: save_prepared_data with file_name={file_name}, dataframe shape={df.shape}")
    file_path = PREPARED_DATA_DIR.joinpath(file_name)
    df.to_csv(file_path, index=False)
    logger.info(f"Data saved to {file_path}")

def process_data(file_name: str) -> None:
    """Process raw data by reading it into a pandas DataFrame object."""
    df = read_raw_data(file_name)
    df_scrubber = DataScrubber(df)
    logger.info(f"Data before cleaning: {df_scrubber.check_data_consistency_before_cleaning()}")

    #Expected column names and formatting
    products_column_info = {
        "productid": "id",
        "productname": "str",
        "category": "str",
        "unitprice": "float",
        "stock": "int",
        "supplier": "str"
    }
    customers_column_info = {
        "customerid": "id",
        "name": "str",
        "region": "str",
        "joindate": "datetime",
        "loyaltypoints": "str",
        "gender": "str"
    }
    sales_column_info = {
        "transactionid": "id",
        "saledate": "datetime",
        "customerid": "id",
        "productid": "id",
        "storeid": "id",
        "campaignid": "id",
        "saleamount": "float",
        "bonuspoints": "int",
        "paymenttype": "str"
    }

    column_info = sales_column_info if "sales" in file_name else \
                  products_column_info if "products" in file_name else \
                  customers_column_info

    #Column titles should be in lowercase
    original_columns = df.columns.tolist()
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

    # Remove duplicates
    df = df_scrubber.remove_duplicate_records()
    
    # Drop columns not in the expected list
    expected_columns = list(column_info.keys())
    df = df_scrubber.drop_columns([col for col in df.columns if col not in expected_columns])

    # Handle missing values
    for col, string in column_info.items():
        if col in df.columns:
            if string == "str":
                df = df_scrubber.handle_missing_data(col, "UNKNOWN")
                #df = df.fillna("UNKNOWN")
            elif string == "int":
                df = df_scrubber.handle_missing_data(col, 0)
                #df = df.fillna(0)
            elif string == "float":
                df = df_scrubber.handle_missing_data(col, 0.0)
                #df = df.fillna(0.0)
            elif string == "datetime":
                df = df_scrubber.handle_missing_data(col, "0/0/0000")
                #df = df.fillna("0/0/0000")
            elif string == "id":
                df = df_scrubber.handle_missing_data(col, True)

    # Format columns to match expected types
    for col, dtype in column_info.items():
        if col in df.columns:
            if dtype == "str":
                df = df_scrubber.convert_column_to_new_data_type(col, str)
                df = df_scrubber.format_column_strings_to_upper_and_trim(col)
            elif dtype == "float":
                df = df_scrubber.convert_column_to_new_data_type(col, float)
            elif dtype == "int":
                df = df_scrubber.convert_column_to_new_data_type(col, int)
            elif dtype == "datetime":
                df = df_scrubber.parse_dates_to_add_standard_datetime(col)
                df = df_scrubber.drop_columns([col])
                df = df_scrubber.rename_columns({"StandardDateTime": col})
            elif dtype == "id":
                df = df_scrubber.convert_column_to_new_data_type(col, int)

    # Remove outliers and handle invalid values
    '''
    for col, dtype in column_info.items():
        if col in df.columns:
            if dtype == "int" or dtype == "float":
                # Check for negative values and remove them
                df = df_scrubber.filter_column_outliers(col, 0, df[col].max())
                # Remove outliers using IQR method
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR               
                df = df_scrubber.filter_column_outliers(col, lower_bound, upper_bound)
    '''
    logger.info(f"Data after cleaning: {df_scrubber.check_data_consistency_after_cleaning()}")

    # Save cleaned data
    save_prepared_data(df, file_name.replace(".csv", "_prepared.csv"))



def main() -> None:
    """Main function for processing customer, product, and sales data."""
    logger.info("Starting data preparation...")
    process_data("customers_data.csv")
    process_data("products_data.csv")
    process_data("sales_data.csv")
    logger.info("Data preparation complete.")

if __name__ == "__main__":
    main()
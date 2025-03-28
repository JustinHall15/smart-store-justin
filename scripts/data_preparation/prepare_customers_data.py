#Imports
import pathlib
import sys
import pandas as pd


#Adds the project directory to the path if it's not already there
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

#Import from the project
from utils.logger import logger

# Constants
DATA_DIR: pathlib.Path = PROJECT_ROOT.joinpath("data")
RAW_DATA_DIR: pathlib.Path = DATA_DIR.joinpath("raw")
PREPARED_DATA_DIR: pathlib.Path = DATA_DIR.joinpath("prepared")

# Functions

def read_raw_data(file_name: str) -> pd.DataFrame:
    """
    Reads a raw data file and returns a pandas DataFrame
    """
    logger.info(f"FUNCTION START: read_raw_data file: {file_name}")
    file_path = RAW_DATA_DIR.joinpath(file_name)
    logger.info(f"Reading data from: {file_path}")
    df = pd.read_csv(file_path)
    logger.info(f"Loaded dataframe with {len(df)} rows and {len(df.columns)} columns")
    return df

def save_prepared_data(df: pd.DataFrame, file_name: str) -> None:
    """
    Save cleaned data to CSV.
    """
    logger.info(f"FUNCTION START: save_prepared_data with file_name={file_name}, dataframe shape={df.shape}")
    file_path = PREPARED_DATA_DIR.joinpath(file_name)
    df.to_csv(file_path, index=False)
    logger.info(f"Data saved to {file_path}")

def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate rows from the DataFrame.
    """
    logger.info(f"FUNCTION START: remove_duplicates with dataframe shape={df.shape}")
    initial_count = len(df)
    df = df.drop_duplicates()
    removed_count = initial_count - len(df)
    logger.info(f"Removed {removed_count} duplicate rows")
    logger.info(f"{len(df)} records remaining after removing duplicates.")
    return df

def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle missing values by filling or dropping.
    """
    logger.info(f"FUNCTION START: handle_missing_values with dataframe shape={df.shape}")
    
    # Log missing values count before handling
    missing_before = df.isna().sum().sum()
    logger.info(f"Total missing values before handling: {missing_before}")
    
    # TODO: Fill or drop missing values based on business rules
    # Example:
    df['Name'].fillna('Unknown', inplace=True)
    df['Region'].fillna('Unknown', inplace=True)
    df['JoinDate'].fillna('0/0/0000', inplace=True)
    df['LoyaltyPoints'].fillna('0', inplace=True)
    df['Gender'].fillna('Unknown', inplace=True)
    logger.info(f"Blank values filled in")
    df.dropna(subset=['CustomerID'], inplace=True)
    logger.info(f"Records with missing CustomerID dropped")
    
    # Log missing values count after handling
    missing_after = df.isna().sum().sum()
    logger.info(f"Total missing values after handling: {missing_after}")
    logger.info(f"{len(df)} records remaining after handling missing values.")
    return df

def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove outliers based on thresholds.
    """
    logger.info(f"FUNCTION START: remove_outliers with dataframe shape={df.shape}")
    initial_count = len(df)
    
    # TODO: Define numeric columns and apply rules for outlier removal
    # Example:
    # Convert LoyaltyPoints to numeric, coercing errors to NaN
    df['LoyaltyPoints'] = pd.to_numeric(df['LoyaltyPoints'], errors='coerce')
    # Checks for LoyaltyPoints values outside the range of 0 to 1,000,000
    df = df[(df['LoyaltyPoints'] >= 0) & (df['LoyaltyPoints'] < 1000000)]
    
    """
    # Checks for gender values that are not 'F', 'M', or 'O'
    df = df[(df['Gender'] == 'F') & (df['Gender'] == 'M') & (df['Gender'] == 'O') & (df['Gender'] == 'Unknown')]
    logger.info(f"{len(df)} records remaining after removing Gender outliers.")
    """
    removed_count = initial_count - len(df)
    logger.info(f"Removed {removed_count} outlier rows")
    logger.info(f"{len(df)} records remaining after removing outliers.")
    return df

# Main function
def main() -> None:
    """
    Main function for processing customer data.
    """
    logger.info("==================================")
    logger.info("STARTING prepare_customers_data.py")
    logger.info("==================================")

    logger.info(f"Root project folder: {PROJECT_ROOT}")
    logger.info(f"data / raw folder: {RAW_DATA_DIR}")
    logger.info(f"data / prepared folder: {PREPARED_DATA_DIR}")
    logger.info(f"scripts folder: {PROJECT_ROOT.joinpath('scripts')}")

    input_file = "customers_data.csv"
    output_file = "customers_data_prepared.csv"
    
    # Read raw data
    df = read_raw_data(input_file)

    # Log initial dataframe information
    logger.info(f"Initial dataframe columns: {', '.join(df.columns.tolist())}")
    logger.info(f"Initial dataframe shape: {df.shape}")
    
    # Clean column names
    original_columns = df.columns.tolist()
    df.columns = df.columns.str.strip()
    
    # Log if any column names changed
    changed_columns = [f"{old} -> {new}" for old, new in zip(original_columns, df.columns) if old != new]
    if changed_columns:
        logger.info(f"Cleaned column names: {', '.join(changed_columns)}")

    # Remove duplicates
    df = remove_duplicates(df)

    # Handle missing values
    df = handle_missing_values(df)

    # Remove outliers
    df = remove_outliers(df)

    # Save prepared data
    save_prepared_data(df, output_file)

    logger.info("==================================")
    logger.info("FINISHED prepare_customers_data.py")
    logger.info("==================================")

# -------------------
# Conditional Execution Block
# -------------------

if __name__ == "__main__":
    main()
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
    
    # Log missing values by column before handling
    # NA means missing or "not a number" - ask your AI for details
    missing_by_col = df.isna().sum()
    logger.info(f"Missing values by column before handling:\n{missing_by_col}")
    
    # TODO: Fill or drop missing values based on business rules
    # Example:
    df['productname'].fillna('Unknown', inplace=True)
    df['supplier'].fillna('Unknown', inplace=True)
    df['category'].fillna('General', inplace=True)
    df['unitprice'].fillna(df['unitprice'].median(), inplace=True)
    df['stock'].fillna(0, inplace=True)
    logger.info(f"Blank values filled in")
    df.dropna(subset=['productid'], inplace=True)
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
    # Convert to numeric, coercing errors to NaN
    df['stock'] = pd.to_numeric(df['stock'], errors='coerce')
    df['unitprice'] = pd.to_numeric(df['unitprice'], errors='coerce')    
    # Checks for values outside the range
    for col in ['stock', 'unitprice']:
        if col in df.columns and df[col].dtype in ['int64', 'float64']:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            df = df[(df[col] >= lower_bound) & (df[col] <= upper_bound)]
            logger.info(f"Applied outlier removal to {col}: bounds [{lower_bound}, {upper_bound}]")

    """
    # Checks for gender values that are not 'F', 'M', or 'O'
    df = df[(df['Gender'] == 'F') & (df['Gender'] == 'M') & (df['Gender'] == 'O') & (df['Gender'] == 'Unknown')]
    logger.info(f"{len(df)} records remaining after removing Gender outliers.")
    """
    removed_count = initial_count - len(df)
    logger.info(f"Removed {removed_count} outlier rows")
    logger.info(f"{len(df)} records remaining after removing outliers.")
    return df

def standardize_formats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize the formatting of various columns.
    """
    logger.info(f"FUNCTION START: standardize_formats with dataframe shape={df.shape}")
    
    # TODO: OPTIONAL ADVANCED Implement standardization for product data
    # Suggestion: Consider standardizing text fields, units, and categorical variables
    # Examples (update based on your column names and types):
    # df['product_name'] = df['product_name'].str.title()  # Title case for product names
    # df['category'] = df['category'].str.lower()  # Lowercase for categories
    # df['price'] = df['price'].round(2)  # Round prices to 2 decimal places
    # df['weight_unit'] = df['weight_unit'].str.upper()  # Uppercase units
    
    logger.info("Completed standardizing formats")
    return df

def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate data against business rules.
    """
    logger.info(f"FUNCTION START: validate_data with dataframe shape={df.shape}")
    
    # TODO: Implement data validation rules specific to products
    # Suggestion: Check for valid values in critical fields
    # Example:
    invalid_prices = df[df['unitprice'] < 0].shape[0]
    logger.info(f"Found {invalid_prices} products with negative prices")
    df = df[df['unitprice'] >= 0]
    invalid_stock = df[df['stock'] < 0].shape[0]
    logger.info(f"Found {invalid_stock} products with negative stock")
    df = df[df['stock'] >= 0]

    df.columns = df.columns.str.strip()
    expected_columns = ['productid', 'productname', 'category', 'unitprice', 'stock', 'supplier']
    if list(df.columns) != expected_columns:
        df = df.drop(columns=[col for col in df.columns if col not in expected_columns])
        logger.info(f"Removed columns not in {expected_columns}")

    logger.info("Data validation complete")
    return df

def main() -> None:
    """
    Main function for processing product data.
    """
    logger.info("==================================")
    logger.info("STARTING prepare_products_data.py")
    logger.info("==================================")

    logger.info(f"Root project folder: {PROJECT_ROOT}")
    logger.info(f"data / raw folder: {RAW_DATA_DIR}")
    logger.info(f"data / prepared folder: {PREPARED_DATA_DIR}")

    input_file = "products_data.csv"
    output_file = "products_data_prepared.csv"
    
    # Read raw data
    df = read_raw_data(input_file)

    # Log initial dataframe information
    logger.info(f"Initial dataframe columns: {', '.join(df.columns.tolist())}")
    logger.info(f"Initial dataframe shape: {df.shape}")
    
    # Clean column names
    original_columns = df.columns.tolist()
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    
    # Log if any column names changed
    changed_columns = [f"{old} -> {new}" for old, new in zip(original_columns, df.columns) if old != new]
    if changed_columns:
        logger.info(f"Cleaned column names: {', '.join(changed_columns)}")

    # Process data
    df = remove_duplicates(df)
    df = handle_missing_values(df)
    df = standardize_formats(df)
    df = remove_outliers(df)
    df = validate_data(df)

    # Save prepared data
    save_prepared_data(df, output_file)

    logger.info("==================================")
    logger.info("FINISHED prepare_products_data.py")
    logger.info("==================================")

# -------------------
# Conditional Execution Block
# -------------------

if __name__ == "__main__":
    main()
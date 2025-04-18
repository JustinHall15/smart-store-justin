import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import pathlib
import sys

# For local imports, temporarily add project root to Python sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from utils.logger import logger  # noqa: E402

# Constants
DW_DIR: pathlib.Path = pathlib.Path("data").joinpath("dw")
DB_PATH: pathlib.Path = DW_DIR.joinpath("smart_sales.db")
OLAP_OUTPUT_DIR: pathlib.Path = pathlib.Path("data").joinpath("olap_cubing_outputs")
CUBED_FILE: pathlib.Path = OLAP_OUTPUT_DIR.joinpath("multidimensional_olap_cube.csv")
RESULTS_OUTPUT_DIR: pathlib.Path = pathlib.Path("data").joinpath("results")

# Create output directory for results if it doesn't exist
RESULTS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def get_campaign_lengths() -> dict:
    try:
        conn = sqlite3.connect(DB_PATH)
        query = """

        """
        sales_df = pd.read_sql_query(query, conn)
        conn.close()
        logger.info("Sales data successfully loaded from SQLite data warehouse.")
        
    except Exception as e:
        logger.error(f"Error loading sale table data from data warehouse: {e}")
        raise        

def load_olap_cube(file_path: pathlib.Path) -> pd.DataFrame:
    """Load the precomputed OLAP cube data."""
    try:
        cube_df = pd.read_csv(file_path)
        logger.info(f"OLAP cube data successfully loaded from {file_path}.")
        return cube_df
    except Exception as e:
        logger.error(f"Error loading OLAP cube data: {e}")
        raise


def analyze_top_product_by_weekday(cube_df: pd.DataFrame) -> pd.DataFrame:
    """Identify the product with the highest revenue for each day of the week."""
    try:
        # Group by DayOfWeek and product_id, sum the sales
        grouped = cube_df.groupby(["DayOfWeek", "product_id"])["sale_amount_usd_sum"].sum().reset_index()
        grouped.rename(columns={"sale_amount_usd_sum": "TotalSales"}, inplace=True)

        # Sort within each day to find the top product
        top_products = grouped.sort_values(["DayOfWeek", "TotalSales"], ascending=[True, False]).groupby("DayOfWeek").head(1)
        logger.info("Top products identified for each day of the week.")
        return top_products
    except Exception as e:
        logger.error(f"Error analyzing top product by DayOfWeek: {e}")
        raise


def visualize_sales_by_weekday_and_product(cube_df: pd.DataFrame) -> None:
    """Visualize total sales by day of the week, broken down by product."""
    try:
        # Pivot the data to organize sales by DayOfWeek and ProductID
        sales_pivot = cube_df.pivot_table(
            index="DayOfWeek",
            columns="product_id",
            values="sale_amount_usd_sum",
            aggfunc="sum",
            fill_value=0
        )

        # Plot the stacked bar chart
        sales_pivot.plot(
            kind="bar",
            stacked=True,
            figsize=(12, 8),
            colormap="tab10"
        )

        plt.title("Total Sales by Day of the Week and Product", fontsize=16)
        plt.xlabel("Day of the Week", fontsize=12)
        plt.ylabel("Total Sales (USD)", fontsize=12)
        plt.xticks(rotation=45)
        plt.legend(title="Product ID", bbox_to_anchor=(1.05, 1), loc="upper left")
        plt.tight_layout()

        # Save the visualization
        output_path = RESULTS_OUTPUT_DIR.joinpath("sales_by_day_and_product.png")
        plt.savefig(output_path)
        logger.info(f"Stacked bar chart saved to {output_path}.")
        plt.show()
    except Exception as e:
        logger.error(f"Error visualizing sales by day and product: {e}")
        raise


def main():
    """Main function for analyzing and visualizing top product sales by day of the week."""
    logger.info("Starting SALES_TOP_PRODUCT_BY_WEEKDAY analysis...")

    # Step 1: Load the precomputed OLAP cube
    cube_df = load_olap_cube(CUBED_FILE)

    # Step 2: Analyze top products by DayOfWeek
    top_products = analyze_top_product_by_weekday(cube_df)
    print(top_products)

    # Step 3: Visualize the results
    visualize_sales_by_weekday_and_product(cube_df)
    logger.info("Analysis and visualization completed successfully.")


if __name__ == "__main__":
    main()
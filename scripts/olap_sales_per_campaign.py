import pandas as pd
import matplotlib.pyplot as plt
import pathlib
import sys

# For local imports, temporarily add project root to Python sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from utils.logger import logger  # noqa: E402

# Constants
OLAP_OUTPUT_DIR: pathlib.Path = pathlib.Path("data").joinpath("olap_cubing_outputs")
CUBED_FILE: pathlib.Path = OLAP_OUTPUT_DIR.joinpath("multidimensional_olap_cube.csv")
RESULTS_OUTPUT_DIR: pathlib.Path = pathlib.Path("data").joinpath("results")

# Create output directory for results if it doesn't exist
RESULTS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def load_olap_cube(file_path: pathlib.Path) -> pd.DataFrame:
    """Load the precomputed OLAP cube data."""
    try:
        cube_df = pd.read_csv(file_path)
        logger.info(f"OLAP cube data successfully loaded from {file_path}.")
        return cube_df
    except Exception as e:
        logger.error(f"Error loading OLAP cube data: {e}")
        raise


def analyze_sales_by_campaign(cube_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate total sales by DayOfWeek."""
    try:
        # Group by campaign_name and sum the sales
        sales_by_campaign = (
            cube_df.groupby("campaign_name")["sale_amount_sum"].sum().reset_index()
        )
        sales_by_campaign.rename(columns={"sale_amount_sum": "TotalSales"}, inplace=True)
        sales_by_campaign.sort_values(by="TotalSales", inplace=True)
        logger.info("Sales aggregated by DayOfWeek successfully.")
        return sales_by_campaign
    except Exception as e:
        logger.error(f"Error analyzing sales by campaign_name: {e}")
        raise


def identify_least_profitable_campaign(sales_by_campaign: pd.DataFrame) -> str:
    """Identify the day with the lowest total sales revenue."""
    try:
        least_profitable_campaign = sales_by_campaign.iloc[0]
        logger.info(
            f"Least profitable campaign: {least_profitable_campaign['campaign_name']} with revenue ${least_profitable_campaign['TotalSales']:.2f}."
        )
        return least_profitable_campaign["campaign_name"]
    except Exception as e:
        logger.error(f"Error identifying least profitable campaign: {e}")
        raise


def visualize_sales_by_campaign(sales_by_campaign: pd.DataFrame) -> None:
    """Visualize total sales by day of the week."""
    try:
        plt.figure(figsize=(10, 6))
        plt.bar(
            sales_by_campaign["campaign_name"],
            sales_by_campaign["TotalSales"],
            color="skyblue",
        )
        plt.title("Total Sales by Campaign", fontsize=16)
        plt.xlabel("Campaign", fontsize=12)
        plt.ylabel("Total Sales (USD)", fontsize=12)
        plt.xticks(rotation=45)
        plt.tight_layout()

        # Save the visualization
        output_path = RESULTS_OUTPUT_DIR.joinpath("sales_by_campaign_of_week.png")
        plt.savefig(output_path)
        logger.info(f"Visualization saved to {output_path}.")
        plt.show()
    except Exception as e:
        logger.error(f"Error visualizing sales by day of the week: {e}")
        raise


def main():
    """Main function for analyzing and visualizing sales data."""
    logger.info("Starting SALES PER CAMPAIGN analysis...")

    # Step 1: Load the precomputed OLAP cube
    cube_df = load_olap_cube(CUBED_FILE)

    # Step 2: Analyze total sales by DayOfWeek
    sales_by_campaign = analyze_sales_by_campaign(cube_df)

    # Step 3: Identify the least profitable day
    least_profitable_campaign = identify_least_profitable_campaign(sales_by_campaign)
    logger.info(f"Least profitable campaign: {least_profitable_campaign}")
    logger.info("Close the Figure to complete this script.")

    # Step 4: Visualize total sales by DayOfWeek
    visualize_sales_by_campaign(sales_by_campaign)
    logger.info("Analysis and visualization completed successfully.")


if __name__ == "__main__":
    main()
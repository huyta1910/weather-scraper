import pandas as pd
import sqlite3
from datetime import datetime
import os
import pandas as pd
from src.utils import setup_database_and_folders, ingest_to_database, save_to_csv, save_text_notifications
from src.scraper import extract_district, scrape_data_for_branch, generate_rain_summary, LOCATIONS

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BRANCHES_FILE = os.path.join(BASE_DIR, "data", "branches", "branches_icool.csv")

branches_df = pd.read_csv(BRANCHES_FILE)
DB_FILE = "weather_forecasts.db"

def run_weather_job():
    print(f"\n--- Running weather job at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
    
    setup_database_and_folders()
    
    # Load branches
    branches_df = pd.read_csv(BRANCHES_FILE)
    branches_df["district"] = branches_df["address"].apply(extract_district)
    
    all_weather_dataframes = []
    all_summaries = []
    
    with sqlite3.connect(DB_FILE) as conn:
        for _, branch_row in branches_df.iterrows():
            district = branch_row["district"]
            if district not in LOCATIONS:
                print(f"Skipping branch {branch_row['branch']} - district not in LOCATIONS list: {district}")
                continue
            
            print(f"\n--- Processing branch: {branch_row['branch']} ({district}) ---")
            try:
                df = scrape_data_for_branch(branch_row, LOCATIONS[district])
                if df.empty:
                    print(f"No data scraped for {branch_row['branch']}. Skipping.")
                    continue

                summaries = generate_rain_summary(df)
                ingest_to_database(conn, df, summaries)

                all_weather_dataframes.append(df)
                all_summaries.extend(summaries)

                for summary in summaries:
                    print(f"  -> {summary['summary_text']}")

            except Exception as e:
                print(f"[CRITICAL ERROR] Failed to process {branch_row['branch']} ({district}). Reason: {e}")
    
    if all_weather_dataframes:
        final_weather_df = pd.concat(all_weather_dataframes, ignore_index=True)
        save_to_csv(final_weather_df, all_summaries)
        
        # This call passes the full dataset to the report generator
        save_text_notifications(final_weather_df, all_summaries) 
    else:
        print("No data collected, skipping CSV export and notifications.")

    print("\n--- Job finished successfully. ---")


if __name__ == "__main__":
    run_weather_job()


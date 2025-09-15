import requests
import pandas as pd
from datetime import date, timedelta
import os
import re
import glob

# --- CONFIGURATION ---
BRANCH_CSV_PATH = 'data/branches/branches_icool.csv'
HISTORICAL_REPORTS_FOLDER = 'data/historical_reports'
TODAY_REPORTS_FOLDER = 'data/today_weather_data_reports'

HISTORICAL_HOURLY_VARIABLES = [
    "temperature_2m", "relativehumidity_2m", "apparent_temperature", "precipitation",
    "rain", "weathercode", "cloudcover", "windspeed_10m"
]
MINUTELY_15_VARIABLES = [
    "temperature_2m", "relativehumidity_2m", "precipitation", "weathercode",
    "windspeed_10m"
]
WMO_WEATHER_CODES = {
    0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast", 45: "Fog",
    48: "Depositing rime fog", 51: "Drizzle: Light", 53: "Drizzle: Moderate", 55: "Drizzle: Dense",
    56: "Freezing Drizzle: Light", 57: "Freezing Drizzle: Dense", 61: "Rain: Slight",
    63: "Rain: Moderate", 65: "Rain: Heavy", 66: "Freezing Rain: Light", 67: "Freezing Rain: Heavy",
    71: "Snow fall: Slight", 73: "Snow fall: Moderate", 75: "Snow fall: Heavy", 77: "Snow grains",
    80: "Rain showers: Slight", 81: "Rain showers: Moderate", 82: "Rain showers: Violent",
    85: "Snow showers: Slight", 86: "Snow showers: Heavy", 95: "Thunderstorm: Slight or moderate",
    96: "Thunderstorm with slight hail", 99: "Thunderstorm with heavy hail",
}

# --- HELPER FUNCTIONS ---

def sanitize_filename(name):
    name = name.replace(' ', '_')
    return re.sub(r'[^\w\-]', '', name)

def find_latest_file(pattern):
    try:
        return max(glob.glob(pattern), key=os.path.getctime)
    except ValueError:
        return None

# --- API FETCHING FUNCTIONS (No changes) ---

def fetch_historical_weather(latitude, longitude, start_date, end_date):
    base_url = "https://archive-api.open-meteo.com/v1/archive"
    params = {"latitude": latitude, "longitude": longitude, "start_date": start_date, "end_date": end_date, "hourly": ",".join(HISTORICAL_HOURLY_VARIABLES), "timezone": "auto"}
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()['hourly']
        df = pd.DataFrame(data)
        df.rename(columns={'time': 'datetime'}, inplace=True)
        df['datetime'] = pd.to_datetime(df['datetime'])
        df['weather_condition'] = df['weathercode'].map(WMO_WEATHER_CODES).fillna('Unknown')
        return df
    except (requests.exceptions.RequestException, KeyError) as e:
        print(f"    -> Historical API Error: {e}")
    return None

def fetch_today_15min_weather(latitude, longitude):
    base_url = "https://api.open-meteo.com/v1/forecast"
    today_str = date.today().strftime("%Y-%m-%d")
    params = {"latitude": latitude, "longitude": longitude, "minutely_15": ",".join(MINUTELY_15_VARIABLES), "start_date": today_str, "end_date": today_str, "timezone": "auto"}
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        json_data = response.json()
        minutely_data = json_data['minutely_15']
        timezone = json_data['timezone']
        df = pd.DataFrame(minutely_data)
        df.rename(columns={'time': 'datetime'}, inplace=True)
        df['datetime'] = pd.to_datetime(df['datetime']).dt.tz_localize(timezone)
        df['weather_condition'] = df['weathercode'].map(WMO_WEATHER_CODES).fillna('Unknown')
        now = pd.Timestamp.now(tz=timezone)
        return df[df['datetime'] <= now].copy()
    except (requests.exceptions.RequestException, KeyError) as e:
        print(f"    -> 15-Minute Data API Error: {e}")
    return None

# --- CORE LOGIC FUNCTIONS ---

def run_historical_fetch(locations_df):
    print("\n--- Starting Historical Data Fetch (Last 60 Days) ---")
    os.makedirs(HISTORICAL_REPORTS_FOLDER, exist_ok=True)
    today = date.today()
    end_date = today
    start_date = today - timedelta(days=720)
    start_str, end_str = start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
    print(f"Fetching data for the period: {start_str} to {end_str}")
    for _, row in locations_df.iterrows():
        branch_name, lat, lon = row['branch'], row['latitude'], row['longitude']
        print(f"-> Processing historical data for: {branch_name}...")
        weather_df = fetch_historical_weather(lat, lon, start_str, end_str)
        if weather_df is not None and not weather_df.empty:
            filename = f"{sanitize_filename(branch_name)}_historical_{start_str}_to_{end_str}.csv"
            path = os.path.join(HISTORICAL_REPORTS_FOLDER, filename)
            weather_df.to_csv(path, index=False)
            print(f"  Saved to '{path}'")
        else:
            print(f"  Failed for {branch_name}.")

def run_today_15min_fetch(locations_df):
    print("\n--- Starting Today's 15-Minute Data Fetch ---")
    os.makedirs(TODAY_REPORTS_FOLDER, exist_ok=True)
    today_str = date.today().strftime("%Y-%m-%d")
    for _, row in locations_df.iterrows():
        branch_name, lat, lon = row['branch'], row['latitude'], row['longitude']
        print(f"-> Fetching today's weather data for: {branch_name}...")
        weather_df = fetch_today_15min_weather(lat, lon)
        if weather_df is not None and not weather_df.empty:
            filename = f"{sanitize_filename(branch_name)}_today_{today_str}.csv"
            path = os.path.join(TODAY_REPORTS_FOLDER, filename)
            weather_df.to_csv(path, index=False)
            print(f"  Saved {len(weather_df)} records to '{path}'")
        else:
            print(f"  Failed for {branch_name}.")

# --- NEW, FOCUSED RAINFALL ANALYSIS FUNCTION ---

def analyze_precipitation_summary(df, interval_minutes):
    """Helper function to calculate rainfall metrics from a dataframe."""
    if df is None or df.empty:
        return 0, 0, 0
        
    # Use a small threshold to count rainy periods
    rainy_periods = df[df['precipitation'] > 0.1]
    
    total_precip = df['precipitation'].sum()
    duration_minutes = len(rainy_periods) * interval_minutes
    peak_precip = rainy_periods['precipitation'].max() if not rainy_periods.empty else 0
    
    return total_precip, duration_minutes, peak_precip

def run_rainfall_analysis(locations_df):
    """
    Provides a detailed summary and comparison of rainfall for today vs. 60 days ago.
    """
    print("\n--- Starting Detailed Rainfall Analysis & Comparison ---")
    today = date.today()
    compare_date = today - timedelta(days=31)

    for _, row in locations_df.iterrows():
        branch_name = row['branch']
        s_branch_name = sanitize_filename(branch_name)
        print(f"\n{'='*20} ANALYSIS FOR: {branch_name.upper()} {'='*20}")

        today_file = find_latest_file(os.path.join(TODAY_REPORTS_FOLDER, f'{s_branch_name}_today_*.csv'))
        hist_file = find_latest_file(os.path.join(HISTORICAL_REPORTS_FOLDER, f'{s_branch_name}_historical_*.csv'))

        if not today_file or not hist_file:
            print("  [Warning] Missing data files. Please run option 3 to fetch them first.")
            continue

        # --- Analyze Today's Data ---
        today_df = pd.read_csv(today_file)
        today_total, today_duration, today_peak = analyze_precipitation_summary(today_df, 15)

        # --- Analyze Historical Data ---
        hist_df = pd.read_csv(hist_file)
        hist_df['datetime'] = pd.to_datetime(hist_df['datetime'])
        hist_day_df = hist_df[hist_df['datetime'].dt.date == compare_date]
        hist_total, hist_duration, hist_peak = analyze_precipitation_summary(hist_day_df, 60)

        # --- Print Report ---
        print(f"  [+] Today's Rainfall Summary ({today.strftime('%Y-%m-%d')}):")
        print(f"      - Total Precipitation: {today_total:.2f} mm")
        print(f"      - Duration of Rain:    {today_duration // 60}h {today_duration % 60}m")
        print(f"      - Peak Intensity:      {today_peak:.2f} mm in a 15-min interval")

        print(f"\n  [+] Historical Summary ({compare_date.strftime('%Y-%m-%d')}):")
        print(f"      - Total Precipitation: {hist_total:.2f} mm")
        print(f"      - Duration of Rain:    {hist_duration // 60}h {hist_duration % 60}m")
        print(f"      - Peak Intensity:      {hist_peak:.2f} mm in an hour")

        # --- Comparison ---
        print("\n  [!] Comparison Highlights:")
        total_diff = today_total - hist_total
        duration_diff = today_duration - hist_duration

        if abs(total_diff) < 1.0:
            print("      - Total rainfall is similar to 60 days ago.")
        else:
            direction = "more" if total_diff > 0 else "less"
            print(f"      - Received {abs(total_diff):.2f} mm {direction} rainfall today.")
        
        if abs(duration_diff) < 30:
            print("      - The duration of rain was comparable.")
        else:
            direction = "longer" if duration_diff > 0 else "shorter"
            print(f"      - Rain events today were significantly {direction}.")
        print(f"{'='*58}")


# --- MAIN MENU ---

def main():
    try:
        locations_df = pd.read_csv(BRANCH_CSV_PATH)
        print(f"Successfully loaded {len(locations_df)} branches from '{BRANCH_CSV_PATH}'.")
    except FileNotFoundError:
        print(f"Error: Branch location file not found at '{BRANCH_CSV_PATH}'.")
        return

    while True:
        print("\n--- Weather Data Tool Menu ---")
        print("1. Fetch Historical Data (Last 60 days, hourly)")
        print("2. Fetch Today's Data (15-minute intervals)") 
        print("3. Fetch Both Historical and Today's Data")
        print("4. Rainfall Analysis & Comparison") # NEW FOCUSED OPTION
        print("5. Exit")
        
        choice = input("Enter your choice (1-5): ")
        
        if choice == '1':
            run_historical_fetch(locations_df)
        elif choice == '2':
            run_today_15min_fetch(locations_df) 
        elif choice == '3':
            run_historical_fetch(locations_df)
            run_today_15min_fetch(locations_df)
        elif choice == '4':
            run_rainfall_analysis(locations_df) # Call the new function
        elif choice == '5':
            print("Exiting tool.")
            break
        else:
            print("Invalid choice. Please enter a number between 1 and 5.")

if __name__ == "__main__":
    main()
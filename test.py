import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import os
import re

# --- CONFIGURATION ---
BRANCHES_FILE = r"D:\weather-forecast\data\branches\branches_icool.csv" # Use your actual path
DB_FILE = "weather_forecasts.db"
CSV_OUTPUT_FOLDER = "weather_reports"
RAIN_KEYWORDS = ["mưa", "dông", "giông", "mưa rào"]

# Mapping forecast days to labels for clarity
DAY_LABELS = {
    1: "hôm nay",
    2: "ngày mai",
    3: "2 ngày tới"
}

# Mapping districts → URLs
LOCATIONS = {
    "Quận 1": "https://www.accuweather.com/vi/vn/district-1/3554433/hourly-weather-forecast/3554433?day={}",
    "Quận 2": "https://www.accuweather.com/vi/vn/district-2/3554434/hourly-weather-forecast/3554434?day={}",
    "Quận 3": "https://www.accuweather.com/vi/vn/district-3/3554435/hourly-weather-forecast/3554435?day={}",
    "Quận 5": "https://www.accuweather.com/vi/vn/district-5/3554437/hourly-weather-forecast/3554437?day={}",
    "Quận 6": "https://www.accuweather.com/vi/vn/district-6/3554438/hourly-weather-forecast/3554438?day={}",
    "Quận 8": "https://www.accuweather.com/vi/vn/district-8/3554440/hourly-weather-forecast/3554440?day={}",
    "Quận 10": "https://www.accuweather.com/vi/vn/district-10/3554442/hourly-weather-forecast/3554442?day={}",
    "Quận 12": "https://www.accuweather.com/vi/vn/district-12/3554444/hourly-weather-forecast/3554444?day={}",
    "Bình Thạnh": "https://www.accuweather.com/vi/vn/binh-thanh/1696411/hourly-weather-forecast/1696411?day={}",
    "Tân Phú": "https://www.accuweather.com/vi/vn/tan-phu/3554445/hourly-weather-forecast/3554445?day={}",
    "Tân Bình": "https://www.accuweather.com/vi/vn/tan-binh/416036/hourly-weather-forecast/416036?day={}",
    "Phú Nhuận": "https://www.accuweather.com/vi/vn/phu-nhuan/418146/hourly-weather-forecast/418146?day={}",
    "TP Thủ Đức": "https://www.accuweather.com/vi/vn/thu-duc/414495/hourly-weather-forecast/414495?day={}",
    "TP Vũng Tàu": "https://www.accuweather.com/vi/vn/vung-tau/352089/hourly-weather-forecast/352089?day={}"
}

# --- HELPERS AND SETUP ---

def setup_database_and_folders():
    if not os.path.exists(CSV_OUTPUT_FOLDER):
        os.makedirs(CSV_OUTPUT_FOLDER)
        print(f"Created folder: {CSV_OUTPUT_FOLDER}")
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS weather_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT, scraped_at TIMESTAMP, branch TEXT, address TEXT, 
                latitude REAL, longitude REAL, district TEXT, forecast_day TEXT, hour TEXT, 
                temperature TEXT, content TEXT, wind TEXT, humidity TEXT, uv_index TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS daily_summaries (
                id INTEGER PRIMARY KEY AUTOINCREMENT, scraped_at TIMESTAMP, branch TEXT, address TEXT, 
                latitude REAL, longitude REAL, district TEXT, forecast_day TEXT, summary_text TEXT
            )
        ''')

def extract_district(address: str):
    match = re.search(r"(Quận\s?\d+|Bình Thạnh|Tân Bình|Phú Nhuận|Tân Phú|TP Thủ Đức|TP Vũng Tàu)", str(address), re.IGNORECASE)
    return match.group(0) if match else None

# --- CORE DATA PROCESSING FUNCTIONS ---

def scrape_data_for_branch(branch_row, base_url):
    headers = { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36" }
    rows = []
    for day_code, day_label in DAY_LABELS.items():
        url = base_url.format(day_code)
        try:
            res = requests.get(url, headers=headers, timeout=15)
            res.raise_for_status()
            soup = BeautifulSoup(res.text, "html.parser")
            for hourly in soup.select("div.accordion-item.hour"):
                panel_items = hourly.select(".panel.no-realfeel-phrase p")
                panel_dict = {p.contents[0].strip().replace(":", ""): (p.select_one(".value").get_text(strip=True) if p.select_one(".value") else "") for p in panel_items}
                rows.append({
                    "branch": branch_row["branch"], "address": branch_row["address"], "latitude": branch_row["latitude"], 
                    "longitude": branch_row["longitude"], "district": branch_row["district"], "forecast_day": day_label,
                    "hour": hourly.select_one(".date").get_text(strip=True), "temperature": hourly.select_one(".temp.metric").get_text(strip=True),
                    "content": hourly.select_one(".phrase").get_text(strip=True), "wind": panel_dict.get("Gió", ""),
                    "humidity": panel_dict.get("Độ ẩm", ""), "uv_index": panel_dict.get("Chỉ số UV tối đa", "")
                })
        except (requests.RequestException, AttributeError) as e:
            print(f" [ERROR] Could not scrape {branch_row['branch']} ({branch_row['district']}) for day {day_code}. Reason: {e}")
            continue
    return pd.DataFrame(rows)

def generate_rain_summary(df):
    summaries = []
    if df.empty: return summaries
    for (branch, forecast_day), group in df.groupby(['branch', 'forecast_day']):
        rainy_hours = group[group['content'].str.contains('|'.join(RAIN_KEYWORDS), case=False, na=False)]
        summary_text = (f"Dự báo cho {branch} ({group['district'].iloc[0]}) {forecast_day}: Có khả năng mưa vào các giờ: {', '.join(rainy_hours['hour'].tolist())}."
                        if not rainy_hours.empty else f"Dự báo cho {branch} ({group['district'].iloc[0]}) {forecast_day}: Trời không mưa.")
        summaries.append({
            "branch": group["branch"].iloc[0], "address": group["address"].iloc[0], "latitude": group["latitude"].iloc[0],
            "longitude": group["longitude"].iloc[0], "district": group["district"].iloc[0], "forecast_day": forecast_day,
            "summary_text": summary_text
        })
    return summaries

def ingest_to_database(conn, weather_df, summaries_list):
    scraped_at = datetime.now()
    if not weather_df.empty:
        weather_df.assign(scraped_at=scraped_at).to_sql('weather_data', conn, if_exists='append', index=False)
    if summaries_list:
        pd.DataFrame(summaries_list).assign(scraped_at=scraped_at).to_sql('daily_summaries', conn, if_exists='append', index=False)

def save_to_csv(weather_df, summaries_list):
    timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    if not weather_df.empty:
        weather_df.to_csv(os.path.join(CSV_OUTPUT_FOLDER, f"hourly_weather_{timestamp_str}.csv"), index=False, encoding='utf-8-sig')
    if summaries_list:
        summaries_df = pd.DataFrame(summaries_list)
        day_order = {"hôm nay": 1, "ngày mai": 2, "2 ngày tới": 3}
        summaries_df["day_order"] = summaries_df["forecast_day"].map(day_order)
        summaries_df.sort_values(["day_order", "branch"]).drop(columns="day_order").to_csv(
            os.path.join(CSV_OUTPUT_FOLDER, f"rain_summaries_{timestamp_str}.csv"), index=False, encoding='utf-8-sig')

# --- REPORT GENERATION FUNCTIONS ---

def _group_consecutive_hours(hours):
    """
    MODIFIED: A helper function to group a list of integer hours into DURATION RANGES.
    - A single hour (e.g., 7) becomes a one-hour period: "07h-08h".
    - Consecutive hours (e.g., 7, 8) become a combined period: "07h-09h".
    - Handles the 23h to 00h wrap-around.
    """
    if not hours:
        return []
    
    sorted_hours = sorted(list(set(hours)))
    ranges = []
    start_of_range = sorted_hours[0]

    for i in range(1, len(sorted_hours)):
        # If the current hour is not consecutive, it's the end of a range.
        if sorted_hours[i] != sorted_hours[i-1] + 1:
            end_of_range = sorted_hours[i-1]
            end_time = (end_of_range + 1) % 24
            ranges.append(f"{start_of_range:02d}h-{end_time:02d}h")
            start_of_range = sorted_hours[i]

    # After the loop, format and add the very last range.
    end_of_range = sorted_hours[-1]
    end_time = (end_of_range + 1) % 24
    ranges.append(f"{start_of_range:02d}h-{end_time:02d}h")
        
    return ranges

def generate_dynamic_report(all_weather_df, rain_keywords, forecast_day_code=1):
    report_date = datetime.now() + timedelta(days=forecast_day_code - 1)
    date_str = report_date.strftime("%d/%m/%Y")
    report_parts = [f"Thông báo: 📢 THÔNG BÁO DỰ BÁO THỜI TIẾT ({date_str})"]
    
    day_label = DAY_LABELS.get(forecast_day_code)
    df_day = all_weather_df[all_weather_df['forecast_day'] == day_label]
    if df_day.empty: 
        return f"Không có dữ liệu dự báo cho {day_label} ({date_str})."

    forecast_groups = {}
    for district_name in df_day['district'].unique():
        df_district = df_day[df_day['district'] == district_name]
        rainy_hours = df_district[df_district['content'].str.contains('|'.join(rain_keywords), case=False, na=False)]
        signature = tuple(sorted(rainy_hours['hour'].str.replace('h', '', regex=False).astype(int).unique()))
        if signature not in forecast_groups: forecast_groups[signature] = []
        forecast_groups[signature].append(district_name)

    sorted_groups = sorted(forecast_groups.items(), key=lambda item: len(item[1]), reverse=True)
    is_first_rain_group, rain_reported = True, False
    for signature, districts in sorted_groups:
        if not signature: continue
        rain_reported = True
        hour_ranges_str = ', '.join(_group_consecutive_hours(list(signature)))
        if is_first_rain_group and len(districts) > 1:
            label = "Toàn hệ thống"
            is_first_rain_group = False
        else:
            label = ", ".join([d.replace('TP ', '') for d in districts])
        report_parts.append(f"{label}: Mưa dông {hour_ranges_str}.")
    
    if not rain_reported:
        report_parts.append("Toàn hệ thống: Trời không mưa.")
    report_parts.append("Lưu ý: Các chi nhánh cần chuẩn bị vật dụng OMOTENASHI hỗ trợ khách dành cho trời mưa.")
    return "\n".join(report_parts)

def save_text_notifications(weather_df, summaries_list):
    print("\nGenerating 3-day notification reports...")
    for day_code, day_label in DAY_LABELS.items():
        timestamp_str = datetime.now().strftime("%Y-%m-%d")
        filename = os.path.join(CSV_OUTPUT_FOLDER, f"report_{day_label}_{timestamp_str}.txt")
        report_text = generate_dynamic_report(weather_df, RAIN_KEYWORDS, forecast_day_code=day_code)
        with open(filename, "w", encoding="utf-8") as f:
            f.write(report_text)
        print(f" - Saved report for '{day_label}' to {filename}")

# --- MAIN WORKFLOW ---

def run_weather_job():
    print(f"\n--- Running weather job at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
    setup_database_and_folders()
    
    try:
        branches_df = pd.read_csv(BRANCHES_FILE)
        branches_df["district"] = branches_df["address"].apply(extract_district)
    except FileNotFoundError:
        print(f"[CRITICAL ERROR] The file '{BRANCHES_FILE}' was not found. Please create it and run again.")
        return

    all_weather_dataframes, all_summaries = [], []
    with sqlite3.connect(DB_FILE) as conn:
        for _, branch_row in branches_df.iterrows():
            district = branch_row["district"]
            if not district or district not in LOCATIONS:
                print(f"Skipping branch '{branch_row['branch']}' - district '{district}' not found or not supported.")
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
                    print(f" -> {summary['summary_text']}")
            except Exception as e:
                print(f"[CRITICAL ERROR] Failed to process {branch_row['branch']} ({district}). Reason: {e}")

    if all_weather_dataframes:
        final_weather_df = pd.concat(all_weather_dataframes, ignore_index=True)
        save_to_csv(final_weather_df, all_summaries)
        save_text_notifications(final_weather_df, all_summaries)
    else:
        print("\nNo data collected, skipping CSV export and notifications.")
    
    print("\n--- Job finished successfully. ---")

if __name__ == "__main__":
    run_weather_job()


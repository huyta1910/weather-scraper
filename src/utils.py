import os
import sqlite3
import pandas as pd
from datetime import datetime

DB_FILE = "weather_forecasts.db"
CSV_OUTPUT_FOLDER = "weather_reports"

DAY_LABELS = {
    1: "hôm nay",
    2: "ngày mai",
    3: "2 ngày tới"
}

RAIN_KEYWORDS = ["mưa", "dông", "giông", "mưa rào"]

def setup_database_and_folders():
    """Create DB tables + CSV folder if missing"""
    if not os.path.exists(CSV_OUTPUT_FOLDER):
        os.makedirs(CSV_OUTPUT_FOLDER)
        print(f"Created folder: {CSV_OUTPUT_FOLDER}")
        
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS weather_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT, scraped_at TIMESTAMP, 
                branch TEXT, address TEXT, latitude REAL, longitude REAL, district TEXT,
                forecast_day TEXT, hour TEXT, temperature TEXT, content TEXT, 
                wind TEXT, humidity TEXT, uv_index TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS daily_summaries (
                id INTEGER PRIMARY KEY AUTOINCREMENT, scraped_at TIMESTAMP, 
                branch TEXT, address TEXT, latitude REAL, longitude REAL, district TEXT,
                forecast_day TEXT, summary_text TEXT
            )
        ''')

def ingest_to_database(conn, weather_df, summaries_list):
    """Insert weather + summaries into DB"""
    scraped_at_timestamp = datetime.now()

    if not weather_df.empty:
        weather_df['scraped_at'] = scraped_at_timestamp
        cols = ['scraped_at','branch','address','latitude','longitude','district',
                'forecast_day','hour','temperature','content','wind','humidity','uv_index']
        weather_df[cols].to_sql('weather_data', conn, if_exists='append', index=False)

    if summaries_list:
        summaries_df = pd.DataFrame(summaries_list)
        summaries_df['scraped_at'] = scraped_at_timestamp
        cols = ['scraped_at','branch','address','latitude','longitude','district',
                'forecast_day','summary_text']
        summaries_df[cols].to_sql('daily_summaries', conn, if_exists='append', index=False)

def save_to_csv(weather_df, summaries_list):
    """Save raw + summary to CSV files"""
    timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    if not weather_df.empty:
        hourly_filename = os.path.join(CSV_OUTPUT_FOLDER, f"hourly_weather_{timestamp_str}.csv")
        weather_df.to_csv(hourly_filename, index=False, encoding='utf-8-sig')
        print(f"  - Saved hourly data to {hourly_filename}")
        
    if summaries_list:
        summaries_df = pd.DataFrame(summaries_list)
        day_order = {"hôm nay": 1, "ngày mai": 2, "2 ngày tới": 3}
        summaries_df["day_order"] = summaries_df["forecast_day"].map(day_order)
        summaries_df = summaries_df.sort_values(["day_order", "branch"]).drop(columns="day_order")
        
        summary_filename = os.path.join(CSV_OUTPUT_FOLDER, f"rain_summaries_{timestamp_str}.csv")
        summaries_df.to_csv(summary_filename, index=False, encoding='utf-8-sig')
        print(f"  - Saved summaries to {summary_filename}")

def generate_rain_summary(df):
    """Generate rain summary per branch/day"""
    summaries = []
    if df.empty: return summaries
    for (branch, forecast_day), group in df.groupby(['branch', 'forecast_day']):
        rainy_hours = group[group['content'].str.contains('|'.join(RAIN_KEYWORDS), case=False, na=False)]
        summary = (f"Dự báo cho {branch} ({group['district'].iloc[0]}) {forecast_day}: "
                   f"Có khả năng mưa vào các giờ: {', '.join(rainy_hours['hour'].tolist())}." 
                   if not rainy_hours.empty else 
                   f"Dự báo cho {branch} ({group['district'].iloc[0]}) {forecast_day}: Trời không mưa.")
        summaries.append({
            "branch": group["branch"].iloc[0],
            "address": group["address"].iloc[0],
            "latitude": group["latitude"].iloc[0],
            "longitude": group["longitude"].iloc[0],
            "district": group["district"].iloc[0],
            "forecast_day": forecast_day,
            "summary_text": summary
        })
    return summaries

def save_text_notifications(weather_df, summaries_list):
    """
    Calls the DYNAMIC report generator and saves the result to a text file.
    """
    print("Generating dynamic, image-style text report...")
    timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = os.path.join(CSV_OUTPUT_FOLDER, f"report_notification_{timestamp_str}.txt")

    # Call the new dynamic report generator for today (forecast_day=1)
    report_text = generate_dynamic_report(weather_df, RAIN_KEYWORDS, forecast_day=1)
    
    with open(filename, "w", encoding="utf-8") as f:
        f.write(report_text)
        
    print(f" - Saved dynamic report to {filename}")

# --- NEW HELPER AND REPORT GENERATION FUNCTIONS ---

def _group_consecutive_hours(hours):
    """
    A helper function to group a list of integer hours into ranges.
    Example: [3, 4, 5, 9, 11, 12] -> ["03h-05h", "09h", "11h-12h"]
    """
    if not hours:
        return []
    
    # Use list(set(hours)) to get unique values before sorting
    sorted_hours = sorted(list(set(hours)))
    ranges = []
    start_of_range = sorted_hours[0]
    
    for i in range(1, len(sorted_hours)):
        if sorted_hours[i] != sorted_hours[i-1] + 1:
            end_of_range = sorted_hours[i-1]
            if start_of_range == end_of_range:
                # Use f-string formatting to ensure leading zero (e.g., 03h)
                ranges.append(f"{start_of_range:02d}h")
            else:
                ranges.append(f"{start_of_range:02d}h-{end_of_range:02d}h")
            start_of_range = sorted_hours[i]
            
    # Add the last range
    end_of_range = sorted_hours[-1]
    if start_of_range == end_of_range:
        ranges.append(f"{start_of_range:02d}h")
    else:
        ranges.append(f"{start_of_range:02d}h-{end_of_range:02d}h")
        
    return ranges

def generate_dynamic_report(all_weather_df, rain_keywords, forecast_day=1):
    """
    Generates a text report by dynamically grouping districts with identical rain forecasts.
    The largest group of rainy districts becomes "Toàn hệ thống".
    """
    report_parts = ["Thông báo: 📢 THÔNG BÁO DỰ BÁO THỜI TIẾT"]
    
    # Filter for the specific day to report on
    day_label = DAY_LABELS.get(forecast_day)
    df_day = all_weather_df[all_weather_df['forecast_day'] == day_label].copy()
    if df_day.empty:
        return "Không có dữ liệu dự báo cho hôm nay."

    # --- Core Dynamic Logic ---
    # 1. Determine the rain "signature" (the exact hours of rain) for each district.
    forecast_groups = {}
    for district_name in df_day['district'].unique():
        df_district = df_day[df_day['district'] == district_name]
        rainy_hours = df_district[df_district['content'].str.contains('|'.join(rain_keywords), case=False, na=False)]
        
        # The signature is a sorted tuple of rainy hours, e.g., (14, 15, 19)
        signature = tuple(sorted(rainy_hours['hour'].str.replace('h', '', regex=False).astype(int).unique()))
        
        # Group districts by their signature
        if signature not in forecast_groups:
            forecast_groups[signature] = []
        forecast_groups[signature].append(district_name)

    # 2. Process the groups to build the report.
    # Sort groups by size (number of districts) in descending order.
    # This puts the most common forecast pattern first.
    sorted_groups = sorted(forecast_groups.items(), key=lambda item: len(item[1]), reverse=True)
    
    is_first_rain_group = True
    rain_reported = False

    for signature, districts in sorted_groups:
        # Skip the "no rain" group (empty signature)
        if not signature:
            continue
        
        rain_reported = True
        hour_ranges_str = ', '.join(_group_consecutive_hours(list(signature)))
        
        # The first and largest group (with more than 1 member) is labeled "Toàn hệ thống"
        if is_first_rain_group and len(districts) > 1:
            label = "Toàn hệ thống"
            is_first_rain_group = False
        else:
            # All other groups are listed by their district names
            # Clean up names like "TP Thủ Đức" -> "Thủ Đức" for display
            cleaned_districts = [d.replace('TP ', '') for d in districts]
            label = ", ".join(cleaned_districts)
            
        report_parts.append(f"{label}: Mưa dông {hour_ranges_str}.")

    if not rain_reported:
        report_parts.append("Toàn hệ thống: Trời không mưa.")
        
    # --- Footer ---
    report_parts.append("Lưu ý: Mưa trùng các khung giờ đón khách, các chi nhánh cần chuẩn bị vật dụng OMOTENASHI hỗ trợ khách.")
    
    return "\n".join(report_parts)

def generate_notification_report(all_weather_df, rain_keywords, forecast_day=1):
    """
    Generates a formatted text report that mimics the structure of the provided image.
    """
    
    # --- Configuration for the report ---
    SYSTEM_DISTRICTS = [
        "Quận 1", "Quận 2", "Quận 3", "Quận 5", "Quận 6",
        "Quận 8", "Quận 10", "Quận 12", "Bình Thạnh", 
        "Tân Phú", "Tân Bình", "Phú Nhuận"
    ]
    SPECIFIC_DISTRICTS = {
        "TP Thủ Đức": "(ICOOL Lê Văn Việt, Hoàng Diệu 2, Đại Lộ 2)",
        "TP Vũng Tàu": "(ICOOL Vũng Tàu)"
    }

    report_parts = ["Thông báo: 📢 THÔNG BÁO DỰ BÁO THỜI TIẾT NGÀY "]
    
    # Filter for the specific day we want to report on (e.g., today)
    df_day = all_weather_df[all_weather_df['forecast_day'] == DAY_LABELS[forecast_day]].copy()
    if df_day.empty:
        return "Không có dữ liệu dự báo cho hôm nay."

    # --- 1. Generate "Toàn hệ thống" Section ---
    df_system = df_day[df_day['district'].isin(SYSTEM_DISTRICTS)]
    rainy_system = df_system[df_system['content'].str.contains('|'.join(rain_keywords), case=False, na=False)]
    
    if not rainy_system.empty:
        system_hours = rainy_system['hour'].str.replace('h', '', regex=False).astype(int).unique()
        system_ranges = _group_consecutive_hours(list(system_hours))
        # The image seems to use "Mưa dông" as a generic term for the system-wide summary.
        report_parts.append(f"Toàn hệ thống: Mưa dông {', '.join(system_ranges)}.")
    
    # --- 2. Generate Sections for Specific Locations ---
    for district_name, details in SPECIFIC_DISTRICTS.items():
        df_loc = df_day[df_day['district'] == district_name]
        rainy_loc = df_loc[df_loc['content'].str.contains('|'.join(rain_keywords), case=False, na=False)]
        
        if not rainy_loc.empty:
            rain_by_type_strings = []
            # Group by the actual rain description 
            for rain_type, group in rainy_loc.groupby('content'):
                loc_hours = group['hour'].str.replace('h', '', regex=False).astype(int).unique()
                loc_ranges = _group_consecutive_hours(list(loc_hours))
                if loc_ranges:
                    rain_by_type_strings.append(f"{rain_type} {', '.join(loc_ranges)}")
            
            display_name = district_name.replace('TP ', '')
            report_parts.append(f"{display_name} {details}: {'; '.join(rain_by_type_strings)}.")

    report_parts.append("Lưu ý: Mưa trùng các khung giờ đón khách, các chi nhánh cần chuẩn bị vật dụng OMOTENASHI hỗ trợ khách.")
    
    return "\n".join(report_parts)

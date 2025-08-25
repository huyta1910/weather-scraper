# scraper.py
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup

RAIN_KEYWORDS = ["mưa", "dông", "giông", "mưa rào"]

# Mapping forecast days to labels
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

def extract_district(address: str):
    """Extracts a district name from an address string."""
    match = re.search(r"(Quận\s?\d+|Bình Thạnh|Tân Bình|Phú Nhuận|Tân Phú|TP Thủ Đức|TP Vũng Tàu)", address, re.IGNORECASE)
    return match.group(0) if match else None

def scrape_data_for_branch(branch_row, base_url):
    """Scrapes 3 days of weather data for a branch (by district URL)."""
    headers = { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36" }
    rows = []
    for day in range(1, 4):
        url = base_url.format(day)
        try:
            res = requests.get(url, headers=headers)
            res.raise_for_status()
            soup = BeautifulSoup(res.text, "html.parser")
            for hourly in soup.select("div.accordion-item.hour"):
                hour = hourly.select_one(".date").get_text(strip=True)
                temp = hourly.select_one(".temp.metric").get_text(strip=True)
                phrase = hourly.select_one(".phrase").get_text(strip=True)
                panel_items = hourly.select(".panel.no-realfeel-phrase p")
                panel_dict = {p.contents[0].strip().replace(":", ""): (p.select_one(".value").get_text(strip=True) if p.select_one(".value") else "") for p in panel_items}
                
                rows.append({
                    "branch": branch_row["branch"],
                    "address": branch_row["address"],
                    "latitude": branch_row["latitude"],
                    "longitude": branch_row["longitude"],
                    "district": branch_row["district"],
                    "forecast_day": DAY_LABELS.get(day, str(day)),
                    "hour": hour,
                    "temperature": temp, 
                    "content": phrase,
                    "wind": panel_dict.get("Gió", ""),
                    "humidity": panel_dict.get("Độ ẩm", ""),
                    "uv_index": panel_dict.get("Chỉ số UV tối đa", "")
                })
        except (requests.RequestException, AttributeError) as e:
            print(f"    [ERROR] Could not scrape {branch_row['branch']} ({branch_row['district']}) for day {day}. Reason: {e}")
            continue
    return pd.DataFrame(rows)

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

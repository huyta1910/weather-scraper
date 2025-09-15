import pandas as pd
import numpy as np
import sys

def analyze_weather_data_corrected(file_path='record.csv'):
    """
    Analyzes weather data, correctly calculating statistics across multiple locations per day,
    and generates an accurate summary report focusing on September's top rainfall days.
    """
    try:
        df = pd.read_csv(file_path)
        print("Successfully loaded record.csv.")
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        sys.exit(1)

    # --- 2. Data Cleaning and Preparation ---
    df['datetime'] = pd.to_datetime(df['datetime'])
    df[['feelslikemax', 'feelslikemin', 'feelslike']] = df[['feelslikemax', 'feelslikemin', 'feelslike']].replace(0, np.nan)
    
    # --- 3. CORRECTED Data Analytics ---
    # Group by date to handle multiple locations per day
    daily_data = df.groupby(df['datetime'].dt.date).agg(
        temp=('temp', 'mean'),
        humidity=('humidity', 'mean'),
        precip=('precip', 'sum') # Sum precipitation from all locations for a given day
    ).reset_index()

    daily_data['datetime'] = pd.to_datetime(daily_data['datetime'])
    daily_data['month_name'] = daily_data['datetime'].dt.strftime('%B')

    august_daily = daily_data[daily_data['month_name'] == 'August']
    september_daily = daily_data[daily_data['month_name'] == 'September']

    # --- August Analysis ---
    aug_avg_temp = august_daily['temp'].mean()
    aug_avg_humidity = august_daily['humidity'].mean()
    aug_total_precip = august_daily['precip'].sum()
    aug_rainy_days_df = august_daily[august_daily['precip'] > 0]
    aug_precip_days = len(aug_rainy_days_df)
    aug_total_days = len(august_daily)
    aug_percent_rainy_days = (aug_precip_days / aug_total_days) * 100 if aug_total_days > 0 else 0
    aug_avg_rain_intensity = aug_rainy_days_df['precip'].mean() if aug_precip_days > 0 else 0

    # --- September Analysis ---
    sep_avg_temp = september_daily['temp'].mean()
    sep_avg_humidity = september_daily['humidity'].mean()
    sep_total_precip = september_daily['precip'].sum()
    sep_rainy_days_df = september_daily[september_daily['precip'] > 0]
    sep_precip_days = len(sep_rainy_days_df)
    sep_total_days = len(september_daily)
    sep_percent_rainy_days = (sep_precip_days / sep_total_days) * 100 if sep_total_days > 0 else 0
    sep_avg_rain_intensity = sep_rainy_days_df['precip'].mean() if sep_precip_days > 0 else 0

    # --- MODIFIED: Find Top 5 Rain Events for September ONLY ---
    top_5_rainiest_days_september = september_daily.sort_values(by='precip', ascending=False).head(5)

    # --- 4. Generate the Reports ---
    vietnamese_report_lines = generate_report_vietnamese(
        df, aug_avg_temp, sep_avg_temp, aug_avg_humidity, sep_avg_humidity,
        aug_total_precip, aug_precip_days, aug_total_days, aug_percent_rainy_days, aug_avg_rain_intensity,
        sep_total_precip, sep_precip_days, sep_total_days, sep_percent_rainy_days, sep_avg_rain_intensity,
        top_5_rainiest_days_september
    )
    save_report(vietnamese_report_lines, 'weather_report_final_VI.txt')
    print("Final Vietnamese report saved to 'weather_report_final_VI.txt'")

def generate_report_vietnamese(df, aug_avg_temp, sep_avg_temp, aug_avg_humidity, sep_avg_humidity,
                               aug_total_precip, aug_precip_days, aug_total_days, aug_percent_rainy_days, aug_avg_rain_intensity,
                               sep_total_precip, sep_precip_days, sep_total_days, sep_percent_rainy_days, sep_avg_rain_intensity,
                               top_5_rainiest_days_september):
    report = [
        "=========================================================",
        "   Báo cáo Phân tích Thời tiết (Tập trung vào Lượng mưa)",
        "=========================================================",
        f"\nPhân tích dựa trên dữ liệu từ ngày {df['datetime'].min().date()} đến {df['datetime'].max().date()}.\n",
        "--- Tóm tắt Tổng quan về Thời tiết ---",
        f"Nhiệt độ TB (Tháng 8): {aug_avg_temp:.2f}°C  |  Nhiệt độ TB (Tháng 9): {sep_avg_temp:.2f}°C",
        f"Độ ẩm TB (Tháng 8): {aug_avg_humidity:.2f}%    |  Độ ẩm TB (Tháng 9): {sep_avg_humidity:.2f}%\n",
        "="*25,
        "   PHÂN TÍCH CHI TIẾT VỀ LƯỢNG MƯA",
        "="*25 + "\n",
        "--- Lượng mưa Tháng 8 năm 2025 ---",
        f"Tổng Lượng mưa: {aug_total_precip:.2f} mm",
        f"Số ngày có mưa: {aug_precip_days} trên tổng số {aug_total_days} ngày ({aug_percent_rainy_days:.1f}%)",
        f"Lượng mưa trung bình vào một ngày có mưa: {aug_avg_rain_intensity:.2f} mm/ngày (Cường độ)\n",
        "--- Lượng mưa Tháng 9 năm 2025 ---",
        f"Tổng Lượng mưa: {sep_total_precip:.2f} mm",
        f"Số ngày có mưa: {sep_precip_days} trên tổng số {sep_total_days} ngày ({sep_percent_rainy_days:.1f}%)",
        f"Lượng mưa trung bình vào một ngày có mưa: {sep_avg_rain_intensity:.2f} mm/ngày (Cường độ)\n",
        "--- So sánh Lượng mưa & Thông tin chi tiết ---"
    ]
    if aug_total_precip > sep_total_precip:
        report.append(f"- Tháng 8 là tháng ẩm ướt hơn, với tổng lượng mưa cao hơn {aug_total_precip - sep_total_precip:.2f} mm.")
    else:
        report.append(f"- Tháng 9 là tháng ẩm ướt hơn, với tổng lượng mưa cao hơn {sep_total_precip - aug_total_precip:.2f} mm.")
    
    if aug_percent_rainy_days > sep_percent_rainy_days:
         report.append(f"- Mưa xuất hiện thường xuyên hơn ở Tháng 8, xảy ra vào {aug_percent_rainy_days:.1f}% số ngày so với {sep_percent_rainy_days:.1f}% ở Tháng 9.")
    else:
         report.append(f"- Mưa xuất hiện thường xuyên hơn ở Tháng 9, xảy ra vào {sep_percent_rainy_days:.1f}% số ngày so với {aug_percent_rainy_days:.1f}% ở Tháng 8.")

    if aug_avg_rain_intensity > sep_avg_rain_intensity:
        report.append(f"- Khi có mưa, các trận mưa ở Tháng 8 có cường độ mạnh hơn (trung bình {aug_avg_rain_intensity:.2f} mm/ngày) so với Tháng 9 (trung bình {sep_avg_rain_intensity:.2f} mm/ngày).")
    else:
        report.append(f"- Khi có mưa, các trận mưa ở Tháng 9 có cường độ mạnh hơn (trung bình {sep_avg_rain_intensity:.2f} mm/ngày) so với Tháng 8 (trung bình {aug_avg_rain_intensity:.2f} mm/ngày).")
    
    # MODIFIED section header and data source
    report.append("\n--- 5 Ngày có Tổng Lượng Mưa Lớn Nhất trong Tháng 9 ---")
    for i, (index, row) in enumerate(top_5_rainiest_days_september.iterrows()):
        report.append(f"  {i+1}. Ngày: {row['datetime'].date()}, Tổng Lượng mưa: {row['precip']:.1f} mm")
    
    report.append("\n--- Lưu ý về Chất lượng Dữ liệu ---")
    report.append("Một số bản ghi trong tháng 9 (bắt đầu từ ngày 2025-09-11) bị thiếu chi tiết về lượng mưa. Điều này có nghĩa là tổng lượng mưa và số ngày mưa của tháng 9 có thể bị báo cáo thiếu, ảnh hưởng đến độ chính xác của việc so sánh.")
    report.append("\n========================= Kết thúc Báo cáo =========================")
    return report

def save_report(report_lines, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("\n".join(report_lines))

if __name__ == '__main__':
    analyze_weather_data_corrected()
"""
Script tải và giải nén dữ liệu Citi Bike NY năm 2024.
Hỗ trợ tải 1 tháng (mặc định tháng 01/2024 để thử nghiệm) hoặc tải toàn bộ 12 tháng.
"""

import os
import sys
import zipfile
import urllib.request
import argparse

# Đảm bảo in tiếng Việt không bị lỗi cp1252 trên Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

BASE_URL = "https://s3.amazonaws.com/tripdata"
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
ZIP_DIR = os.path.join(DATA_DIR, "zip")
CSV_DIR = os.path.join(DATA_DIR, "csv")

def download_with_progress(url, dest_path):
    print(f"-> Đang tải: {url}")
    print(f"-> Lưu vào: {dest_path}")
    
    def report(block_num, block_size, total_size):
        downloaded = block_num * block_size
        if total_size > 0:
            percent = downloaded / total_size * 100
            mb_downloaded = downloaded / (1024 * 1024)
            mb_total = total_size / (1024 * 1024)
            sys.stdout.write(f"\r   [{percent:5.1f}%] {mb_downloaded:6.1f} MB / {mb_total:6.1f} MB")
            sys.stdout.flush()
        else:
            mb_downloaded = downloaded / (1024 * 1024)
            sys.stdout.write(f"\r   Đã tải: {mb_downloaded:6.1f} MB")
            sys.stdout.flush()
            
    urllib.request.urlretrieve(url, dest_path, reporthook=report)
    print("\n   ✓ Hoàn tất tải file.")

def extract_and_organize(zip_path, target_month_dir):
    print(f"-> Đang giải nén: {zip_path}")
    os.makedirs(target_month_dir, exist_ok=True)
    with zipfile.ZipFile(zip_path, 'r') as zf:
        # Chỉ giải nén các file CSV liên quan đến NY (bỏ JC hoặc thư mục __MACOSX)
        for member in zf.namelist():
            filename = os.path.basename(member)
            if not filename.endswith('.csv'):
                continue
            if filename.startswith('JC-'):
                print(f"   [Bỏ qua] {filename} (Jersey City, ngoài phạm vi đề tài)")
                continue
            if '__MACOSX' in member:
                continue
            
            dest_file = os.path.join(target_month_dir, filename)
            print(f"   [Giải nén] {filename} -> {target_month_dir}")
            with zf.open(member) as source, open(dest_file, "wb") as target:
                target.write(source.read())
    print("   ✓ Giải nén thành công.")

def process_month(year, month):
    m_str = f"{month:02d}"
    zip_name = f"{year}{m_str}-citibike-tripdata.zip"
    url = f"{BASE_URL}/{zip_name}"
    zip_path = os.path.join(ZIP_DIR, zip_name)
    month_csv_dir = os.path.join(CSV_DIR, f"{year}-{m_str}")
    
    os.makedirs(ZIP_DIR, exist_ok=True)
    os.makedirs(CSV_DIR, exist_ok=True)
    
    if not os.path.exists(zip_path):
        try:
            download_with_progress(url, zip_path)
        except Exception as e:
            print(f"   [LỖI] Không tải được {url}: {e}")
            return False
    else:
        print(f"-> Tệp nén đã tồn tại sẵn: {zip_path}")
        
    extract_and_organize(zip_path, month_csv_dir)
    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tải dữ liệu Citi Bike")
    parser.add_argument("--months", type=str, default="1", 
                        help="Danh sách tháng cần tải, ví dụ: '1' hoặc '1,2,3' hoặc '1-12'")
    parser.add_argument("--seasonal", action="store_true",
                        help="Tải 4 tháng đại diện 4 mùa (01, 04, 07, 10)")
    parser.add_argument("--all", action="store_true",
                        help="Tải toàn bộ 12 tháng năm 2024 (01-12)")
    parser.add_argument("--year", type=int, default=2024, help="Năm dữ liệu (mặc định 2024)")
    args = parser.parse_args()
    
    months_to_run = []
    if args.seasonal:
        months_to_run = [1, 4, 7, 10]
    elif args.all:
        months_to_run = list(range(1, 13))
    elif "-" in args.months:
        start_m, end_m = map(int, args.months.split("-"))
        months_to_run = list(range(start_m, end_m + 1))
    else:
        months_to_run = [int(m.strip()) for m in args.months.split(",")]
        
    print(f"=== Bắt đầu tải dữ liệu Citi Bike năm {args.year} (Tháng: {months_to_run}) ===")
    for m in months_to_run:
        print(f"\n--- Xử lý Tháng {m:02d}/{args.year} ---")
        process_month(args.year, m)
    print("\n=== Hoàn tất toàn bộ yêu cầu tải dữ liệu ===")

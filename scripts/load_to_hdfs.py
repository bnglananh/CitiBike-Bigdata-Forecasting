"""
Script đưa dữ liệu CSV từ thư mục data/csv vào cụm HDFS tầng raw.
Được tổ chức phân vùng theo Hive-style: /citibike/raw/year=YYYY/month=MM/
"""

import os
import sys
import shutil
import subprocess
import argparse

# Đảm bảo in tiếng Việt không bị lỗi cp1252 trên Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

DOCKER_CONTAINER = "citibike-namenode"
DOCKER_EXE = shutil.which("docker") or r"C:\Program Files\Docker\Docker\resources\bin\docker.exe"

def run_hdfs_cmd(cmd_args):
    docker_cmd = [DOCKER_EXE, "exec", DOCKER_CONTAINER] + cmd_args
    # Đảm bảo đường dẫn PATH có docker nếu chạy trên Windows
    env = os.environ.copy()
    docker_bin = r"C:\Program Files\Docker\Docker\resources\bin"
    if docker_bin not in env.get("PATH", ""):
        env["PATH"] = docker_bin + ";" + env.get("PATH", "")
        
    print(f"-> Thực thi: {' '.join(docker_cmd)}")
    result = subprocess.run(docker_cmd, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"   [CẢNH BÁO/LỖI]: {result.stderr.strip()}")
    else:
        if result.stdout.strip():
            print(result.stdout.strip())
    return result.returncode == 0

def load_month_to_hdfs(year, month):
    m_str = f"{month:02d}"
    hdfs_dest = f"/citibike/raw/year={year}/month={m_str}"
    container_csv_src = f"/data/csv/{year}-{m_str}"
    
    print(f"\n--- Đưa dữ liệu tháng {m_str}/{year} lên HDFS ---")
    # 1. Tạo thư mục đích trên HDFS
    run_hdfs_cmd(["hdfs", "dfs", "-mkdir", "-p", hdfs_dest])
    
    # 2. Đưa các file CSV vào thư mục phân vùng
    success = run_hdfs_cmd(["bash", "-c", f"hdfs dfs -put -f {container_csv_src}/*.csv {hdfs_dest}/"])
    if success:
        print(f"   ✓ Đã nạp thành công vào {hdfs_dest}")
        # Kiểm tra danh sách file
        run_hdfs_cmd(["hdfs", "dfs", "-ls", "-h", hdfs_dest])
    return success

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Đưa dữ liệu CSV lên HDFS")
    parser.add_argument("--months", type=str, default="1", 
                        help="Danh sách tháng cần đưa lên HDFS, ví dụ: '1' hoặc '1,2,3' hoặc '1-12'")
    parser.add_argument("--year", type=int, default=2024, help="Năm dữ liệu")
    args = parser.parse_args()
    
    months_to_run = []
    if "-" in args.months:
        start_m, end_m = map(int, args.months.split("-"))
        months_to_run = list(range(start_m, end_m + 1))
    else:
        months_to_run = [int(m.strip()) for m in args.months.split(",")]
        
    for m in months_to_run:
        load_month_to_hdfs(args.year, m)
        
    print("\n=== Tổng kết dung lượng tầng /citibike/raw trên HDFS ===")
    run_hdfs_cmd(["hdfs", "dfs", "-du", "-h", "/citibike/raw"])

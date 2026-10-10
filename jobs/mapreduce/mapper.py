#!/usr/bin/env python3
"""
Mapper cho Hadoop Streaming MapReduce:
Đọc các dòng CSV từ sys.stdin, trích xuất start_station_id và phát ra cặp (station_id, 1)
"""
import sys
import csv

def main():
    reader = csv.reader(sys.stdin)
    for row in reader:
        # Bỏ qua dòng tiêu đề hoặc dòng không đủ số cột
        if not row or len(row) < 6:
            continue
        if row[0] == 'ride_id':
            continue
        
        # Cột start_station_id nằm ở vị trí index 5
        station_id = row[5].strip()
        if station_id:
            print(f"{station_id}\t1")

if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""
Reducer cho Hadoop Streaming MapReduce:
Đọc các cặp key-value đã được shuffle & sort theo station_id từ sys.stdin và cộng dồn số lượt thuê
"""
import sys

def main():
    current_station = None
    current_count = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            station_id, count_str = line.split('\t', 1)
            count = int(count_str)
        except ValueError:
            continue

        if current_station == station_id:
            current_count += count
        else:
            if current_station is not None:
                print(f"{current_station}\t{current_count}")
            current_station = station_id
            current_count = count

    if current_station is not None:
        print(f"{current_station}\t{current_count}")

if __name__ == '__main__':
    main()

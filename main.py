'''
Main module for dwell estimation application.
This module initializes the application, sets up logging, and handles exceptions.
'''
import argparse
import re
import sys
import subprocess
import time

from src.logging.logging import logging
from src.exception.exception import CustomException

from src.pipeline.ingest_parser import (
    run_netsh, parse_netsh, ensure_csv_header, append_rows
)

# Define configuration variables
CSV_OUTPUT = "ingested_wifi_scans.csv"

# Define main function
def main():
    parser = argparse.ArgumentParser(description="Dwell Estimation Application")
    parser.add_argument("--interval-sec", type=int, default=60, help="Interval in seconds for data ingestion")
    parser.add_argument("--duration-min", type=int, default=60, help="Duration in minutes for data ingestion")
    
    # Define arguments
    args = parser.parse_args()
    
    # Define ingestion parameters
    interval = args.interval_sec
    duration = args.duration_min
    end_time = time.time() + (duration * 60)
    
    # Define fieldnames
    field_names = [
        "ts_utc", "ssid", "bssid_hash",
        "auth", "encryption",
        "signal_percent", "rssi_dbm_est",
        "radio_type", "band", "channel",
        "connected_stations",
        "channel_utilization_pct",
        "medium_available_capacity"
    ]
    
    ensure_csv_header(CSV_OUTPUT, field_names=field_names)
    
    # Print start message
    print(f"Starting data ingestion for {duration} minutes, interval {interval} seconds to {CSV_OUTPUT}")
    
    # Iterate until end time
    while time.time() < end_time:
        output = run_netsh()
        rows = parse_netsh(output=output)
        append_rows(CSV_OUTPUT, rows, field_names)
        
        # Check if rows were added
        if rows:
            r0 = rows[0]
            print(f"{r0['ts_utc']} | {r0['ssid']} | "f"Sig={r0['signal_percent']}% | "f"Stations={r0['connected_stations']} | "f"Util={r0['channel_utilization_pct']}%")
        
        time.sleep(interval)
    
    print("Data ingestion completed.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        raise CustomException(e, sys)
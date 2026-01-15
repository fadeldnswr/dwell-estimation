'''
Docstring for src.pipeline.ingest_parser
'''
import subprocess
import re
import csv
import time
import sys
import hashlib
import argparse

from datetime import datetime, timezone
from src.exception.exception import CustomException
from src.logging.logging import logging

# Define configuration variables
CMD_COMMAND = ["netsh", "wlan", "show", "networks", "mode=bssid"]
HASH_SALT = "fjierbvbg03ur90485h0943uf8j4"

# Function to convert signal percentage to dBm
def percentage_to_dbm(percentage: int) -> float:
  return (percentage / 2) - 100.0

# Function to hash BSSID with salt
def hash_with_salt(raw: str) -> str:
  hashed = hashlib.sha256((raw + HASH_SALT).encode("utf-8")).hexdigest()
  return hashed[:16]

# Function to run netsh command
def run_netsh() -> str:
  process = subprocess.run(
    CMD_COMMAND, capture_output=True, text=True,
    encoding="utf-8", errors="ignore"
  )
  return process.stdout

# Function to parse netsh output
def parse_netsh(output: str) -> list:
  try:
    # Rows to hold parsed data
    rows: list = []
    
    # Parse SSID
    ssid_blocks = re.split(r"\n\s*SSID\s+\d+\s*:\s*", output)
    for block in ssid_blocks[1:]:
      lines = block.splitlines()
      if not lines:
        continue
      
      # Extract SSID
      ssid = lines[0].strip()
      auth= None
      enc = None
      
      for line in lines[1:]:
        # Match Authentication and Encryption
        matched = re.match(r"\s*Authentication\s*:\s*(.+)", line)
        if matched:
          auth = matched.group(1).strip()
          
        matched = re.match(r"\s*Encryption\s*:\s*(.+)", line)
        if matched:
          enc = matched.group(1).strip()
      
      # Split BSSID
      bssid_splits = re.split(r"\n\s*BSSID\s+\d+\s*:\s*", "\n" + block)
      for bss in bssid_splits[1:]:
        bssid_lines = bss.splitlines()
        if not bssid_lines:
          continue
        
        # Define default values
        bssid = bssid_lines[0].strip()
        
        # Initialize signal and channel
        signal_pct = None
        channel = None
        radio_type = None
        band = None
        connected_station = None
        channel_util_pct = None
        medium_avail_pct = None
        in_bss_load = None
        
        # Extract BSSID details
        for line in bssid_lines[1:]:
          line = line.rstrip()
          
          # Matched signal percentage
          matched = re.match(r"\s*Signal\s*:\s*(\d+)%", line)
          if matched:
            signal_pct = int(matched.group(1)) if matched else None
          
          # Matched channel
          matched = re.match(r"\s*Channel\s*:\s*(\d+)", line)
          if matched:
            channel = int(matched.group(1)) if matched else None
          
          # Matched radio type
          matched = re.match(r"\s*Radio\s+type\s*:\s*(.+)", line)
          if matched:
            radio_type = matched.group(1).strip() if matched else None
          
          # Matched band
          matched = re.match(r"\s*Band\s*:\s*(.+)", line)
          if matched:
            band = matched.group(1).strip() if matched else None
          
          # Matched Bss Load
          if re.match(r"\s*Bss\s+Load\s*:\s*", line):
            in_bss_load = True
            continue
          
          # Inside Bss Load block
          if in_bss_load:
            # Matched connected stations
            matched = re.match(r"\s*Connected\s+Stations\s*:\s*(\d+)", line)
            if matched:
              connected_station = int(matched.group(1)) if matched else None
            
            # Matched Channel Utilization
            matched = re.match(r"\s*Channel\s+Utilization\s*:\s*\d+\s*\((\d+)\s*%\)", line)
            if matched:
              channel_util_pct = int(matched.group(1)) if matched else None
            
            # Matched Medium Available Capacity
            matched = re.match(r"\s*Medium\s+Available\s+Capacity\s*:\s*(\d+)", line)
            if matched:
              medium_avail_pct = int(matched.group(1)) if matched else None
        
        # Define timestamp
        ts = datetime.now(timezone.utc).isoformat()
        
        # Append row data
        rows.append({
          "ts_utc": ts,
          "ssid": ssid,
          "bssid_hash": hash_with_salt(bssid),
          "auth": auth,
          "encryption": enc,
          "signal_percent": signal_pct,
          "rssi_dbm_est": percentage_to_dbm(signal_pct) if signal_pct is not None else None,
          "radio_type": radio_type,
          "band": band,
          "channel": channel,
          "connected_stations": connected_station,
          "channel_utilization_pct": channel_util_pct,
          "medium_available_capacity": medium_avail_pct
        })
      return rows
  except Exception as e:
    raise CustomException(e, sys)

# Function to ensure CSV header
def ensure_csv_header(path, field_names) -> None:
  try:
    with open(path, "r", encoding="utf-8"):
      pass
  except FileNotFoundError:
    with open(path, "w", newline="", encoding="utf-8") as file:
      writer = csv.DictWriter(file, fieldnames=field_names)
      writer.writeheader()

# Function to append rows to CSV
def append_rows(path, rows, field_names) -> None:
  try:
    with open(path, "a", newline="", encoding="utf-8") as file:
      writer = csv.DictWriter(file, fieldnames=field_names)
      for row in rows:
        writer.writerow(row)
  except Exception as e:
    raise CustomException(e, sys)
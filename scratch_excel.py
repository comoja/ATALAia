import pandas as pd
import sys

def analyze_excel(file_path):
    print(f"\n--- Analyzing {file_path} ---")
    try:
        # Load the Excel file to get sheet names
        xls = pd.ExcelFile(file_path, engine='openpyxl')
        print(f"Sheet names: {xls.sheet_names}")
        
        for sheet in xls.sheet_names:
            print(f"\nSheet: {sheet}")
            # Read just 5 rows to see structure
            df = pd.read_excel(xls, sheet_name=sheet, nrows=5)
            print("Columns:", list(df.columns))
            print("First few rows:")
            print(df.head(2))
    except Exception as e:
        print(f"Error reading {file_path}: {e}")

analyze_excel("/Volumes/TOSHIBA5TB/Backup/desarrollo/ATALAia/docs/ATALA IA MODEL (1).xlsm")
analyze_excel("/Volumes/TOSHIBA5TB/Backup/desarrollo/ATALAia/docs/HIST PRICES ATALAIA (1).xlsm")

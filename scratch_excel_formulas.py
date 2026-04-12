import openpyxl
import pandas as pd

def extract_formulas(file_path, sheet_name):
    print(f"Extracting formulas from {sheet_name} in {file_path}")
    # data_only=False reads the formulas instead of the values
    wb = openpyxl.load_workbook(file_path, data_only=False)
    sheet = wb[sheet_name]
    
    # We want to find the headers and then the formulas in the second row (or first row with data)
    headers = []
    for cell in sheet[1]: # Assuming headers are row 1
        headers.append(cell.value)
    
    if not any(headers):
        # Maybe row 2 or 3 are headers
        headers = []
        for cell in sheet[2]:
            headers.append(cell.value)

    print(f"Headers found: {headers[:20]}...") # Print first 20 to check
    
    # Let's get the values/formulas of row 5 as a sample (to avoid headers)
    print("\nSample formulas (Row 5):")
    for col_idx in range(1, sheet.max_column + 1):
        header = sheet.cell(row=2, column=col_idx).value
        header = header if header is not None else f"Col{col_idx}"
        cell_formula = sheet.cell(row=5, column=col_idx).value
        # Only print if it's an interesting column based on names we saw earlier
        if any(keyword in str(header).upper() for keyword in ['PNL', 'STRIKE', 'SEVERIDADES', 'VEL', 'NOMBRE', 'DC', 'DP']):
             print(f"{header}: {cell_formula}")
        # or if it starts with '='
        elif str(cell_formula).startswith('='):
             print(f"{header} (Formula): {cell_formula}")

extract_formulas("/Volumes/TOSHIBA5TB/Backup/desarrollo/ATALAia/docs/ATALA IA MODEL (1).xlsm", "EURGBPUSD")

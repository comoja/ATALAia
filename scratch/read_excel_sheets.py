import openpyxl
import pandas as pd

def inspect_model():
    print("=== INSPECTING ATALA IA MODEL (1).xlsm ===")
    wb = openpyxl.load_workbook('docs/ATALA IA MODEL (1).xlsm', data_only=False)
    sheet = wb['EURGBPUSD']
    
    # Imprimir las primeras 10 filas de las primeras 15 columnas con sus fórmulas/valores
    for r in range(1, 20):
        row_vals = []
        for c in range(1, 15):
            cell = sheet.cell(row=r, column=c)
            row_vals.append(f"{openpyxl.utils.get_column_letter(c)}{r}: {cell.value}")
        print(f"Row {r}:", row_vals)

def inspect_catalogo():
    print("\n=== INSPECTING CATALOGO ===")
    wb = openpyxl.load_workbook('docs/ATALA IA MODEL (1).xlsm', data_only=True)
    sheet = wb['CATALOGO']
    for r in range(1, 15):
        row_vals = [sheet.cell(row=r, column=c).value for c in range(1, 10)]
        print(f"Row {r}:", row_vals)

if __name__ == '__main__':
    inspect_model()
    inspect_catalogo()

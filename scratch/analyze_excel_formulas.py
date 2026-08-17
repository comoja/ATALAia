import openpyxl

def inspect_formulas():
    wb = openpyxl.load_workbook('docs/ATALA IA MODEL (1).xlsm', read_only=True)
    sheet = wb['EURGBPUSD']
    
    # Recorrer filas buscando dónde hay títulos de columnas
    print("=== BUSCANDO FILAS DE TÍTULOS Y CABECERAS ===")
    for r in range(1, 40):
        row_vals = [sheet.cell(row=r, column=c).value for c in range(1, 45)]
        non_empty = [(c+1, v) for c, v in enumerate(row_vals) if v is not None]
        if non_empty:
            # Imprimir si parece fila de cabecera o parámetros
            print(f"Fila {r}: {non_empty[:12]}...")

if __name__ == '__main__':
    inspect_formulas()

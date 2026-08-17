import pandas as pd
import numpy as np

def print_clean_grid_more():
    df = pd.read_excel('docs/ATALA IA MODEL (1).xlsm', sheet_name='EURGBPUSD', nrows=25)
    
    # Seleccionar columnas de la 30 a la 60
    df_subset = df.iloc[:, 30:60].fillna('')
    
    def col_letter(idx):
        if idx < 26:
            return chr(65 + idx)
        else:
            first = chr(65 + (idx // 26) - 1)
            second = chr(65 + (idx % 26))
            return first + second
            
    col_mappings = {df_subset.columns[i]: f"{col_letter(i+30)} ({df_subset.columns[i]})" for i in range(len(df_subset.columns))}
    df_subset = df_subset.rename(columns=col_mappings)
    
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    
    cols = df_subset.columns.tolist()
    step = 6
    for i in range(0, len(cols), step):
        segment_cols = cols[i:i+step]
        print(f"\n--- Columnas {segment_cols[0].split()[0]} a {segment_cols[-1].split()[0]} ---")
        print(df_subset[segment_cols].to_string(index=True))

if __name__ == '__main__':
    print_clean_grid_more()

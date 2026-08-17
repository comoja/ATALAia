import os
import glob
import re

def prepare_optimizers():
    directory = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting"
    files = glob.glob(os.path.join(directory, "run_*_optimization.py"))
    
    start_date = "'2026-04-16 00:00:00'"
    end_date = "'2026-06-16 23:59:59'"
    
    date_start_pattern = re.compile(r"startDateStr\s*=\s*['\"].*?['\"]")
    date_end_pattern = re.compile(r"endDateStr\s*=\s*['\"].*?['\"]")
    
    # Exigir PF >= 1.0 (en lugar de 1.25) y relajar Win Rate a 35% en los filtros finales de viabilidad
    pf_threshold_pattern = re.compile(r"profitFactor['\"]\s*\]\s*>=\s*1\.25")
    pf_threshold_repl = "profitFactor'] >= 1.00"
    
    wr_threshold_pattern = re.compile(r"winRate['\"]\s*\]\s*>=\s*42\.0")
    wr_threshold_repl = "winRate'] >= 35.0"
    
    count = 0
    for file_path in files:
        if "genericfvg" in file_path or "sniper" in file_path or "breakout" in file_path or "grid_search" in file_path:
            # Estos ya fueron procesados o son scripts base
            continue
            
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # Actualizar fechas
        content = date_start_pattern.sub(f"startDateStr = {start_date}", content)
        content = date_end_pattern.sub(f"endDateStr = {end_date}", content)
        
        # Actualizar thresholds de viabilidad
        content = pf_threshold_pattern.sub(pf_threshold_repl, content)
        content = wr_threshold_pattern.sub(wr_threshold_repl, content)
        
        # Algunas veces el criterio está en un comentario
        content = content.replace("PF >= 1.25 y WR >= 42%", "PF >= 1.0 y WR >= 35%")
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
            
        count += 1
        print(f"Preparado: {os.path.basename(file_path)}")
        
    print(f"Total optimizadores actualizados: {count}")

if __name__ == '__main__':
    prepare_optimizers()

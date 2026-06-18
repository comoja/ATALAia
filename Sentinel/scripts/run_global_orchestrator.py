import os
import subprocess
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

STRATEGIES = [
    ('run_breakoutny_optimization.py', 'BreakoutNY'),
    ('run_breakoutprobability_optimization.py', 'BreakoutProbability'),
    ('run_genericfvg_optimization.py', 'GenericFVG'),
    ('run_sniper_optimization.py', 'Sniper'),
    ('run_silverbullet_optimization.py', 'SilverBullet'),
    ('run_cruceema_optimization.py', 'CruceEMA'),
    ('run_fvgdiario_optimization.py', 'FvgDiario'),
    ('run_ichimoku_optimization.py', 'Ichimoku'),
    ('run_imbalance_optimization.py', 'Imbalance'),
    ('run_patron4h_optimization.py', 'Patron4h'),
    ('run_qtrend_optimization.py', 'QTrend'),
    ('run_reversionmedia_optimization.py', 'ReversionMedia'),
    ('run_sesgobiashtf_optimization.py', 'SesgoBiasHTF'),
    ('run_speedbot_optimization.py', 'SpeedBot'),
    ('run_grid_search_optimization.py', 'PremiumConfluence')
]

def run_orchestrator():
    logger.info("==========================================================")
    logger.info("🚀 INICIANDO GLOBAL ORCHESTRATOR - DEEP GRID SEARCH")
    logger.info(f"Estrategias a procesar: {len(STRATEGIES)}")
    logger.info("==========================================================")
    
    base_dir = "/Volumes/TimeMachine/ATALAia/Sentinel"
    backtest_dir = os.path.join(base_dir, "backtesting")
    scripts_dir = os.path.join(base_dir, "scripts")
    
    total_start = time.time()
    
    for script_name, strat_name in STRATEGIES:
        script_path = os.path.join(backtest_dir, script_name)
        if not os.path.exists(script_path):
            logger.warning(f"⚠️ Script {script_name} no encontrado. Saltando.")
            continue
            
        logger.info(f"\n▶️ Iniciando optimización para: {strat_name}")
        start_time = time.time()
        
        # 1. Ejecutar Optimizador
        try:
            logger.info(f"Corriendo {script_name}...")
            # Popen en vez de run para redirigir stdout al log si lo ejecutamos asíncrono
            result = subprocess.run(["python3", script_path], cwd=backtest_dir, capture_output=True, text=True)
            if result.returncode != 0:
                logger.error(f"❌ Error en {strat_name}:\n{result.stderr}")
                continue
                
            elapsed = (time.time() - start_time) / 60
            logger.info(f"✅ Optimización completada en {elapsed:.1f} minutos.")
        except Exception as e:
            logger.error(f"❌ Excepción corriendo {strat_name}: {e}")
            continue
            
        # 2. Inyectar Resultados usando Master Injector
        injections = []
        if strat_name == 'Imbalance':
            injections = [
                ('imbalanceldn_grid_results_best.csv', 'ImbalanceLDN'),
                ('imbalanceny_grid_results_best.csv', 'ImbalanceNY'),
                ('imbalancepmny_grid_results_best.csv', 'ImbalancePMNY')
            ]
        else:
            csv_name = f"{strat_name.lower()}_grid_results_best.csv"
            injections = [(csv_name, strat_name)]
            
        for csv_file, actual_strat in injections:
            csv_path = os.path.join(backtest_dir, csv_file)
            
            if not os.path.exists(csv_path):
                logger.warning(f"⚠️ No se encontró {csv_file}. Buscando alternativa...")
                import glob
                possible = glob.glob(os.path.join(backtest_dir, f"*{actual_strat.lower().replace('_','')}*_best.csv"))
                if possible:
                    csv_path = max(possible, key=os.path.getmtime)
                    logger.info(f"Usando CSV alternativo: {csv_path}")
                else:
                    logger.error(f"❌ No se encontró CSV de resultados para {actual_strat}. No se puede inyectar.")
                    continue
                    
            logger.info(f"Inyectando resultados de {actual_strat} en BD...")
            injector_script = os.path.join(scripts_dir, "update_master_all_symbols.py")
            
            try:
                inj_res = subprocess.run(
                    ["python3", injector_script, "--csv", csv_path, "--strategy", actual_strat],
                    cwd=base_dir, capture_output=True, text=True
                )
                if inj_res.returncode == 0:
                    logger.info(f"💉 Inyección exitosa para {actual_strat}:\n{inj_res.stdout.strip()}")
                else:
                    logger.error(f"❌ Error inyectando {actual_strat}:\n{inj_res.stderr}")
            except Exception as e:
                logger.error(f"❌ Excepción inyectando {actual_strat}: {e}")
            
    total_elapsed = (time.time() - total_start) / 3600
    logger.info("==========================================================")
    logger.info(f"🏁 GLOBAL ORCHESTRATOR FINALIZADO ({total_elapsed:.2f} horas)")
    logger.info("==========================================================")

if __name__ == '__main__':
    run_orchestrator()

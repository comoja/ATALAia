import sys
import os
import json

sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection

def migrar_datos():
    conn = dbConnection.getConnection()
    if not conn:
        print("❌ No se pudo conectar a la base de datos.")
        return

    cursor = conn.cursor(dictionary=True)
    try:
        # Asegurar que la columna jsonIMACD exista
        print("Asegurando que la columna jsonIMACD existe...")
        try:
            cursor.execute("ALTER TABLE symbolStrategyConfig ADD COLUMN jsonIMACD JSON DEFAULT NULL")
            conn.commit()
            print("✅ Columna jsonIMACD creada con éxito.")
        except Exception as alter_err:
            # Si ya existe, no pasa nada
            print("ℹ️ La columna jsonIMACD ya existía o no se pudo crear (probablemente ya existe).")
            conn.rollback()

        # Obtener las columnas existentes de la tabla para ver si existen las columnas de MACD individuales
        cursor.execute("DESCRIBE symbolStrategyConfig")
        columns = [row['Field'] for row in cursor.fetchall()]
        has_macd_cols = all(col in columns for col in ['useImpulseMacdFilter', 'macdFast', 'macdSlow', 'macdSignal'])

        if not has_macd_cols:
            print("⚠️ Las columnas individuales de MACD no están completas en el esquema. Se migrarán los valores por defecto o los existentes.")

        # Obtener todos los registros
        query_cols = "strategy, symbol, parametersJson"
        if 'useImpulseMacdFilter' in columns:
            query_cols += ", useImpulseMacdFilter"
        if 'macdFast' in columns:
            query_cols += ", macdFast"
        if 'macdSlow' in columns:
            query_cols += ", macdSlow"
        if 'macdSignal' in columns:
            query_cols += ", macdSignal"

        cursor.execute(f"SELECT {query_cols} FROM symbolStrategyConfig")
        records = cursor.fetchall()
        print(f"Encontrados {len(records)} registros para procesar.")

        migrated_count = 0
        for row in records:
            strategy = row['strategy']
            symbol = row['symbol']
            
            # Obtener datos de IMACD
            use_filter = row.get('useImpulseMacdFilter', 0)
            macd_fast = row.get('macdFast', 12)
            macd_slow = row.get('macdSlow', 26)
            macd_signal = row.get('macdSignal', 9)

            # Si alguno es None, usar valores por defecto
            if use_filter is None: use_filter = 0
            if macd_fast is None: macd_fast = 12
            if macd_slow is None: macd_slow = 26
            if macd_signal is None: macd_signal = 9

            imacd_obj = {
                "useImpulseMacdFilter": int(use_filter),
                "macdFast": int(macd_fast),
                "macdSlow": int(macd_slow),
                "macdSignal": int(macd_signal)
            }

            # Procesar parametersJson para remover minConfidence si existe
            params_json_str = row['parametersJson']
            params = {}
            if params_json_str:
                params = json.loads(params_json_str) if isinstance(params_json_str, str) else params_json_str

            modified_params = False
            if 'minConfidence' in params:
                print(f"[{symbol} - {strategy}] Eliminando minConfidence redundante de parametersJson: {params['minConfidence']}")
                del params['minConfidence']
                modified_params = True
            
            # Actualizar base de datos
            if modified_params:
                update_query = """
                    UPDATE symbolStrategyConfig 
                    SET jsonIMACD = %s, parametersJson = %s 
                    WHERE strategy = %s AND symbol = %s
                """
                cursor.execute(update_query, (json.dumps(imacd_obj), json.dumps(params), strategy, symbol))
            else:
                update_query = """
                    UPDATE symbolStrategyConfig 
                    SET jsonIMACD = %s 
                    WHERE strategy = %s AND symbol = %s
                """
                cursor.execute(update_query, (json.dumps(imacd_obj), strategy, symbol))
            
            migrated_count += 1

        conn.commit()
        print(f"🎉 Migración exitosa. Procesados y actualizados {migrated_count} registros.")

    except Exception as e:
        print(f"❌ Error durante la migración: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    migrar_datos()

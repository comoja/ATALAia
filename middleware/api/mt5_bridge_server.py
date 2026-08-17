"""
Servidor puente (Bridge) para MetaTrader 5 ejecutado bajo Wine.
Expone la API de MT5 a través de HTTP JSON en localhost:8005 para que
el Python nativo de Linux pueda consultar tasas, órdenes y cuenta.
"""
import sys
import json
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime
import pandas as pd
import pytz

try:
    import MetaTrader5 as mt5
except ImportError:
    mt5 = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("MT5Bridge")

TIMEFRAME_MAP = {}
if mt5:
    TIMEFRAME_MAP = {
        "1min": mt5.TIMEFRAME_M1,
        "5min": mt5.TIMEFRAME_M5,
        "15min": mt5.TIMEFRAME_M15,
        "30min": mt5.TIMEFRAME_M30,
        "1h": mt5.TIMEFRAME_H1,
        "4h": mt5.TIMEFRAME_H4,
        "1day": mt5.TIMEFRAME_D1,
        "1d": mt5.TIMEFRAME_D1,
        "1week": mt5.TIMEFRAME_W1,
        "1month": mt5.TIMEFRAME_MN1,
    }

def init_mt5():
    if not mt5:
        return False
    if mt5.terminal_info() is not None:
        return True
    return mt5.initialize()

class MT5Handler(BaseHTTPRequestHandler):
    def _send_json(self, data, status=200):
        body = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health" or self.path == "/":
            is_init = init_mt5()
            term = mt5.terminal_info()._asdict() if is_init and mt5.terminal_info() else None
            self._send_json({"status": "online" if is_init else "offline", "terminal": term})
        elif self.path == "/account":
            if not init_mt5():
                self._send_json({"error": "MT5 not initialized"}, 500)
                return
            acc = mt5.account_info()._asdict() if mt5.account_info() else None
            self._send_json({"account": acc})
        else:
            self._send_json({"error": "Not found"}, 404)

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        post_data = self.rfile.read(content_length).decode("utf-8") if content_length > 0 else "{}"
        try:
            params = json.loads(post_data)
        except Exception:
            params = {}

        if self.path == "/rates":
            if not init_mt5():
                self._send_json({"error": "MT5 not initialized", "last_error": mt5.last_error() if mt5 else None}, 500)
                return

            symbol = params.get("symbol")
            interval = params.get("interval", "5min")
            output_size = int(params.get("output_size", 100))
            ts_from = params.get("ts_from")
            ts_to = params.get("ts_to")

            if not symbol:
                self._send_json({"error": "Symbol is required"}, 400)
                return

            mt5_tf = TIMEFRAME_MAP.get(interval, mt5.TIMEFRAME_M5)
            mt5.symbol_select(symbol, True)

            rates = None
            if ts_from and ts_to:
                rates = mt5.copy_rates_range(symbol, mt5_tf, int(ts_from), int(ts_to))
            elif ts_from:
                rates = mt5.copy_rates_from(symbol, mt5_tf, int(ts_from), output_size)
            else:
                rates = mt5.copy_rates_from_pos(symbol, mt5_tf, 0, output_size)

            if rates is None or len(rates) == 0:
                self._send_json({"rates": []})
                return

            # Convert to list of dicts
            df = pd.DataFrame(rates)
            records = df.to_dict(orient="records")
            self._send_json({"rates": records, "count": len(records)})
        else:
            self._send_json({"error": "Unknown endpoint"}, 404)

    def log_message(self, format, *args):
        # Silenciar logs excesivos
        pass

def run_server(port=8005):
    server_address = ("127.0.0.1", port)
    httpd = HTTPServer(server_address, MT5Handler)
    logger.info(f"🚀 MT5 Bridge Server iniciado en http://127.0.0.1:{port}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Deteniendo MT5 Bridge Server...")
        httpd.server_close()

if __name__ == "__main__":
    if init_mt5():
        logger.info("✅ Conexión con MetaTrader 5 establecida en Wine.")
    else:
        logger.warning("⚠️ No se pudo inicializar MT5 al arrancar el servidor.")
    run_server()

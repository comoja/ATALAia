"""
Servicio de Monitoreo de Ecosistema ATALAia y Consola Terminal
Inspecciona servicios systemd, puertos de red, procesos activos y bitácoras en caliente.
"""

import os
import subprocess
import socket
import re
from datetime import datetime
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger("SystemMonitor")

class SystemMonitorService:
    SERVICES_DEF = [
        {
            "id": "mysql",
            "name": "Base de Datos MySQL",
            "unit": "mysql",
            "is_system": True,
            "port": 3306,
            "category": "Infraestructura",
            "icon": "pi pi-database",
            "description": "Servidor relacional para usuarios, cuentas, configuraciones y velas históricas."
        },
        {
            "id": "connectionpool",
            "name": "ConnectionPool Microservice",
            "unit": "atalaia-1-connectionpool",
            "is_system": False,
            "port": 8000,
            "category": "Microservicios",
            "icon": "pi pi-server",
            "description": "Gestor centralizado de conexiones SQLAlchemy y almacenamiento masivo de velas."
        },
        {
            "id": "datasymbol",
            "name": "dataSymbol Orchestrator",
            "unit": "atalaia-2-datasymbol",
            "is_system": False,
            "port": None,
            "category": "Ingesta de Datos",
            "icon": "pi pi-sync",
            "description": "Orquestador continuo de descarga y sincronización de velas desde MetaTrader 5."
        },
        {
            "id": "webhook",
            "name": "WebHook Server",
            "unit": "atalaia-3-webhook",
            "is_system": False,
            "port": 8002,
            "category": "Señales & Alertas",
            "icon": "pi pi-bolt",
            "description": "Servidor FastAPI receptor de webhooks externos y alertas TradingView."
        },
        {
            "id": "sentinel",
            "name": "Sentinel Trading Bots",
            "unit": "atalaia-4-sentinel",
            "is_system": False,
            "port": None,
            "category": "Trading Cuantitativo",
            "icon": "pi pi-shield",
            "description": "Motor de ejecución multiextrategia institucional en tiempo real (Ichimoku, QTrend, ICT)."
        },
        {
            "id": "backend",
            "name": "ATALAia Backend Engine",
            "unit": "atalaia-5-atalaia",
            "is_system": False,
            "port": 8004,
            "category": "Core / Motor API",
            "icon": "pi pi-code",
            "description": "Motor de cálculo cuantitativo, análisis de correlación y APIs REST institucionales."
        },
        {
            "id": "mt5bridge",
            "name": "MT5 Wine Bridge",
            "unit": "atalaia-5-atalaia",
            "is_system": False,
            "port": 8005,
            "category": "Conectividad Broker",
            "icon": "pi pi-chart-line",
            "description": "Puente HTTP JSON con la terminal MetaTrader 5 ejecutada bajo Wine."
        },
        {
            "id": "frontend",
            "name": "Tomcat Webserver (Frontend)",
            "unit": "atalaia-5-atalaia",
            "is_system": False,
            "port": 8080,
            "category": "Interfaz Gráfica",
            "icon": "pi pi-desktop",
            "description": "Servidor de aplicaciones web para la consola JSF PrimeFaces y Aetherial UI."
        },
        {
            "id": "microratio",
            "name": "microRatio Daemon",
            "unit": "microRatio",
            "is_system": False,
            "port": None,
            "category": "Trading Cuantitativo",
            "icon": "pi pi-percentage",
            "description": "Daemon de seguimiento horario de ratios, gestión de PnL y liquidación por convergencia."
        },
        {
            "id": "ngrok",
            "name": "Ngrok Tunnel (WebHook)",
            "unit": "ngrok",
            "is_system": False,
            "port": 8002,
            "is_tunnel": True,
            "category": "Conectividad Externa",
            "icon": "pi pi-globe",
            "description": "Túnel seguro para recepción de webhooks de TradingView hacia el puerto 8002."
        },
        {
            "id": "cloudflared",
            "name": "Cloudflare Tunnel (Frontend)",
            "unit": None,
            "is_process": True,
            "process_match": "cloudflared tunnel",
            "port": 8080,
            "is_tunnel": True,
            "category": "Conectividad Externa",
            "icon": "pi pi-cloud",
            "description": "Acceso web seguro institucional de alta disponibilidad al Frontend (puerto 8080)."
        }
    ]

    @classmethod
    def check_port(cls, port: Optional[int], host: str = "127.0.0.1", timeout: float = 0.3) -> bool:
        if not port or port <= 0:
            return False
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(timeout)
                return s.connect_ex((host, port)) == 0
        except Exception:
            return False

    @classmethod
    def check_systemd_unit(cls, unit: str, is_system: bool = False) -> Dict[str, Any]:
        cmd_act = ["systemctl", "is-active", unit] if is_system else ["systemctl", "--user", "is-active", unit]
        cmd_ena = ["systemctl", "is-enabled", unit] if is_system else ["systemctl", "--user", "is-enabled", unit]
        
        status_text = "inactive"
        enabled = False
        try:
            r1 = subprocess.run(cmd_act, capture_output=True, text=True, timeout=1.0)
            status_text = r1.stdout.strip()
        except Exception:
            status_text = "unknown"

        try:
            r2 = subprocess.run(cmd_ena, capture_output=True, text=True, timeout=1.0)
            enabled = (r2.stdout.strip() == "enabled")
        except Exception:
            enabled = False

        return {
            "active": (status_text == "active"),
            "status_text": status_text,
            "enabled": enabled
        }

    @classmethod
    def check_process(cls, match: str) -> Dict[str, Any]:
        try:
            r = subprocess.run(["pgrep", "-f", match], capture_output=True, text=True, timeout=1.0)
            pids = [p.strip() for p in r.stdout.splitlines() if p.strip()]
            return {
                "active": len(pids) > 0,
                "pids": pids,
                "status_text": "active" if pids else "inactive"
            }
        except Exception:
            return {"active": False, "pids": [], "status_text": "unknown"}

    @classmethod
    def get_services_status(cls) -> Dict[str, Any]:
        results = []
        active_count = 0
        listening_count = 0

        for svc in cls.SERVICES_DEF:
            svc_id = svc["id"]
            name = svc["name"]
            category = svc["category"]
            desc = svc["description"]
            icon = svc["icon"]
            port = svc.get("port")
            
            is_listening = cls.check_port(port) if port else False
            if is_listening:
                listening_count += 1

            is_active = False
            auto_start = "N/A"
            status_label = "INACTIVO"
            status_css = "status-inactive"
            pid_info = "-"

            if svc.get("is_process"):
                proc_info = cls.check_process(svc["process_match"])
                is_active = proc_info["active"]
                if proc_info["pids"]:
                    pid_info = ",".join(proc_info["pids"])
                auto_start = "Manual / Proceso"
            elif svc.get("unit"):
                unit_info = cls.check_systemd_unit(svc["unit"], svc.get("is_system", False))
                is_active = unit_info["active"]
                auto_start = "Habilitado" if unit_info["enabled"] else "Deshabilitado"
                
                # Para subcomponentes de la suite (FastAPI, MT5, Frontend)
                if svc_id in ["backend", "mt5bridge", "frontend"]:
                    is_active = is_listening or unit_info["active"]

            # Si tiene puerto asociado y está escuchando, se considera activo
            if port and is_listening and not is_active:
                is_active = True

            if is_active:
                active_count += 1
                status_label = "ACTIVO"
                status_css = "status-active"
            else:
                status_label = "INACTIVO"
                status_css = "status-inactive"

            results.append({
                "id": svc_id,
                "name": name,
                "category": category,
                "description": desc,
                "icon": icon,
                "port": str(port) if port else "N/A",
                "unit": svc.get("unit") or svc.get("process_match", "N/A"),
                "status": status_label,
                "active": is_active,
                "listening": is_listening,
                "autoStart": auto_start,
                "pid": pid_info,
                "statusCss": status_css
            })

        total = len(results)
        inactive_count = total - active_count

        return {
            "services": results,
            "summary": {
                "total": total,
                "active": active_count,
                "inactive": inactive_count,
                "listening": listening_count,
                "healthy": (inactive_count == 0),
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        }

    @classmethod
    def get_terminal_logs(cls, service: str = "all", max_lines: int = 50) -> List[Dict[str, str]]:
        """Obtiene las líneas de log más recientes para la consola interactiva."""
        raw_entries = []
        now_str = datetime.now().strftime("%H:%M:%S")

        def add_from_cmd(tag: str, cmd: List[str], max_take: int = 25):
            try:
                res = subprocess.run(cmd, capture_output=True, text=True, timeout=1.5)
                for line in res.stdout.splitlines():
                    clean = line.strip()
                    if clean:
                        # Extraer hora si viene en formato estándar
                        time_match = re.search(r'(\d{2}:\d{2}:\d{2})', clean)
                        entry_time = time_match.group(1) if time_match else now_str
                        # Limpiar nombre del host si viene de journalctl
                        display_text = clean
                        if "ATALAia" in display_text:
                            parts = display_text.split("]: ", 1)
                            if len(parts) > 1:
                                display_text = parts[1]
                        raw_entries.append({
                            "time": entry_time,
                            "tag": tag,
                            "message": display_text
                        })
            except Exception as e:
                logger.debug(f"Error leyendo logs para {tag}: {e}")

        def add_from_file(tag: str, filepath: str, max_take: int = 25):
            if os.path.exists(filepath):
                try:
                    res = subprocess.run(["tail", "-n", str(max_take), filepath], capture_output=True, text=True, timeout=1.0)
                    for line in res.stdout.splitlines():
                        clean = line.strip()
                        if clean:
                            time_match = re.search(r'(\d{2}:\d{2}:\d{2})', clean)
                            entry_time = time_match.group(1) if time_match else now_str
                            raw_entries.append({
                                "time": entry_time,
                                "tag": tag,
                                "message": clean
                            })
                except Exception:
                    pass

        svc_lower = service.lower().strip()
        base_dir = "/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia"

        if svc_lower in ["all", "datasymbol", "data_symbol", "ds"]:
            add_from_cmd("dataSymbol", ["journalctl", "--user", "-u", "atalaia-2-datasymbol", "-n", "30", "--no-pager"])
        
        if svc_lower in ["all", "sentinel", "bots", "bot"]:
            add_from_cmd("Sentinel", ["journalctl", "--user", "-u", "atalaia-4-sentinel", "-n", "30", "--no-pager"])

        if svc_lower in ["all", "microratio", "ratio"]:
            add_from_file("microRatio", f"{base_dir}/logs/microRatio.log", 30)

        if svc_lower in ["all", "backend", "fastapi"]:
            add_from_file("Backend-FastAPI", f"{base_dir}/logs/backend_output.log", 25)

        if svc_lower in ["all", "mt5bridge", "mt5"]:
            add_from_file("MT5-Bridge", f"{base_dir}/logs/mt5_bridge_output.log", 25)

        if svc_lower in ["all", "connectionpool", "cp"]:
            add_from_cmd("ConnectionPool", ["journalctl", "--user", "-u", "atalaia-1-connectionpool", "-n", "20", "--no-pager"])

        if svc_lower in ["webhook"]:
            add_from_cmd("WebHook", ["journalctl", "--user", "-u", "atalaia-3-webhook", "-n", "30", "--no-pager"])

        # Filtrar o tomar las últimas max_lines
        return raw_entries[-max_lines:]

system_monitor = SystemMonitorService()

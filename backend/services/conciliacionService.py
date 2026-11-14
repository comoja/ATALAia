import os
import sys
import logging
import asyncio
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

# Asegurar que la ruta raíz del Sistema esté en sys.path para importar Webhook
sistema_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if sistema_root not in sys.path:
    sys.path.insert(0, sistema_root)

import httpx
from sqlalchemy import text
from sqlalchemy.orm import Session
from Webhook.services.forex_client import ForexClient

logger = logging.getLogger(__name__)

async def obtenerConciliacionPosiciones(
    dbSession: Session,
    idUsuarioFiltro: Optional[int] = None
) -> Dict[str, Any]:
    """
    Realiza una auditoría y conciliación de posiciones abiertas (ESTRICTAMENTE DE SOLO CONSULTA)
    entre la base de datos de ATALAia (tabla trades con status='OPEN') y el broker FOREX.com (OpenPositions).
    
    No realiza ningún cambio en base de datos ni en el broker.
    Estructura jerárquica de agrupación:
      Usuario -> Cuenta -> Estrategia -> Símbolo -> Transacciones (por id de transacción/ticketId)
    """
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"🔍 [CONCILIACIÓN] Iniciando auditoría de posiciones (filtro idUsuario={idUsuarioFiltro})...")

    # 1. Mapeo de idForex (MarketId) a Símbolo
    sym_rows = dbSession.execute(text("""
        SELECT idForex, symbol FROM sentinelsymbol WHERE idForex IS NOT NULL AND idForex != ''
        UNION 
        SELECT idForex, symbol FROM symbols WHERE idForex IS NOT NULL AND idForex != ''
    """)).fetchall()
    
    market_to_sym = {}
    for r in sym_rows:
        if r[0] and str(r[0]).isdigit():
            market_to_sym[int(r[0])] = str(r[1]).strip()

    # 2. Cuentas activas con credenciales en brokercuenta
    user_filter_sql = "AND uc.idUsuario = :uid" if idUsuarioFiltro else ""
    user_filter_param = {"uid": idUsuarioFiltro} if idUsuarioFiltro else {}

    cuentas_raw = dbSession.execute(text(f"""
        SELECT 
            c.idCuenta, 
            c.Nombre AS nombreCuenta, 
            bc.loginUsuario, 
            bc.tokenAcceso, 
            COALESCE(u.idUsuario, 0) AS idUsuario, 
            COALESCE(u.nombre, 'Sin Asignar') AS nombreUsuario, 
            COALESCE(u.email, 'sin_correo') AS emailUsuario
        FROM cuenta c
        JOIN brokercuenta bc ON bc.idCuenta = c.idCuenta AND bc.activo = 1
        LEFT JOIN usuarioCuenta uc ON uc.idCuenta = c.idCuenta AND uc.activo = 1
        LEFT JOIN usuario u ON u.idUsuario = uc.idUsuario
        WHERE c.Activo = 1 {user_filter_sql}
        ORDER BY u.idUsuario, c.idCuenta
    """), user_filter_param).mappings().fetchall()

    if not cuentas_raw:
        return {
            "status": "success",
            "timestamp": now_str,
            "kpis": {
                "totalCuentas": 0,
                "totalAtalaia": 0,
                "totalForex": 0,
                "totalConciliadas": 0,
                "totalHuerfanasAtalaia": 0,
                "totalHuerfanasForex": 0,
                "totalDiferencias": 0,
                "estadoGlobal": "CONCILIADO"
            },
            "usuarios": []
        }

    # Cuentas válidas y mapa por idCuenta
    cuenta_map = {}
    cuentas_por_login = {}
    user_cuentas_map = {}

    for c in cuentas_raw:
        c_dict = dict(c)
        idc = c_dict["idCuenta"]
        login = c_dict["loginUsuario"]
        uid = c_dict["idUsuario"]

        cuenta_map[idc] = c_dict

        if login not in cuentas_por_login:
            cuentas_por_login[login] = []
        cuentas_por_login[login].append(c_dict)

        if uid not in user_cuentas_map:
            user_cuentas_map[uid] = {
                "idUsuario": uid,
                "nombre": c_dict["nombreUsuario"],
                "email": c_dict["emailUsuario"],
                "cuentas": []
            }
        if c_dict not in user_cuentas_map[uid]["cuentas"]:
            user_cuentas_map[uid]["cuentas"].append(c_dict)

    # 3. Consultar trades OPEN en ATALAia
    id_cuentas_list = list(cuenta_map.keys())
    trades_open_raw = dbSession.execute(text("""
        SELECT 
            t.idTrade, 
            t.idCuenta, 
            t.strategy, 
            t.setup, 
            t.symbol, 
            t.direction, 
            t.size, 
            t.entryPrice, 
            t.ticketId, 
            t.openTime
        FROM trades t
        WHERE t.status = 'OPEN' AND t.idCuenta IN :idcs
        ORDER BY t.idCuenta, t.strategy, t.symbol, t.idTrade
    """), {"idcs": id_cuentas_list}).mappings().fetchall()
    
    # Liberar la conexión y transacción de MySQL antes de realizar llamadas HTTP a FOREX.com
    try:
        dbSession.commit()
    except Exception:
        pass

    atalaia_trades_by_ticket = {}
    atalaia_orphan_trades = []

    for t in trades_open_raw:
        t_dict = dict(t)
        raw_tick = str(t_dict.get("ticketId") or "").strip()
        tickets = [tk.strip() for tk in raw_tick.split(",") if tk.strip() and tk.strip() not in ["0", "None", "", "CLOSED"]]
        if tickets:
            for tk in tickets:
                t_copy = dict(t_dict)
                t_copy["ticketIdSingle"] = tk
                atalaia_trades_by_ticket[tk] = t_copy
        else:
            # Trade OPEN sin ticket válido asignado
            t_copy = dict(t_dict)
            t_copy["ticketIdSingle"] = f"ATALAIA-NO-TICKET-{t_copy['idTrade']}"
            atalaia_orphan_trades.append(t_copy)

    # 4. Consultar posiciones abiertas en FOREX.com para cada login único
    forex_positions_by_ticket = {}
    unique_logins = {}
    for c in cuentas_raw:
        login = c["loginUsuario"]
        token = c["tokenAcceso"]
        if login and login not in unique_logins:
            unique_logins[login] = token

    for login, token in unique_logins.items():
        try:
            client = ForexClient(username=login, password=token)
            auth_ok = await client.authenticate()
            if auth_ok:
                info_ok = await client.get_account_info()
                if info_ok and client.trading_account_id:
                    headers = {"UserName": client.username, "Session": client.session_token}
                    url = f"{client.api_url}/order/openpositions?TradingAccountId={client.trading_account_id}"
                    async with httpx.AsyncClient() as http_client:
                        res = None
                        for attempt in range(3):
                            res = await http_client.get(url, headers=headers, timeout=20)
                            if res.status_code == 429:
                                logger.warning(f"⚠️ [CONCILIACIÓN] FOREX.com 429 Too Many Requests para {login}. Esperando {(attempt+1)*3}s...")
                                await asyncio.sleep((attempt + 1) * 3)
                                continue
                            break

                        if res is not None and res.status_code == 200:
                            pos_list = res.json().get("OpenPositions", [])
                            logger.info(f"📥 [CONCILIACIÓN] FOREX.com login '{login}': {len(pos_list)} posiciones abiertas recibidas.")
                            for p in pos_list:
                                oid = str(p.get("OrderId"))
                                p_dict = dict(p)
                                p_dict["loginUsuario"] = login
                                p_dict["TradingAccountId"] = client.trading_account_id
                                # Resolver símbolo de forex
                                mid = int(p.get("MarketId") or 0)
                                sym_resuelto = market_to_sym.get(mid) or str(p.get("MarketName") or f"MarketId {mid}").strip()
                                p_dict["symbolResuelto"] = sym_resuelto
                                forex_positions_by_ticket[oid] = p_dict
                        else:
                            logger.error(f"❌ [CONCILIACIÓN] Error HTTP {res.status_code} consultando openpositions para {login}: {res.text}")
                else:
                    logger.error(f"❌ [CONCILIACIÓN] No se pudo obtener TradingAccountId para {login}")
            else:
                logger.error(f"❌ [CONCILIACIÓN] Falló autenticación en FOREX.com para {login}")
        except Exception as e:
            logger.error(f"❌ [CONCILIACIÓN] Excepción consultando FOREX.com para {login}: {e}", exc_info=True)

    # 5. Cruce Transaccional de Conciliación
    # Identificar todos los tickets involucrados
    all_ticket_keys = set(atalaia_trades_by_ticket.keys()).union(set(forex_positions_by_ticket.keys()))

    reconciled_items = []

    for tk in all_ticket_keys:
        in_atalaia = tk in atalaia_trades_by_ticket
        in_forex = tk in forex_positions_by_ticket

        if in_atalaia and in_forex:
            t = atalaia_trades_by_ticket[tk]
            f = forex_positions_by_ticket[tk]

            dir_a = "BUY" if str(t.get("direction") or "").upper() in ["BUY", "LARGO"] else "SELL"
            dir_f = str(f.get("Direction") or "").upper()
            qty_a = float(t.get("size") or 0.0)
            qty_f = float(f.get("Quantity") or 0.0)
            px_a = float(t.get("entryPrice") or 0.0)
            px_f = float(f.get("Price") or 0.0)
            sym_a = str(t.get("symbol") or "").strip()
            sym_f = str(f.get("symbolResuelto") or "").strip()

            diffs = []
            if dir_a != dir_f:
                diffs.append(f"Dirección difiere: ATALAia={dir_a} vs FOREX={dir_f}")
            if abs(qty_a - qty_f) > 0.001:
                diffs.append(f"Volumen difiere: ATALAia={qty_a:g} vs FOREX={qty_f:g}")

            if diffs:
                st = "DIFERENCIA"
                det = " | ".join(diffs)
            else:
                st = "MATCH"
                det = "Coincidencia exacta de ticket, símbolo y volumen"

            open_time_str = t.get("openTime").strftime("%Y-%m-%d %H:%M:%S") if isinstance(t.get("openTime"), datetime) else str(t.get("openTime") or "-")
            created_utc_str = str(f.get("CreatedDateTimeUTC") or "-")

            reconciled_items.append({
                "ticketId": tk,
                "idTrade": t.get("idTrade"),
                "idCuenta": t.get("idCuenta"),
                "idUsuario": cuenta_map.get(t.get("idCuenta"), {}).get("idUsuario", 0),
                "strategy": str(t.get("strategy") or "Sin Estrategia").strip(),
                "symbol": sym_a,
                "status": st,
                "detalleDiferencia": det,
                "direccionAtalaia": dir_a,
                "direccionForex": dir_f,
                "lotesAtalaia": qty_a,
                "lotesForex": qty_f,
                "precioAtalaia": px_a,
                "precioForex": px_f,
                "fechaAtalaia": open_time_str,
                "fechaForex": created_utc_str,
                "loginBroker": f.get("loginUsuario", "")
            })

        elif in_atalaia:
            # Huérfano en ATALAia
            t = atalaia_trades_by_ticket[tk]
            dir_a = "BUY" if str(t.get("direction") or "").upper() in ["BUY", "LARGO"] else "SELL"
            qty_a = float(t.get("size") or 0.0)
            px_a = float(t.get("entryPrice") or 0.0)
            sym_a = str(t.get("symbol") or "").strip()
            open_time_str = t.get("openTime").strftime("%Y-%m-%d %H:%M:%S") if isinstance(t.get("openTime"), datetime) else str(t.get("openTime") or "-")

            reconciled_items.append({
                "ticketId": tk,
                "idTrade": t.get("idTrade"),
                "idCuenta": t.get("idCuenta"),
                "idUsuario": cuenta_map.get(t.get("idCuenta"), {}).get("idUsuario", 0),
                "strategy": str(t.get("strategy") or "Sin Estrategia").strip(),
                "symbol": sym_a,
                "status": "HUERFANO_ATALAIA",
                "detalleDiferencia": "Registrada como ABIERTA en ATALAia, pero inexistente o liquidada en FOREX.com",
                "direccionAtalaia": dir_a,
                "direccionForex": "-",
                "lotesAtalaia": qty_a,
                "lotesForex": 0.0,
                "precioAtalaia": px_a,
                "precioForex": 0.0,
                "fechaAtalaia": open_time_str,
                "fechaForex": "-",
                "loginBroker": cuenta_map.get(t.get("idCuenta"), {}).get("loginUsuario", "")
            })

        elif in_forex:
            # Huérfano en FOREX.com
            f = forex_positions_by_ticket[tk]
            login = f.get("loginUsuario", "")
            cuentas_candidatas = cuentas_por_login.get(login, [])
            
            # Asociar a la cuenta candidata del usuario
            if cuentas_candidatas:
                cuenta_asig = cuentas_candidatas[0]
                id_c = cuenta_asig["idCuenta"]
                id_u = cuenta_asig["idUsuario"]
            else:
                id_c = 0
                id_u = 0

            dir_f = str(f.get("Direction") or "").upper()
            qty_f = float(f.get("Quantity") or 0.0)
            px_f = float(f.get("Price") or 0.0)
            sym_f = str(f.get("symbolResuelto") or "").strip()
            created_utc_str = str(f.get("CreatedDateTimeUTC") or "-")

            reconciled_items.append({
                "ticketId": tk,
                "idTrade": None,
                "idCuenta": id_c,
                "idUsuario": id_u,
                "strategy": "Sin Registrar en ATALAia (Externo)",
                "symbol": sym_f,
                "status": "HUERFANO_FOREX",
                "detalleDiferencia": "Posición abierta en FOREX.com sin registro correspondiente en ATALAia",
                "direccionAtalaia": "-",
                "direccionForex": dir_f,
                "lotesAtalaia": 0.0,
                "lotesForex": qty_f,
                "precioAtalaia": 0.0,
                "precioForex": px_f,
                "fechaAtalaia": "-",
                "fechaForex": created_utc_str,
                "loginBroker": login
            })

    # Agregar trades sin ticket válido
    for t in atalaia_orphan_trades:
        dir_a = "BUY" if str(t.get("direction") or "").upper() in ["BUY", "LARGO"] else "SELL"
        qty_a = float(t.get("size") or 0.0)
        px_a = float(t.get("entryPrice") or 0.0)
        sym_a = str(t.get("symbol") or "").strip()
        open_time_str = t.get("openTime").strftime("%Y-%m-%d %H:%M:%S") if isinstance(t.get("openTime"), datetime) else str(t.get("openTime") or "-")

        reconciled_items.append({
            "ticketId": t["ticketIdSingle"],
            "idTrade": t.get("idTrade"),
            "idCuenta": t.get("idCuenta"),
            "idUsuario": cuenta_map.get(t.get("idCuenta"), {}).get("idUsuario", 0),
            "strategy": str(t.get("strategy") or "Sin Estrategia").strip(),
            "symbol": sym_a,
            "status": "HUERFANO_ATALAIA",
            "detalleDiferencia": "Trade OPEN en ATALAia sin TicketId de broker asignado",
            "direccionAtalaia": dir_a,
            "direccionForex": "-",
            "lotesAtalaia": qty_a,
            "lotesForex": 0.0,
            "precioAtalaia": px_a,
            "precioForex": 0.0,
            "fechaAtalaia": open_time_str,
            "fechaForex": "-",
            "loginBroker": cuenta_map.get(t.get("idCuenta"), {}).get("loginUsuario", "")
        })

    # 6. Agrupamiento Jerárquico: Usuario -> Cuenta -> Estrategia -> Símbolo -> Posiciones
    def make_kpi():
        return {
            "totalAtalaia": 0,
            "totalForex": 0,
            "totalConciliadas": 0,
            "totalHuerfanasAtalaia": 0,
            "totalHuerfanasForex": 0,
            "totalDiferencias": 0,
            "estado": "CONCILIADO"
        }

    def update_kpi(kpi, st):
        if st == "MATCH":
            kpi["totalAtalaia"] += 1
            kpi["totalForex"] += 1
            kpi["totalConciliadas"] += 1
        elif st == "DIFERENCIA":
            kpi["totalAtalaia"] += 1
            kpi["totalForex"] += 1
            kpi["totalDiferencias"] += 1
            kpi["estado"] = "DISCREPANCIAS"
        elif st == "HUERFANO_ATALAIA":
            kpi["totalAtalaia"] += 1
            kpi["totalHuerfanasAtalaia"] += 1
            kpi["estado"] = "DISCREPANCIAS"
        elif st == "HUERFANO_FOREX":
            kpi["totalForex"] += 1
            kpi["totalHuerfanasForex"] += 1
            kpi["estado"] = "DISCREPANCIAS"

    global_kpi = make_kpi()
    global_kpi["totalCuentas"] = len(cuentas_raw)

    tree_usuarios = {}

    # Pre-poblar todos los usuarios y todas sus cuentas auditadas para que siempre aparezcan
    # aunque no tengan posiciones abiertas activas (cuentas planas / conciliadas en 0)
    for uid, u_info in user_cuentas_map.items():
        tree_usuarios[uid] = {
            "idUsuario": uid,
            "nombre": u_info["nombre"],
            "email": u_info["email"],
            "kpis": make_kpi(),
            "cuentasDict": {}
        }
        for c_dict in u_info["cuentas"]:
            idc = c_dict["idCuenta"]
            tree_usuarios[uid]["cuentasDict"][idc] = {
                "idCuenta": idc,
                "nombreCuenta": c_dict["nombreCuenta"],
                "brokerLogin": c_dict.get("loginUsuario", ""),
                "kpis": make_kpi(),
                "estrategiasDict": {}
            }

    for item in reconciled_items:
        uid = item["idUsuario"]
        idc = item["idCuenta"]
        strat = item["strategy"]
        sym = item["symbol"]
        st = item["status"]

        update_kpi(global_kpi, st)

        # Usuario
        if uid not in tree_usuarios:
            u_info = user_cuentas_map.get(uid, {"nombre": f"Usuario #{uid}", "email": ""})
            tree_usuarios[uid] = {
                "idUsuario": uid,
                "nombre": u_info["nombre"],
                "email": u_info["email"],
                "kpis": make_kpi(),
                "cuentasDict": {}
            }
        u_node = tree_usuarios[uid]
        update_kpi(u_node["kpis"], st)

        # Cuenta
        if idc not in u_node["cuentasDict"]:
            c_info = cuenta_map.get(idc, {"nombreCuenta": f"Cuenta #{idc}", "loginUsuario": item.get("loginBroker", "")})
            u_node["cuentasDict"][idc] = {
                "idCuenta": idc,
                "nombreCuenta": c_info["nombreCuenta"],
                "brokerLogin": c_info.get("loginUsuario", ""),
                "kpis": make_kpi(),
                "estrategiasDict": {}
            }
        c_node = u_node["cuentasDict"][idc]
        update_kpi(c_node["kpis"], st)

        # Estrategia
        if strat not in c_node["estrategiasDict"]:
            c_node["estrategiasDict"][strat] = {
                "estrategia": strat,
                "kpis": make_kpi(),
                "simbolosDict": {}
            }
        e_node = c_node["estrategiasDict"][strat]
        update_kpi(e_node["kpis"], st)

        # Símbolo
        if sym not in e_node["simbolosDict"]:
            e_node["simbolosDict"][sym] = {
                "symbol": sym,
                "kpis": make_kpi(),
                "posiciones": []
            }
        s_node = e_node["simbolosDict"][sym]
        update_kpi(s_node["kpis"], st)

        # Posición
        s_node["posiciones"].append(item)

    # Convertir diccionarios internos a listas ordenadas
    usuarios_list = []
    for uid, u_val in tree_usuarios.items():
        cuentas_list = []
        for idc, c_val in u_val["cuentasDict"].items():
            estrategias_list = []
            for strat, e_val in c_val["estrategiasDict"].items():
                simbolos_list = []
                for sym, s_val in e_val["simbolosDict"].items():
                    # Ordenar posiciones por estado (discrepancias primero, luego conciliadas)
                    s_val["posiciones"].sort(key=lambda x: (0 if x["status"] != "MATCH" else 1, str(x["ticketId"])))
                    simbolos_list.append({
                        "symbol": s_val["symbol"],
                        "kpis": s_val["kpis"],
                        "posiciones": s_val["posiciones"]
                    })
                # Ordenar símbolos: primero los que tienen discrepancias
                simbolos_list.sort(key=lambda s: (0 if s["kpis"]["estado"] == "DISCREPANCIAS" else 1, s["symbol"]))
                estrategias_list.append({
                    "estrategia": e_val["estrategia"],
                    "kpis": e_val["kpis"],
                    "simbolos": simbolos_list
                })
            estrategias_list.sort(key=lambda e: (0 if e["kpis"]["estado"] == "DISCREPANCIAS" else 1, e["estrategia"]))
            cuentas_list.append({
                "idCuenta": c_val["idCuenta"],
                "nombreCuenta": c_val["nombreCuenta"],
                "brokerLogin": c_val["brokerLogin"],
                "kpis": c_val["kpis"],
                "estrategias": estrategias_list
            })
        cuentas_list.sort(key=lambda c: (0 if c["kpis"]["estado"] == "DISCREPANCIAS" else 1, c["idCuenta"]))
        usuarios_list.append({
            "idUsuario": u_val["idUsuario"],
            "nombre": u_val["nombre"],
            "email": u_val["email"],
            "kpis": u_val["kpis"],
            "cuentas": cuentas_list
        })
    usuarios_list.sort(key=lambda u: (0 if u["kpis"]["estado"] == "DISCREPANCIAS" else 1, u["idUsuario"]))

    global_kpi["estadoGlobal"] = "DISCREPANCIAS" if (global_kpi["totalHuerfanasAtalaia"] > 0 or global_kpi["totalHuerfanasForex"] > 0 or global_kpi["totalDiferencias"] > 0) else "CONCILIADO"

    logger.info(f"✅ [CONCILIACIÓN] Finalizada: {global_kpi['totalConciliadas']} Match, "
                f"{global_kpi['totalHuerfanasAtalaia']} Huérfanas ATALAia, "
                f"{global_kpi['totalHuerfanasForex']} Huérfanas FOREX, "
                f"{global_kpi['totalDiferencias']} Diferencias.")

    return {
        "status": "success",
        "timestamp": now_str,
        "kpis": global_kpi,
        "usuarios": usuarios_list
    }

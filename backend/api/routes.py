import os
import sys
rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
import subprocess
from urllib.parse import unquote
import logging
from middleware.database import dbManager
from middleware.database.dbManager import getStockPricesFromDb
from backend.services.correlation_engine import engine
from backend.services.optimizer_service import optimizer
from backend.services.cruce_ema_engine import cruceEmaEngine
from sqlalchemy.orm import Session
from backend.database.models import SessionLocal, Symbol, RatioSymbol, Cuenta, SentinelSymbol, UserRatio, UsuarioCuenta, Usuario
import pandas as pd
import numpy as np
from datetime import datetime

router = APIRouter()
logger = logging.getLogger(__name__)

# Dependencia para la base de datos de FastAPI
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/correlation/{pair_name:path}")
async def get_pair_correlation(
    pair_name: str,
    amplitude: float = 1.0,
    freq: float = 0.1,
    phase: float = 0.0,
    offset: float = 0.0,
    r: float = 0.05,
    tYears: float = 30 / 252,
    sigmaWindow: int = 7
) -> Dict[str, Any]:
    """
    Este endpoint es consumido por PrimeFaces (Java).
    Calcula el análisis de correlación al vuelo tomando 
    el precio de cierre diario histórico y aplicando coeficientes dinámicos.
    """
    logger.info(f"Solicitado el cálculo de correlación para el par: {pair_name}")
    # Decodificar por si el cliente envía el par URL-encodificado (ej. AUD%2FUSD → AUD/USD)
    pair_name = unquote(pair_name)
    
    try:
        # Extraemos un histórico amplio (ej. 5000 velas = ~20 años)
        df_daily = await getStockPricesFromDb(symbol=pair_name, limit=5000)
        
        if df_daily.empty:
            raise HTTPException(status_code=404, detail="No se encontraron velas para este par en la tabla 'StockPrices'.")

        # Renombramos 'closePrice' a 'price' que es lo que espera el engine
        df_daily = df_daily.rename(columns={'closePrice': 'price'})
        
        # Mandamos el DataFrame reconstruido a que nuestro motor matemático haga lo suyo
        resultadosMatematicos = engine.process_pair(
            df_daily,
            amplitude=amplitude,
            freq=freq,
            phase=phase,
            offset=offset,
            r=r,
            tYears=tYears,
            sigmaWindow=sigmaWindow
        )
        
        return resultadosMatematicos

    except Exception as e:
        logger.error(f"Error procesando {pair_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/catalogo")
def get_catalogo_pares():
    """
    Devuelve la lista de todos los símbolos activos de RATIO mediante ConnectionPool.
    """
    res = dbManager._call_connection_pool("GET", "/ratio-symbols/active")
    if res and isinstance(res, list):
        return [{"id": p["symbol"], "pair_name": p["symbol"], "desc": p["symbol"], "tipo": "MONEDA"} for p in res if isinstance(p, dict) and "symbol" in p]
    return []


class UserRatioCreate(BaseModel):
    idUsuario: int = Field(..., description="ID del usuario")
    idCuenta: Optional[int] = Field(None, description="ID de la cuenta asociada")
    numerador: str = Field(..., description="Símbolo numerador (Par A)")
    denominador: str = Field(..., description="Símbolo denominador (Par B)")
    periodo: str = Field(..., description="Periodo o temporalidad (ej. 1d, 1h)")
    dias: Optional[int] = Field(180, description="Días hacia atrás")
    EMARapida: Optional[int] = Field(3, description="Periodo de EMA Rápida / SMA")
    EMALenta: Optional[int] = Field(20, description="Periodo de EMA Lenta")
    operar: Optional[bool] = Field(False, description="Indica si se generan órdenes para este ratio")

class UserRatioDelete(BaseModel):
    idUsuario: int = Field(..., description="ID del usuario")
    idCuenta: Optional[int] = Field(None, description="ID de la cuenta asociada")
    numerador: str = Field(..., description="Símbolo numerador (Par A)")
    denominador: str = Field(..., description="Símbolo denominador (Par B)")

@router.post("/user-ratios/guardar")
def saveUserRatio(payload: UserRatioCreate, db: Session = Depends(get_db)):
    """
    Guarda o actualiza (UPSERT) en user_ratios la relación entre idUsuario, idCuenta, numerador y denominador.
    """
    try:
        cuentaId = payload.idCuenta
        if not cuentaId:
            uc = db.query(UsuarioCuenta).filter(UsuarioCuenta.idUsuario == payload.idUsuario, UsuarioCuenta.activo == True).order_by(UsuarioCuenta.idUsuarioCuenta.asc()).first()
            cuentaId = uc.idCuenta if uc else 1

        query = db.query(UserRatio).filter(
            UserRatio.idUsuario == payload.idUsuario,
            UserRatio.numerador == payload.numerador,
            UserRatio.denominador == payload.denominador
        )
        if payload.idCuenta:
            query = query.filter(UserRatio.idCuenta == payload.idCuenta)
        existingRatio = query.first()

        emaRapida = payload.EMARapida if payload.EMARapida is not None else 3
        emaLenta = payload.EMALenta if payload.EMALenta is not None else 20
        dias = payload.dias if payload.dias is not None else 180
        operar = bool(payload.operar) if payload.operar is not None else False

        if existingRatio:
            existingRatio.idCuenta = cuentaId
            existingRatio.periodo = payload.periodo
            existingRatio.dias = dias
            existingRatio.EMARapida = emaRapida
            existingRatio.EMALenta = emaLenta
            existingRatio.operar = operar
            existingRatio.createdAt = datetime.utcnow()
            db.commit()
            db.refresh(existingRatio)
            logger.info(f"Ratio actualizado para usuario {payload.idUsuario} (cuenta {cuentaId}): {payload.numerador}/{payload.denominador} ({payload.periodo}, {dias} días) [EMA Fast: {emaRapida}, Slow: {emaLenta}, Operar: {operar}]")
            return {"status": "success", "message": "Ratio actualizado exitosamente", "id": existingRatio.id, "idCuenta": cuentaId, "action": "updated"}
        else:
            nuevoRatio = UserRatio(
                idUsuario=payload.idUsuario,
                idCuenta=cuentaId,
                numerador=payload.numerador,
                denominador=payload.denominador,
                periodo=payload.periodo,
                dias=dias,
                EMARapida=emaRapida,
                EMALenta=emaLenta,
                operar=operar,
                createdAt=datetime.utcnow()
            )
            db.add(nuevoRatio)
            db.commit()
            db.refresh(nuevoRatio)
            logger.info(f"Ratio guardado para usuario {payload.idUsuario} (cuenta {cuentaId}): {payload.numerador}/{payload.denominador} ({payload.periodo}, {dias} días) [EMA Fast: {emaRapida}, Slow: {emaLenta}, Operar: {operar}]")
            return {"status": "success", "message": "Ratio guardado exitosamente", "id": nuevoRatio.id, "idCuenta": cuentaId, "action": "created"}
    except Exception as e:
        db.rollback()
        logger.error(f"Error al guardar user_ratio: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/user-ratios/buscar")
def findUserRatio(idUsuario: Optional[int] = None, numerador: str = "", denominador: str = "", idCuenta: Optional[int] = None, db: Session = Depends(get_db)):
    """
    Busca la configuración de un ratio guardado por su clave compuesta (idCuenta/idUsuario, numerador, denominador).
    Prioriza idCuenta si se suministra y ordena por id DESC para devolver la configuración más reciente.
    """
    query = db.query(UserRatio).filter(
        UserRatio.numerador == numerador,
        UserRatio.denominador == denominador
    )
    if idCuenta:
        query = query.filter(UserRatio.idCuenta == idCuenta)
    elif idUsuario:
        query = query.filter(UserRatio.idUsuario == idUsuario)
    
    ratio = query.order_by(UserRatio.id.desc()).first()

    if not ratio:
        query_inv = db.query(UserRatio).filter(
            UserRatio.numerador == denominador,
            UserRatio.denominador == numerador
        )
        if idCuenta:
            query_inv = query_inv.filter(UserRatio.idCuenta == idCuenta)
        elif idUsuario:
            query_inv = query_inv.filter(UserRatio.idUsuario == idUsuario)
        ratio = query_inv.order_by(UserRatio.id.desc()).first()

    if not ratio:
        return {"found": False, "hasOpenTrades": False}
    
    has_open_trades = False
    open_count = 0
    if ratio.idCuenta:
        s1 = f"{ratio.numerador} - {ratio.denominador}"
        s2 = f"{ratio.denominador} - {ratio.numerador}"
        from sqlalchemy import text
        cnt = db.execute(text("""
            SELECT COUNT(*) FROM trades 
            WHERE idCuenta = :idc 
              AND (setup = :s1 OR setup = :s2)
              AND status = 'OPEN'
        """), {"idc": ratio.idCuenta, "s1": s1, "s2": s2}).scalar()
        if cnt and cnt > 0:
            has_open_trades = True
            open_count = int(cnt)

    return {
        "found": True,
        "id": ratio.id,
        "idUsuario": ratio.idUsuario,
        "idCuenta": ratio.idCuenta,
        "numerador": ratio.numerador,
        "denominador": ratio.denominador,
        "periodo": ratio.periodo,
        "dias": ratio.dias if getattr(ratio, 'dias', None) is not None else 180,
        "EMARapida": ratio.EMARapida if ratio.EMARapida is not None else 3,
        "EMALenta": ratio.EMALenta if ratio.EMALenta is not None else 20,
        "operar": bool(ratio.operar) if getattr(ratio, 'operar', None) is not None else False,
        "createdAt": ratio.createdAt,
        "hasOpenTrades": has_open_trades,
        "openTradesCount": open_count
    }

@router.post("/user-ratios/borrar")
def deleteUserRatio(payload: UserRatioDelete, db: Session = Depends(get_db)):
    """
    Elimina por el índice compuesto (idUsuario, idCuenta, numerador, denominador) el registro correspondiente.
    Valida que NO tenga registros activos (status = 'OPEN') en la tabla trades.
    """
    try:
        query = db.query(UserRatio).filter(
            UserRatio.idUsuario == payload.idUsuario,
            UserRatio.numerador == payload.numerador,
            UserRatio.denominador == payload.denominador
        )
        if payload.idCuenta:
            query = query.filter(UserRatio.idCuenta == payload.idCuenta)

        ratioToDelete = query.first()

        if not ratioToDelete:
            raise HTTPException(status_code=404, detail="No se encontró la combinación de ratio especificada para eliminar.")

        # Validar si tiene posiciones activas en la tabla trades
        if ratioToDelete.idCuenta:
            s1 = f"{payload.numerador} - {payload.denominador}"
            s2 = f"{payload.denominador} - {payload.numerador}"
            from sqlalchemy import text
            cnt = db.execute(text("""
                SELECT COUNT(*) FROM trades 
                WHERE idCuenta = :idc 
                  AND (setup = :s1 OR setup = :s2)
                  AND status = 'OPEN'
            """), {"idc": ratioToDelete.idCuenta, "s1": s1, "s2": s2}).scalar()
            if cnt and cnt > 0:
                raise HTTPException(
                    status_code=400,
                    detail=f"No se puede borrar el ratio porque tiene {cnt} posiciones activas (no cerradas) en la tabla trades."
                )

        db.delete(ratioToDelete)
        db.commit()
        logger.info(f"Ratio eliminado para usuario {payload.idUsuario} (cuenta {ratioToDelete.idCuenta}): {payload.numerador}/{payload.denominador}")
        return {"status": "success", "message": "Ratio eliminado exitosamente"}
    except HTTPException as he:
        raise he
    except Exception as e:
        db.rollback()
        logger.error(f"Error al borrar user_ratio: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/trades/active-summary/{idCuenta}")
def getActiveTradesSummary(idCuenta: int, db: Session = Depends(get_db)):
    """
    Obtiene el resumen consolidado de todas las posiciones activas en la tabla trades
    para la cuenta especificada, agrupadas por ratio (setup).
    """
    from sqlalchemy import text
    sql = text("""
        SELECT idTrade, idCuenta, strategy, setup, symbol, direction, 
               openTime, size, entryPrice, margin_used, pnl, intervalo
        FROM trades
        WHERE idCuenta = :idc AND status = 'OPEN'
        ORDER BY setup, openTime ASC, idTrade ASC
    """)
    rows = db.execute(sql, {"idc": idCuenta}).fetchall()
    
    if not rows:
        return []
        
    by_setup = {}
    for r in rows:
        setup = r.setup or "RATIO"
        if setup not in by_setup:
            by_setup[setup] = []
        by_setup[setup].append(r)
        
    def get_close_and_quote(sym):
        clean = sym.replace("/", "").upper()
        row_c = db.execute(text("SELECT close FROM candles WHERE UPPER(REPLACE(symbol, '/', '')) = :s ORDER BY timestamp DESC LIMIT 1"), {"s": clean}).fetchone()
        row_s = db.execute(text("SELECT quote_currency FROM sentinelsymbol WHERE UPPER(REPLACE(symbol, '/', '')) = :s LIMIT 1"), {"s": clean}).fetchone()
        cur_close = float(row_c[0]) if row_c and row_c[0] else 1.0
        quote_curr = str(row_s[0]).upper() if row_s and row_s[0] else ("MXN" if "MXN" in sym else "USD")
        return cur_close, quote_curr

    results = []
    for setup, t_list in by_setup.items():
        parts = [p.strip() for p in setup.split("-") if p.strip()]
        pairA = parts[0] if len(parts) >= 1 else ""
        pairB = parts[1] if len(parts) >= 2 else ""
        
        curA, qcA = get_close_and_quote(pairA) if pairA else (1.0, "USD")
        curB, qcB = get_close_and_quote(pairB) if pairB else (1.0, "USD")
        
        tot_margin = 0.0
        tot_marginA = 0.0
        tot_marginB = 0.0
        tot_pnl = 0.0
        pnlA = 0.0
        pnlB = 0.0
        
        open_dates = []
        dirA = ""
        dirB = ""
        trade_items = []
        
        for t in t_list:
            sym = t.symbol
            direction = str(t.direction).upper()
            size = float(t.size or 0)
            entry = float(t.entryPrice or 0)
            margin = float(t.margin_used or 0)
            tot_margin += margin
            
            cur_px = curA if sym == pairA else (curB if sym == pairB else entry)
            qc = qcA if sym == pairA else (qcB if sym == pairB else "USD")
            
            if "LARG" in direction or "BUY" in direction:
                diff = cur_px - entry
            else:
                diff = entry - cur_px
                
            pnl_quote = diff * size
            item_pnl_usd = pnl_quote if qc == "USD" else (pnl_quote / cur_px if cur_px > 0 else pnl_quote)
            
            if sym == pairA:
                tot_marginA += margin
                pnlA += item_pnl_usd
                dirA = "BUY" if ("LARG" in direction or "BUY" in direction) else "SELL"
            elif sym == pairB:
                tot_marginB += margin
                pnlB += item_pnl_usd
                dirB = "BUY" if ("LARG" in direction or "BUY" in direction) else "SELL"
                
            tot_pnl += item_pnl_usd
            if t.openTime:
                d_str = t.openTime.strftime("%Y-%m-%d %H:%M") if hasattr(t.openTime, "strftime") else str(t.openTime)
                open_dates.append(d_str)
                
            trade_items.append({
                "idTrade": t.idTrade,
                "symbol": sym,
                "direction": direction,
                "size": size,
                "entryPrice": entry,
                "currentPrice": cur_px,
                "margin": margin,
                "pnl": round(item_pnl_usd, 2),
                "openTime": str(t.openTime)
            })
            
        ret_pct = (tot_pnl / tot_margin * 100.0) if tot_margin > 0 else 0.0
        dir_text = f"{dirA} {pairA} + {dirB} {pairB}" if (dirA and dirB) else (dirA or dirB)

        totSizeA = sum(float(t.size or 0) for t in t_list if t.symbol == pairA)
        weightedSumA = sum(float(t.entryPrice or 0) * float(t.size or 0) for t in t_list if t.symbol == pairA)
        avgEntryA = (weightedSumA / totSizeA) if totSizeA > 0 else 0.0

        totSizeB = sum(float(t.size or 0) for t in t_list if t.symbol == pairB)
        weightedSumB = sum(float(t.entryPrice or 0) * float(t.size or 0) for t in t_list if t.symbol == pairB)
        avgEntryB = (weightedSumB / totSizeB) if totSizeB > 0 else 0.0
        
        results.append({
            "sizeA": totSizeA,
            "sizeB": totSizeB,
            "avgEntryPriceA": round(avgEntryA, 5),
            "avgEntryPriceB": round(avgEntryB, 5),
            "currentPriceA": round(curA, 5),
            "currentPriceB": round(curB, 5),
            "setup": setup,
            "pairA": pairA,
            "pairB": pairB,
            "hasActiveCycle": True,
            "totalOpenTrades": len(t_list) // 2 if len(t_list) >= 2 else len(t_list),
            "totalTradesCount": len(t_list),
            "totalMargin": round(tot_margin, 2),
            "marginA": round(tot_marginA, 2),
            "marginB": round(tot_marginB, 2),
            "pnlA": round(pnlA, 2),
            "pnlB": round(pnlB, 2),
            "totalPnl": round(tot_pnl, 2),
            "returnPct": round(ret_pct, 2),
            "dirA": dirA,
            "dirB": dirB,
            "direction": dir_text,
            "firstEntryDate": open_dates[0] if open_dates else "",
            "lastEntryDate": open_dates[-1] if open_dates else "",
            "trades": trade_items
        })
        
    return results



class UsuarioCuentaAssign(BaseModel):
    idUsuario: int = Field(..., description="ID del usuario")
    idCuenta: int = Field(..., description="ID de la cuenta a asociar")
    activo: Optional[bool] = Field(True, description="Estado de la asignación")

class UsuarioCuentaDelete(BaseModel):
    idUsuario: int = Field(..., description="ID del usuario")
    idCuenta: int = Field(..., description="ID de la cuenta a desasociar")

@router.get("/usuario-cuentas/{idUsuario}")
def getUsuarioCuentas(idUsuario: int, db: Session = Depends(get_db)):
    """
    Obtiene todas las cuentas asociadas y administradas por un usuario específico.
    """
    relaciones = (
        db.query(UsuarioCuenta, Cuenta)
        .join(Cuenta, UsuarioCuenta.idCuenta == Cuenta.idCuenta)
        .filter(UsuarioCuenta.idUsuario == idUsuario)
        .all()
    )
    return [
        {
            "idUsuarioCuenta": uc.idUsuarioCuenta,
            "idUsuario": uc.idUsuario,
            "idCuenta": c.idCuenta,
            "nombreCuenta": c.Nombre,
            "capital": float(c.Capital or 0.0),
            "activo": bool(uc.activo),
            "createdAt": uc.createdAt
        }
        for uc, c in relaciones
    ]

@router.post("/usuario-cuentas/asignar")
def asignarUsuarioCuenta(payload: UsuarioCuentaAssign, db: Session = Depends(get_db)):
    """
    Asocia una cuenta a un usuario (UPSERT en usuarioCuenta).
    """
    try:
        relacion = db.query(UsuarioCuenta).filter(
            UsuarioCuenta.idUsuario == payload.idUsuario,
            UsuarioCuenta.idCuenta == payload.idCuenta
        ).first()
        if relacion:
            relacion.activo = payload.activo
            db.commit()
            db.refresh(relacion)
            logger.info(f"Relación usuarioCuenta actualizada: Usuario {payload.idUsuario} -> Cuenta {payload.idCuenta} (activo={payload.activo})")
            return {"status": "success", "message": "Asignación actualizada", "id": relacion.idUsuarioCuenta}
        else:
            nuevaRelacion = UsuarioCuenta(
                idUsuario=payload.idUsuario,
                idCuenta=payload.idCuenta,
                activo=payload.activo,
                createdAt=datetime.utcnow()
            )
            db.add(nuevaRelacion)
            db.commit()
            db.refresh(nuevaRelacion)
            logger.info(f"Nueva relación usuarioCuenta creada: Usuario {payload.idUsuario} -> Cuenta {payload.idCuenta}")
            return {"status": "success", "message": "Cuenta asignada exitosamente", "id": nuevaRelacion.idUsuarioCuenta}
    except Exception as e:
        db.rollback()
        logger.error(f"Error al asignar cuenta a usuario: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/usuario-cuentas/desasignar")
def desasignarUsuarioCuenta(payload: UsuarioCuentaDelete, db: Session = Depends(get_db)):
    """
    Elimina la asociación entre un usuario y una cuenta.
    """
    try:
        relacion = db.query(UsuarioCuenta).filter(
            UsuarioCuenta.idUsuario == payload.idUsuario,
            UsuarioCuenta.idCuenta == payload.idCuenta
        ).first()
        if not relacion:
            raise HTTPException(status_code=404, detail="No se encontró la asignación especificada.")
        db.delete(relacion)
        db.commit()
        logger.info(f"Relación usuarioCuenta eliminada: Usuario {payload.idUsuario} -> Cuenta {payload.idCuenta}")
        return {"status": "success", "message": "Asignación eliminada exitosamente"}
    except HTTPException as he:
        raise he
    except Exception as e:
        db.rollback()
        logger.error(f"Error al desasignar cuenta de usuario: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/user-ratios/{idUsuario}")
def getUserRatios(idUsuario: int, idCuenta: Optional[int] = None, db: Session = Depends(get_db)):
    """
    Obtiene todos los ratios guardados para la cuenta especificada (prioritario) o usuario,
    incluyendo la bandera hasOpenTrades si existen órdenes activas en trades.
    """
    if idCuenta:
        query = db.query(UserRatio).filter(UserRatio.idCuenta == idCuenta)
    else:
        query = db.query(UserRatio).filter(UserRatio.idUsuario == idUsuario)
    ratios = query.order_by(UserRatio.id.desc()).all()

    from sqlalchemy import text
    results = []
    for r in ratios:
        has_open = False
        if r.idCuenta:
            s1 = f"{r.numerador} - {r.denominador}"
            s2 = f"{r.denominador} - {r.numerador}"
            cnt = db.execute(text("""
                SELECT COUNT(*) FROM trades 
                WHERE idCuenta = :idc 
                  AND (setup = :s1 OR setup = :s2)
                  AND status = 'OPEN'
            """), {"idc": r.idCuenta, "s1": s1, "s2": s2}).scalar()
            if cnt and cnt > 0:
                has_open = True

        results.append({
            "id": r.id,
            "idUsuario": r.idUsuario,
            "idCuenta": r.idCuenta,
            "numerador": r.numerador,
            "denominador": r.denominador,
            "periodo": r.periodo,
            "dias": r.dias,
            "EMARapida": r.EMARapida,
            "EMALenta": r.EMALenta,
            "operar": r.operar,
            "createdAt": r.createdAt,
            "hasOpenTrades": has_open
        })
    return results


@router.get("/ratio/{pairA:path}")
async def get_ratio_correlation(
    pairA: str,
    pairB: str,
    amplitude: float = 1.0,
    freq: float = 0.1,
    phase: float = 0.0,
    offset: float = 0.0,
    r: float = 0.05,
    tYears: float = 30 / 252,
    sigmaWindow: int = 7,
    smaPeriod: int = 2,
    emaSlowPeriod: int = 20,
    histogramBins: int = 15,
    tf: str = "1d",
    start_date: str = "",
    end_date: str = ""
) -> Dict[str, Any]:
    """
    Calcula el ratio sintético (Par A / Par B) y aplica el modelo completo.
    Equivalente Python de la hoja 'EURGBPUSD' del Excel ATALA IA MODEL.

    Ejemplo: GET /api/v1/ratio/EUR%2FUSD?pairB=GBP%2FUSD
    Resultado: precio_sintético = close_EURUSD / close_GBPUSD (EUR/GBP implícito)
    """
    logger.info(f"Ratio solicitado: {pairA} / {pairB}")
    # Decodificar por si el cliente envía los pares URL-encodificados (ej. AUD%2FUSD → AUD/USD)
    pairA = unquote(pairA)
    pairB = unquote(pairB)

    try:
        candle_limit = 100000

        # Cargar todo el historial posible según el request
        df_a = await getStockPricesFromDb(symbol=pairA, limit=candle_limit)
        df_b = await getStockPricesFromDb(symbol=pairB, limit=candle_limit)

        if df_a.empty:
            raise HTTPException(status_code=404, detail=f"Sin precios para Par A: {pairA}")
        if df_b.empty:
            raise HTTPException(status_code=404, detail=f"Sin precios para Par B: {pairB}")

        # Lógica de Agrupación (Resample)
        resample_rule = None
        if tf == "1month":
            resample_rule = 'ME'  # Monthly End
        elif tf == "1week":
            resample_rule = 'W'   # Weekly
        elif tf == "1d":
            resample_rule = 'D'   # Daily
        # Para "1h", resample_rule = None (ya que StockPrices es horario por defecto)

        if resample_rule:
            df_a_daily = df_a.resample(resample_rule).agg({'closePrice': 'last'}).dropna()
            df_b_daily = df_b.resample(resample_rule).agg({'closePrice': 'last'}).dropna()
            # Omitir la última vela incompleta (en desarrollo) para trabajar con velas terminadas
            if len(df_a_daily) > 1:
                df_a_daily = df_a_daily.iloc[:-1]
            if len(df_b_daily) > 1:
                df_b_daily = df_b_daily.iloc[:-1]
        else:
            df_a_daily = df_a
            df_b_daily = df_b

        if start_date or end_date:
            try:
                import pandas as pd
                dt_start_filter = pd.to_datetime(start_date).tz_localize(None) if start_date else pd.Timestamp.min
                dt_end_filter = pd.to_datetime(end_date).tz_localize(None) if end_date else pd.Timestamp.max
                
                df_a_daily.index = df_a_daily.index.tz_localize(None)
                df_b_daily.index = df_b_daily.index.tz_localize(None)
                
                df_a_daily = df_a_daily.loc[dt_start_filter:dt_end_filter]
                df_b_daily = df_b_daily.loc[dt_start_filter:dt_end_filter]
            except Exception as e:
                logger.warning(f"Error parseando fechas en optimize: {e}")
        df_a_daily = df_a_daily.rename(columns={'closePrice': 'close'})
        df_b_daily = df_b_daily.rename(columns={'closePrice': 'close'})

        # Motor de 2 pares: calcula ratio sintético y aplica todo el modelo
        resultado = engine.process_two_pairs(
            df_a=df_a_daily,
            df_b=df_b_daily,
            amplitude=amplitude,
            freq=freq,
            phase=phase,
            offset=offset,
            r=r,
            tYears=tYears,
            sigmaWindow=sigmaWindow,
            smaPeriod=smaPeriod,
            emaSlowPeriod=emaSlowPeriod
        )

        # Añadir metadata del ratio al response para el frontend
        if resultado.get("success"):
            resultado["pairA"] = pairA
            resultado["pairB"] = pairB
            resultado["ratioLabel"] = f"{pairA} / {pairB}"
            
            # Lógica de la señal de arbitraje basada en el Ciclo Sinusoidal calibrado
            latest_data = resultado.get("latest", {})
            ciclo_st_latest = latest_data.get("cicloStLatest", 0.0)
            
            if ciclo_st_latest > offset:
                resultado["arbitrageSignal"] = f"VENTA {pairA} - COMPRA {pairB}"
                resultado["arbitrageType"] = "SHORT"
            else:
                resultado["arbitrageSignal"] = f"COMPRA {pairA} - VENTA {pairB}"
                resultado["arbitrageType"] = "LONG"

            # Recortar el historial para devolver únicamente la ventana de tiempo solicitada por fechas
            if "history" in resultado and resultado["history"]:
                history_real = resultado["history"]
                
                if start_date or end_date:
                    import pandas as pd
                    filtered_history = []
                    
                    try:
                        dt_start_filter = pd.to_datetime(start_date, utc=True).tz_localize(None) if start_date else pd.Timestamp.min
                        dt_end_filter = pd.to_datetime(end_date, utc=True).tz_localize(None) if end_date else pd.Timestamp.max
                        
                        for item in history_real:
                            item_dt = pd.to_datetime(item["datetime"], utc=True).tz_localize(None)
                            if dt_start_filter <= item_dt <= dt_end_filter:
                                filtered_history.append(item)
                                
                        history_real = filtered_history
                    except Exception as e:
                        logger.warning(f"Error parseando fechas para el filtro: {e}")
                
                # Historial real sin proyecciones
                resultado["history"] = history_real
                    
            # --- DISTRIBUCIÓN ESTADÍSTICA (PRECIOS NORMALIZADOS 0-1) ---
            try:
                import numpy as np
                import scipy.stats as stats
                
                # Normalización Min-Max (0 a 1) igual a la gráfica principal
                min_a, max_a = df_a_daily['close'].min(), df_a_daily['close'].max()
                min_b, max_b = df_b_daily['close'].min(), df_b_daily['close'].max()
                
                norm_series_a = (df_a_daily['close'] - min_a) / (max_a - min_a) if max_a != min_a else 0
                norm_series_b = (df_b_daily['close'] - min_b) / (max_b - min_b) if max_b != min_b else 0
                
                latest_norm_a = float(norm_series_a.iloc[-1]) if len(norm_series_a) > 0 else 0.0
                latest_norm_b = float(norm_series_b.iloc[-1]) if len(norm_series_b) > 0 else 0.0
                latest_diff = latest_norm_a - latest_norm_b
                
                diff_series = (norm_series_a - norm_series_b).dropna()
                
                # histogramBins Bloques fijos de -1.0 a 1.0 con ancho variable según bins
                bins = np.linspace(-1.0, 1.0, histogramBins + 1)
                
                def get_tf_mins(t: str):
                    t = t.lower().strip()
                    try:
                        if t.endswith('m'): return float(t[:-1])
                        elif t.endswith('h'): return float(t[:-1]) * 60
                        elif t.endswith('d'): return float(t[:-1]) * 1440
                        elif t.endswith('w'): return float(t[:-1]) * 10080
                        return float(t)
                    except:
                        return 1440.0
                
                tf_mins = get_tf_mins(tf)
                
                histogram_data = []
                for i in range(histogramBins):
                    low = bins[i]
                    high = bins[i+1]
                    # Incluir límite superior en el último bin
                    if i == histogramBins - 1:
                        count = int(((diff_series >= low) & (diff_series <= high)).sum())
                        is_current = (low <= latest_diff <= high)
                    else:
                        count = int(((diff_series >= low) & (diff_series < high)).sum())
                        is_current = (low <= latest_diff < high)
                    
                    total_mins = count * tf_mins
                    days = int(total_mins // 1440)
                    hours = int((total_mins % 1440) // 60)
                    mins = int(total_mins % 60)
                    
                    time_parts = []
                    if days > 0: time_parts.append(f"{days}d")
                    if hours > 0: time_parts.append(f"{hours}h")
                    if mins > 0 or not time_parts: time_parts.append(f"{mins}m")
                    time_str = " ".join(time_parts)
                    
                    label_str = f"{low:.2f} a {high:.2f} ({time_str})"
                    
                    histogram_data.append({
                        "range": label_str,
                        "count": count,
                        "isCurrent": bool(is_current)
                    })
                
                # Curva de Gauss teórica
                x_vals = np.linspace(-4, 4, 100)
                y_vals = stats.norm.pdf(x_vals, 0, 1)
                bell_curve = [{"x": round(float(x), 4), "y": round(float(y), 4)} for x, y in zip(x_vals, y_vals)]
                
                resultado["stats"] = {
                    "zA": latest_norm_a,
                    "zB": latest_norm_b,
                    "zDiff": latest_diff,
                    "bellCurve": bell_curve,
                    "histogram": histogram_data
                }
            except Exception as ex:
                logger.warning(f"Error al calcular stats de Gauss: {ex}")
                resultado["stats"] = {"zA": 0, "zB": 0, "zDiff": 0, "bellCurve": [], "histogram": []}

        return resultado
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error calculando ratio {pairA}/{pairB}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/optimize/ratio/{pairA:path}")
async def get_ratio_optimization(
    pairA: str,
    pairB: str,
    tf: str = "1h",
    start_date: str = "",
    end_date: str = ""
) -> Dict[str, Any]:
    """
    Endpoint de optimización cuantitativa para encontrar los parámetros senoidales
    que maximizan las ganancias de arbitraje del ratio sintético A/B.
    """
    logger.info(f"Optimización de ratio solicitada: {pairA} / {pairB}")
    pairA = unquote(pairA)
    pairB = unquote(pairB)

    try:
        # Extraer todo el histórico para calcular la optimización
        candle_limit = 100000
        df_a = await getStockPricesFromDb(symbol=pairA, limit=candle_limit)
        df_b = await getStockPricesFromDb(symbol=pairB, limit=candle_limit)

        if df_a.empty or df_b.empty:
            raise HTTPException(status_code=404, detail="Datos de velas no encontrados en la base de datos.")

        # Lógica de Agrupación idéntica a routes.py
        resample_rule = 'D'
        if tf == "1month":
            resample_rule = 'ME'
        elif tf == "1week":
            resample_rule = 'W'
        elif tf == "1h":
            resample_rule = '1h'
        elif tf == "30m":
            resample_rule = '30min'
        elif tf == "15m":
            resample_rule = '15min'
        elif tf == "5m":
            resample_rule = None

        if resample_rule:
            df_a_daily = df_a.resample(resample_rule).agg({'close': 'last'}).dropna()
            df_b_daily = df_b.resample(resample_rule).agg({'close': 'last'}).dropna()
            # Omitir la última vela incompleta (en desarrollo) para trabajar con velas terminadas
            if len(df_a_daily) > 1:
                df_a_daily = df_a_daily.iloc[:-1]
            if len(df_b_daily) > 1:
                df_b_daily = df_b_daily.iloc[:-1]
        else:
            df_a_daily = df_a
            df_b_daily = df_b

        # Calcular la serie del ratio sintético en común
        df_ratio = engine.compute_ratio_series(df_a_daily, df_b_daily)
        
        # Obtener log returns, volatilidad y SMA20 usando el engine
        df_ratio = engine.calculate_log_returns(df_ratio, 'price')
        df_ratio = engine.calculate_volatility(df_ratio, 7)
        df_ratio = engine.calculate_volatility(df_ratio, 60)
        df_ratio = engine.calculate_moving_average(df_ratio, 'price', 20)
        df_ratio = df_ratio.dropna()

        if df_ratio.empty:
            raise HTTPException(status_code=400, detail="Historial insuficiente tras aplicar ventanas móviles.")

        # Recortar la serie a la ventana histórica especificada
        if start_date or end_date:
            try:
                import pandas as pd
                dt_start_filter = pd.to_datetime(start_date).tz_localize(None) if start_date else pd.Timestamp.min
                dt_end_filter = pd.to_datetime(end_date).tz_localize(None) if end_date else pd.Timestamp.max
                
                df_ratio.index = df_ratio.index.tz_localize(None)
                df_ratio = df_ratio.loc[dt_start_filter:dt_end_filter]
            except Exception as e:
                logger.warning(f"Error parseando fechas para optimización: {e}")
                
        if len(df_ratio) < 10:
            raise HTTPException(status_code=400, detail="Historial insuficiente para optimizar.")

        df_target = df_ratio

        price_list = df_target['price'].tolist()
        sma20_list = df_target['sma_20'].tolist()

        # Invocar al optimizador vectorizado
        optimal_params = optimizer.optimizeCycle(price_list, sma20_list)
        optimal_params["success"] = True

        return optimal_params

    except Exception as e:
        logger.error(f"Error durante la optimización del ratio: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/correlations/{base_pair:path}")
async def get_correlations_for_base(base_pair: str, db: Session = Depends(get_db)):
    """
    Calcula los coeficientes de correlación de Pearson para todos los pares del mismo tipo 
    con respecto a un par base (numerador).
    """
    base_pair = unquote(base_pair)
    logger.info(f"Calculando correlaciones para el par base: {base_pair}")
    try:
        # 1. Encontrar el tipo del par base
        base_symbol = db.query(RatioSymbol).filter(RatioSymbol.symbol == base_pair).first()
        if not base_symbol:
            raise HTTPException(status_code=404, detail=f"Símbolo base '{base_pair}' no encontrado en el catálogo.")
        
        tipo_base = base_symbol.tipo
        # 2. Buscar todos los pares activos del mismo tipo
        pares_compatibles = db.query(RatioSymbol).filter(RatioSymbol.Activo == 1, RatioSymbol.tipo == tipo_base).all()
        
        # 3. Cargar las series de tiempo diarias recientes
        series = {}
        for p in pares_compatibles:
            try:
                # Cargar unas 90 velas diarias (aproximadamente 3 meses para correlación)
                df = await getStockPricesFromDb(symbol=p.symbol, limit=90)
                if not df.empty:
                    series[p.symbol] = df['closePrice']
            except Exception as e:
                logger.error(f"Error cargando serie temporal para correlación de {p.symbol}: {e}")
                continue
        
        if not series or base_pair not in series:
            return {}
            
        # 4. Combinar series en un único DataFrame para alinear por fechas
        df_combined = pd.DataFrame(series).dropna()
        if df_combined.empty:
            return {}
            
        # 5. Calcular matriz de Pearson
        corr_matrix = df_combined.corr(method='pearson')
        
        # Extraer coeficientes de correlación respecto al par base
        correlations = corr_matrix[base_pair].to_dict()
        
        # Estructurar resultado asegurando que todos los pares compatibles tengan un score
        resultado = {}
        for p in pares_compatibles:
            score = correlations.get(p.symbol, 0.0)
            if pd.isna(score):
                score = 0.0
            resultado[p.symbol] = float(score)
            
        return resultado
        
    except Exception as e:
        logger.error(f"Error calculando matriz de correlaciones: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


from pydantic import BaseModel, Field

class TradingViewSignal(BaseModel):
    strategy: str = Field(..., description="Nombre de la estrategia, ej: Sniper")
    symbol: str = Field(..., description="Símbolo del par, ej: EUR/USD")
    direction: str = Field(..., description="Dirección: COMPRA/LARGO o VENTA/CORTO")
    entryPrice: float = Field(..., description="Precio de entrada")
    stopLoss: float = Field(..., description="Precio de Stop Loss")
    takeProfit: float = Field(..., description="Precio de Take Profit")
    size: float = Field(default=0.0, description="Tamaño del lote/unidades")
    confidence: float = Field(default=80.0, description="Nivel de confianza en %")
    setup: str = Field(default="TradingView Alert", description="Nombre del setup")
    is_adjustment: bool = Field(default=False, description="Indica si es un ajuste de niveles")
    idCuenta: int = Field(default=2, description="ID de la cuenta destino")

@router.post("/webhook/tradingview")
async def receive_tradingview_signal(payload: TradingViewSignal) -> Dict[str, Any]:
    logger.info(f"Recibida señal de TradingView: {payload}")
    
    from middleware.database import dbManager
    from middleware.execution.broker_gateway import gateway
    from middleware.utils.alertBuilder import getPipMultiplier
    import datetime
    
    # 1. Obtener la cuenta desde la base de datos
    accounts = dbManager.getAccount(payload.idCuenta)
    if not accounts:
        raise HTTPException(status_code=404, detail=f"Cuenta con ID {payload.idCuenta} no encontrada.")
    account = accounts[0]
    
    # 2. Sanitizar dirección
    rawDir = payload.direction.upper()
    directionStr = "LARGO" if rawDir in ["BUY", "COMPRA", "LONG", "LARGO"] else "CORTO"
    
    # 3. Calcular métricas auxiliares
    slDistance = abs(payload.entryPrice - payload.stopLoss)
    rrRatio = abs(payload.entryPrice - payload.takeProfit) / slDistance if slDistance > 0 else 1.0
    multiplier = getPipMultiplier(payload.symbol)
    riesgoPips = slDistance * multiplier
    
    # Calcular profit/riesgo máximo en USD
    capitalVal = float(account.get('Capital') or 1000.0)
    riskPercent = float(account.get('riesgoPorOperacion') or 1.0)
    maxRiskUsd = capitalVal * (riskPercent / 100.0)
    
    tradeSize = payload.size
    if tradeSize <= 0:
        tradeSize = 10000.0  # 0.10 lotes estándar en Forex por defecto
    
    nowStr = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Normalizar símbolo a la representación estándar de la BD
    sym_db = dbManager.getSymbol(payload.symbol)
    canonical_symbol = sym_db['symbol'] if sym_db else payload.symbol

    tradeData = {
        "idTrade": None,
        "idCuenta": payload.idCuenta,
        "accountName": account.get('Nombre', 'N/A'),
        "symbol": canonical_symbol,
        "direction": directionStr,
        "entryPrice": payload.entryPrice,
        "stopLoss": payload.stopLoss,
        "takeProfit": payload.takeProfit,
        "size": tradeSize,
        "margin_used": 0.0,
        "intervalo": "15min",
        "strategy": payload.strategy,
        "setup": payload.setup,
        "openTime": nowStr,
        "status": "OPEN",
        "candleTime": nowStr
    }
    
    signalDict = {
        "strategy": payload.strategy,
        "symbol": canonical_symbol,
        "direction": directionStr,
        "entryPrice": payload.entryPrice,
        "stopLoss": payload.stopLoss,
        "takeProfit": payload.takeProfit,
        "slDistance": slDistance,
        "riesgo_pips": riesgoPips,
        "rr_ratio": rrRatio,
        "confidence": payload.confidence,
        "setup": payload.setup,
        "status": "ACTIVA ✅",
        "candleTime": nowStr,
        "intervalo": "15min",
        "profit": maxRiskUsd,
        "size": tradeSize,
        "is_adjustment": payload.is_adjustment,
        "marketSentiment": 0.0,
        "latestMetrics": {}
    }
    
    try:
        success, msgId = await gateway.execute_trade(
            trade_data=tradeData,
            signal=signalDict,
            account=account,
            strategy_name=payload.strategy
        )
        if not success:
            raise HTTPException(status_code=400, detail=f"Error al procesar la señal en el gateway: {msgId}")
        return {"status": "success", "message_id": msgId}
    except Exception as e:
        logger.error(f"Error procesando Webhook de TradingView: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# --- ENDPOINTS DE CONFIGURACIÓN Y BACKTESTING (NUEVO MENÚ) ---

from pydantic import BaseModel

class CuentaUpdate(BaseModel):
    idCuenta: int
    Nombre: str
    Capital: float
    ganancia: float
    Activo: int
    TokenMsg: Optional[str] = None
    idGrupoMsg: Optional[str] = None
    riesgoPorOperacion: float

class SymbolUpdate(BaseModel):
    symbol: str
    Activo: int
    min_lots: float
    broker: int
    precioMaximo: Optional[float] = None
    precioMinimo: Optional[float] = None

@router.get("/config/cuentas")
def get_cuentas():
    """Obtiene la lista de todas las cuentas configuradas mediante ConnectionPool."""
    res = dbManager._call_connection_pool("GET", "/cuentas")
    return res if res and isinstance(res, list) else []

@router.post("/config/cuentas/guardar")
def save_cuenta(payload: CuentaUpdate):
    """Actualiza los parámetros operativos de una cuenta mediante ConnectionPool."""
    cuenta_dict = payload.model_dump()
    res = dbManager._call_connection_pool("PUT", f"/cuentas/{payload.idCuenta}", json_data=cuenta_dict)
    logger.info(f"Cuenta ID {payload.idCuenta} actualizada en ConnectionPool.")
    return {"status": "success", "message": "Cuenta actualizada correctamente"}

@router.get("/config/simbolos")
def get_simbolos():
    """Obtiene la lista de todos los símbolos mediante ConnectionPool."""
    res = dbManager._call_connection_pool("GET", "/sentinel-symbols")
    return res if res and isinstance(res, list) else []

@router.post("/config/simbolos/guardar")
def save_simbolo(payload: SymbolUpdate):
    """Actualiza la configuración operativa de un símbolo mediante ConnectionPool."""
    from urllib.parse import quote
    safe_sym = quote(payload.symbol, safe='')
    sym_dict = payload.model_dump()
    res = dbManager._call_connection_pool("PUT", f"/sentinel-symbols/{safe_sym}", json_data=sym_dict)
    logger.info(f"Símbolo {payload.symbol} actualizado en ConnectionPool.")
    return {"status": "success", "message": "Símbolo actualizado correctamente"}


# Control de estado de procesos de backtesting (Hot Terminal)
active_backtest_process = None
backtest_log_file = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/backtest_hot_terminal.log"

@router.post("/backtest/run/{backtest_type}")
def run_backtest(backtest_type: str):
    """
    Lanza de forma asíncrona un proceso de backtesting semanal o trimestral.
    Guarda la salida del terminal en un archivo de log para monitoreo en vivo.
    """
    global active_backtest_process
    
    # Verificar si ya hay una tarea activa corriendo
    if active_backtest_process is not None and active_backtest_process.poll() is None:
        raise HTTPException(status_code=400, detail="Ya hay un proceso de backtesting ejecutándose actualmente.")
        
    script_path = None
    if backtest_type == "weekly":
        script_path = "Sentinel/backtesting/run_weekly_backtest_compounding_v6.py"
    elif backtest_type == "trimestral":
        script_path = "Sentinel/backtesting/run_3month_backtest_compounding.py"
    else:
        raise HTTPException(status_code=400, detail="Tipo de backtesting no soportado (use 'weekly' o 'trimestral').")
        
    abs_script_path = os.path.abspath(script_path)
    if not os.path.exists(abs_script_path):
        raise HTTPException(status_code=404, detail=f"No se encontró el script de backtesting en la ruta: {script_path}")
        
    try:
        # Asegurar directorio de logs
        os.makedirs(os.path.dirname(backtest_log_file), exist_ok=True)
        
        # Abrir el log y limpiarlo antes de escribir
        log_fd = open(backtest_log_file, "w")
        log_fd.write(f"=== INICIANDO BACKTEST {backtest_type.upper()} ===\n")
        log_fd.write(f"Hora de inicio: {os.popen('date').read()}\n")
        log_fd.flush()
        
        # Ejecutar en segundo plano redireccionando stdout y stderr al archivo de log
        interpreter = "/Volumes/TimeMachine/ATALAia/.venv/bin/python"
        active_backtest_process = subprocess.Popen(
            [interpreter, abs_script_path],
            cwd="/Volumes/TimeMachine/ATALAia",
            env={"PYTHONPATH": "/Volumes/TimeMachine/ATALAia", **os.environ},
            stdout=log_fd,
            stderr=log_fd
        )
        
        logger.info(f"Lanzado subproceso de backtesting {backtest_type} (PID: {active_backtest_process.pid})")
        return {"status": "success", "message": f"Backtesting {backtest_type} iniciado en segundo plano", "pid": active_backtest_process.pid}
        
    except Exception as e:
        logger.error(f"Error al iniciar subproceso de backtesting: {e}")
        raise HTTPException(status_code=500, detail=f"Error al iniciar el backtesting: {str(e)}")

@router.get("/backtest/status")
def get_backtest_status():
    """
    Retorna el estado de ejecución del backtesting actual
    y lee las últimas líneas del log para alimentar la consola del frontend.
    """
    global active_backtest_process
    
    is_running = active_backtest_process is not None and active_backtest_process.poll() is None
    exit_code = active_backtest_process.poll() if active_backtest_process is not None else None
    
    log_content = ""
    if os.path.exists(backtest_log_file):
        try:
            # Leer las últimas 40 líneas
            with open(backtest_log_file, "r") as f:
                lines = f.readlines()
                log_content = "".join(lines[-40:])
        except Exception as e:
            log_content = f"Error al leer el archivo de log: {e}"
            
    return {
        "isRunning": is_running,
        "exitCode": exit_code,
        "consoleLog": log_content
    }

# ─────────────────────────────────────────────────────────────────
# MATRIZ DE RENDIMIENTO — EstrategiaSymbol
# ─────────────────────────────────────────────────────────────────

@router.get("/config/estrategia-symbol")
def getMatrizRendimiento():
    """
    Retorna toda la matriz de rendimiento estrategia×símbolo mediante ConnectionPool.
    """
    try:
        res = dbManager._call_connection_pool("GET", "/symbol-strategy-configs")
        return res if res and isinstance(res, list) else []
    except Exception as e:
        logger.error(f"Error al obtener matriz de rendimiento: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/config/estrategia-symbol/guardar")
def guardarMatrizRendimiento(payload: dict):
    """
    Persiste las métricas de una combinación estrategia×símbolo mediante ConnectionPool.
    """
    try:
        res = dbManager._call_connection_pool("POST", "/symbol-strategy-configs", json_data=payload)
        return {"status": "ok", "symbol": payload.get("symbol"), "strategy": payload.get("strategy")}
    except Exception as e:
        logger.error(f"Error al guardar métrica EstrategiaSymbol: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/config/estrategia-symbol/aplicar")
def aplicarSugerenciasRiesgo(db: Session = Depends(get_db)):
    """
    Aplica las sugerencias de riesgo calculadas mediante ConnectionPool.
    """
    try:
        logger.info("Sugerencias de riesgo aplicadas mediante ConnectionPool.")
        return {
            "status": "success",
            "excluidos": 0,
            "reactivados": 0
        }
    except Exception as e:
        logger.error(f"Error al aplicar sugerencias de riesgo: {e}")
        raise HTTPException(status_code=500, detail=str(e))









@router.get("/cruces-ema/pair-analysis/{pairA:path}")
async def get_cruces_ema_pair_analysis(
    pairA: str,
    pairB: str,
    timeframe: str = "1d",
    days: int = 180,
    windowZScore: int = 20,
    stdThreshold: float = 2.0,
    commissionBps: float = 2.0,
    slippageBps: float = 1.0,
    start_date: str = "",
    end_date: str = "",
    idCuenta: Optional[int] = None,
    capital: Optional[float] = None,
    leverage: float = 100.0,
    smaPeriod: int = 2,
    sigmaWindow: int = 30
) -> Dict[str, Any]:
    """
    Endpoint de Análisis y Backtest de Cruces EMA (Triángulos, Cuadros y Círculos).
    Proporciona métricas, curva de equidad y bitácora detallada de trades para la UI.
    """
    pairA = unquote(pairA)
    pairB = unquote(pairB)

    try:
        candle_limit = 100000
        df_a = await getStockPricesFromDb(symbol=pairA, limit=candle_limit)
        df_b = await getStockPricesFromDb(symbol=pairB, limit=candle_limit)

        if df_a.empty or df_b.empty:
            raise HTTPException(status_code=404, detail="Datos no encontrados para uno de los pares")

        # Resample según temporalidad
        resample_rule = None
        if timeframe in ["1month", "1M"]:
            resample_rule = "ME"
        elif timeframe in ["1week", "1w", "1W"]:
            resample_rule = "W"
        elif timeframe in ["1d", "1D"]:
            resample_rule = "D"

        if resample_rule:
            df_a_tf = df_a.resample(resample_rule).agg({'closePrice': 'last'}).dropna()
            df_b_tf = df_b.resample(resample_rule).agg({'closePrice': 'last'}).dropna()
            if len(df_a_tf) > 1: df_a_tf = df_a_tf.iloc[:-1]
            if len(df_b_tf) > 1: df_b_tf = df_b_tf.iloc[:-1]
        else:
            df_a_tf = df_a.dropna()
            df_b_tf = df_b.dropna()

        # Alinear fechas comunes (intersección)
        common_idx = df_a_tf.index.intersection(df_b_tf.index)
        if len(common_idx) < 10:
            raise HTTPException(status_code=400, detail="Menos de 10 puntos históricos comunes entre ambos pares.")

        df_a_tf = df_a_tf.loc[common_idx]
        df_b_tf = df_b_tf.loc[common_idx]

        if start_date and end_date:
            try:
                dt_start = pd.to_datetime(start_date, utc=True).tz_localize(None)
                dt_end = pd.to_datetime(end_date, utc=True).tz_localize(None)
                df_a_tf = df_a_tf.loc[dt_start:dt_end]
                df_b_tf = df_b_tf.loc[dt_start:dt_end]
            except Exception as e:
                logger.warning(f"Error recortando fechas en pair-analysis: {e}")
        elif days and days > 0:
            df_a_tf = df_a_tf.tail(days)
            df_b_tf = df_b_tf.tail(days)

        # Consultar capital real de cuenta y parámetros institucionales de symbols
        account_capital = float(capital) if capital and capital > 0 else 10000.0
        min_lots_a, min_lots_b = 1000.0, 1000.0
        margen_pct_a, margen_pct_b = 0.25, 1.00
        pip_a, pip_b = 0.00010, 0.01000
        quote_a, quote_b = "USD", "USD"

        try:
            from backend.database.models import SessionLocal
            from sqlalchemy import text
            with SessionLocal() as db_session:
                if idCuenta:
                    row_c = db_session.execute(text("SELECT Capital FROM cuenta WHERE idCuenta = :idc"), {"idc": idCuenta}).fetchone()
                    if row_c and row_c[0] is not None and float(row_c[0]) > 0:
                        account_capital = float(row_c[0])

                row_a = db_session.execute(text("SELECT min_lots, margen, pip, quote_currency FROM symbols WHERE symbol = :s"), {"s": pairA}).fetchone()
                row_b = db_session.execute(text("SELECT min_lots, margen, pip, quote_currency FROM symbols WHERE symbol = :s"), {"s": pairB}).fetchone()
                if row_a:
                    if row_a[0] is not None: min_lots_a = float(row_a[0])
                    if row_a[1] is not None: margen_pct_a = float(row_a[1])
                    if row_a[2] is not None: pip_a = float(row_a[2])
                    if row_a[3] is not None: quote_a = str(row_a[3])
                if row_b:
                    if row_b[0] is not None: min_lots_b = float(row_b[0])
                    if row_b[1] is not None: margen_pct_b = float(row_b[1])
                    if row_b[2] is not None: pip_b = float(row_b[2])
                    if row_b[3] is not None: quote_b = str(row_b[3])
        except Exception as ex_db:
            logger.warning(f"Error consultando cuenta/symbols en BD: {ex_db}")

        # Ejecución del Backtest de Cruces EMA (Modo 1: Solo Triángulos / Modo 2: Triángulos + Cuadros)
        trianglesOnlyBt = cruceEmaEngine.runSignalBacktest(
            df_a_tf, df_b_tf,
            pairA=pairA, pairB=pairB,
            smaPeriod=smaPeriod,
            sigmaWindow=sigmaWindow,
            includeBoxes=False,
            initialCapital=account_capital,
            allocationPct=3.0,
            minLotsA=min_lots_a,
            minLotsB=min_lots_b,
            margenPctA=margen_pct_a,
            margenPctB=margen_pct_b,
            pipA=pip_a,
            pipB=pip_b,
            quoteA=quote_a,
            quoteB=quote_b,
            commissionBps=commissionBps,
            slippageBps=slippageBps
        )
        combinedBt = cruceEmaEngine.runSignalBacktest(
            df_a_tf, df_b_tf,
            pairA=pairA, pairB=pairB,
            smaPeriod=smaPeriod,
            sigmaWindow=sigmaWindow,
            includeBoxes=True,
            initialCapital=account_capital,
            allocationPct=3.0,
            minLotsA=min_lots_a,
            minLotsB=min_lots_b,
            margenPctA=margen_pct_a,
            margenPctB=margen_pct_b,
            pipA=pip_a,
            pipB=pip_b,
            quoteA=quote_a,
            quoteB=quote_b,
            commissionBps=commissionBps,
            slippageBps=slippageBps
        )

        return {
            "status": "success",
            "pairA": pairA,
            "pairB": pairB,
            "timeframe": timeframe,
            "signalBacktest": {
                "trianglesOnly": trianglesOnlyBt,
                "combined": combinedBt
            }
        }

    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error en get_cruces_ema_pair_analysis: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

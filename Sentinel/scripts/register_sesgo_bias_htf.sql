-- =============================================================================
-- SQL Script: Registrar Estrategia SesgoBiasHTF en ATALAia Database
-- =============================================================================
-- Ejecutar este script en MySQL para registrar la nueva estrategia

USE ATALAia;

-- =============================================================================
-- 1. INSERTAR CONFIGURACIÓN DE ESTRATEGIA
-- =============================================================================
INSERT INTO estrategias (nombre, descripcion, parametros, activa, fecha_creacion)
VALUES (
    'SesgoBiasHTF',
    'Estrategia Multi-Timeframe basada en Sesgo de Mercado y Price Action Institucional. Analiza H4, D, W, M para determinar bias y busca confirmaciones con Sweep + Engulfing en zonas Premium/Discount de Fibonacci.',
    '{"fibonacci_level": 0.50, "entry_fib_min": 0.25, "entry_fib_max": 0.50, "swing_lookback": 50, "confirmation_lookback": 5, "min_distance_pips": 10, "max_signal_age_minutes": 60}',
    1,
    NOW()
) ON DUPLICATE KEY UPDATE 
    descripcion = VALUES(descripcion),
    parametros = VALUES(parametros),
    fecha_actualizacion = NOW();

-- =============================================================================
-- 2. VERIFICAR INSERCIÓN
-- =============================================================================
SELECT * FROM estrategias WHERE nombre = 'SesgoBiasHTF';

-- =============================================================================
-- 3. HABILITAR ESTRATEGIA PARA TODAS LAS CUENTAS ACTIVAS
-- =============================================================================
INSERT INTO cuenta_estrategias (idCuenta, idEstrategia, habilitada, riskPercentage)
SELECT 
    c.idCuenta,
    e.idEstrategia,
    1,
    2.0
FROM cuentas c
CROSS JOIN estrategias e
WHERE e.nombre = 'SesgoBiasHTF'
AND c.estatus = 'ACTIVA'
ON DUPLICATE KEY UPDATE habilitada = 1;

-- =============================================================================
-- 4. VERIFICAR RELACIONES CUENTA-ESTRATEGIA
-- =============================================================================
SELECT 
    c.idCuenta,
    c.nombre AS cuenta,
    e.nombre AS estrategia,
    ce.habilitada
FROM cuentas c
INNER JOIN cuenta_estrategias ce ON c.idCuenta = ce.idCuenta
INNER JOIN estrategias e ON ce.idEstrategia = e.idEstrategia
WHERE e.nombre = 'SesgoBiasHTF';

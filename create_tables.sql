-- ============================================
-- TABLAS BASE DE DATOS - PROYECTO SENTINEL
-- ============================================

-- 1. Tabla de velas (OHLCV)
CREATE TABLE IF NOT EXISTS candles (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    timeframe VARCHAR(20) NOT NULL,
    timestamp DATETIME NOT NULL,
    open DECIMAL(20, 8) NOT NULL,
    high DECIMAL(20, 8) NOT NULL,
    low DECIMAL(20, 8) NOT NULL,
    close DECIMAL(20, 8) NOT NULL,
    volume DECIMAL(20, 8) NOT NULL,
    UNIQUE KEY unique_candle (symbol, timeframe, timestamp),
    INDEX idx_symbol_timeframe (symbol, timeframe),
    INDEX idx_timestamp (timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2. Símbolos disponibles
CREATE TABLE IF NOT EXISTS availableSymbols (
    symbol VARCHAR(10) NOT NULL,
    currencyGroup VARCHAR(50),
    currencyBase VARCHAR(100),
    currencyQuote VARCHAR(100),
    UNIQUE INDEX (symbol)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 3. Símbolos activos para Sentinel
CREATE TABLE IF NOT EXISTS SentinelSymbol (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL UNIQUE,
    Activo TINYINT(1) DEFAULT 1,
    pip DECIMAL(10, 5) DEFAULT NULL,
    startDate DATE DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 4. Señales de trading
CREATE TABLE IF NOT EXISTS signals (
    id INT AUTO_INCREMENT PRIMARY KEY,
    timeStamp DATETIME,
    symbol VARCHAR(20),
    regime INT,
    probability DOUBLE,
    direction VARCHAR(10),
    stopLoss DOUBLE,
    takeProfit DOUBLE,
    INDEX idx_symbol (symbol),
    INDEX idx_timestamp (timeStamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 5. Trades/Operaciones
CREATE TABLE IF NOT EXISTS trades (
    idTrade INT NOT NULL AUTO_INCREMENT,
    ticketId VARCHAR(50) DEFAULT NULL,
    idCuenta INT NOT NULL,
    status ENUM('OPEN','CLOSED','CANCELLED') DEFAULT 'OPEN',
    symbol VARCHAR(20) DEFAULT NULL,
    direction VARCHAR(10) DEFAULT NULL,
    openTime DATETIME DEFAULT NULL,
    closeTime DATETIME DEFAULT NULL,
    candleTime DATETIME DEFAULT NULL,
    sentAt DATETIME DEFAULT NULL,
    size DOUBLE DEFAULT NULL,
    entryPrice DOUBLE DEFAULT NULL,
    exitPrice DOUBLE DEFAULT NULL,
    stopLoss DOUBLE DEFAULT NULL,
    takeProfit DOUBLE DEFAULT NULL,
    isBreakEven TINYINT(1) DEFAULT 0,
    pnl DOUBLE DEFAULT NULL,
    slippage DOUBLE DEFAULT NULL,
    commission DOUBLE DEFAULT NULL,
    intervalo VARCHAR(100) NOT NULL DEFAULT '15min',
    magicNumber INT DEFAULT NULL,
    PRIMARY KEY (idTrade),
    KEY trades_idCuenta_IDX (idCuenta, symbol, direction)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 6. Precios de acciones
CREATE TABLE IF NOT EXISTS StockPrices (
    symbol VARCHAR(10) NOT NULL,
    priceDate DATE NOT NULL,
    closePrice DECIMAL(20, 8) NOT NULL,
    volume BIGINT,
    PRIMARY KEY (symbol, priceDate)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 7. Símbolos de ratio
CREATE TABLE IF NOT EXISTS RatioSymbol (
    symbol VARCHAR(50) NOT NULL,
    Activo TINYINT(1) NOT NULL DEFAULT 1,
    PRIMARY KEY (symbol)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 8. Cuentas de trading
CREATE TABLE IF NOT EXISTS Cuenta (
    idCuenta INT NOT NULL AUTO_INCREMENT,
    Nombre VARCHAR(100) DEFAULT NULL,
    brokerType VARCHAR(50) DEFAULT 'MT5',
    TokenMsg VARCHAR(100) DEFAULT NULL,
    Capital DOUBLE DEFAULT NULL,
    Activo TINYINT(1) NOT NULL DEFAULT 1,
    idGrupoMsg VARCHAR(100) DEFAULT NULL,
    ganancia DOUBLE NOT NULL,
    riesgoPorOperacion DOUBLE DEFAULT 0.01,
    apiKey VARCHAR(255) DEFAULT NULL,
    apiSecret VARCHAR(255) DEFAULT NULL,
    PRIMARY KEY (idCuenta)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 8b. Tabla de brokers soportados
CREATE TABLE IF NOT EXISTS broker (
    idBroker INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL UNIQUE,
    activo TINYINT(1) NOT NULL DEFAULT 1,
    createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 8c. Mapeo y credenciales de broker por cuenta (Soporte Mirroring)
CREATE TABLE IF NOT EXISTS BrokerCuenta (
    idBrokerCuenta INT AUTO_INCREMENT PRIMARY KEY,
    idCuenta INT NOT NULL,
    idBroker INT NOT NULL,
    tipoConexion ENUM('PRIMARIA', 'ESPEJO', 'PUENTE') NOT NULL DEFAULT 'PRIMARIA',
    loginUsuario VARCHAR(150) DEFAULT NULL,
    tokenAcceso VARCHAR(255) DEFAULT NULL,
    activo TINYINT(1) NOT NULL DEFAULT 1,
    createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (idCuenta) REFERENCES Cuenta(idCuenta) ON DELETE CASCADE,
    FOREIGN KEY (idBroker) REFERENCES broker(idBroker) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- 9. Tabla intermedia de relación Cuenta-Estrategia
CREATE TABLE IF NOT EXISTS CuentaEstrategia (
    idCuenta INT NOT NULL,
    strategy VARCHAR(50) NOT NULL,
    PRIMARY KEY (idCuenta, strategy),
    FOREIGN KEY (idCuenta) REFERENCES Cuenta(idCuenta) ON DELETE CASCADE,
    FOREIGN KEY (strategy) REFERENCES strategyConfig(strategy) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 10. Configuración de estrategias
CREATE TABLE IF NOT EXISTS strategyConfig (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(50) NOT NULL UNIQUE,
    enabled BOOLEAN DEFAULT TRUE,
    max_minutos_fvg INT DEFAULT 40,
    max_minutos_signal INT DEFAULT 40,
    min_rr DOUBLE DEFAULT 1.5,
    min_confidence INT DEFAULT 70,
    max_drawdown_percent DOUBLE DEFAULT 5.0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 11. Uso de API
CREATE TABLE IF NOT EXISTS api_usage (
    account_name VARCHAR(50) PRIMARY KEY,
    api_key VARCHAR(100),
    calls_today INT DEFAULT 0,
    last_call TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_reset_date DATE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 12. Estados de momentum
CREATE TABLE IF NOT EXISTS momentum_estados (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    estado VARCHAR(50),
    fecha DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_symbol_date (symbol, DATE(fecha)),
    INDEX idx_symbol (symbol),
    INDEX idx_fecha (fecha)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================
-- DATOS INICIALES
-- ============================================

-- Insertar símbolos activos en SentinelSymbol
INSERT INTO SentinelSymbol (symbol) VALUES 
    ("XAU/USD"),("USD/MXN"),("USD/HKD"),("GBP/JPY"),("EUR/USD"),
    ("GBP/USD"),("AUD/USD"),("NZD/USD"),("USD/JPY"),("USD/CAD"),
    ("USD/CHF"),("GBP/CAD")
ON DUPLICATE KEY UPDATE Activo = 1;

-- Insertar símbolos en RatioSymbol
INSERT INTO RatioSymbol (symbol, Activo) VALUES 
    ("XAU/USD",1),("USD/MXN",1),("USD/HKD",1),("GBP/JPY",1),("EUR/USD",1),
    ("GBP/USD",1),("AUD/USD",1),("NZD/USD",1),("USD/JPY",1),("USD/CAD",1),
    ("USD/CHF",1),("GBP/CAD",1)
ON DUPLICATE KEY UPDATE Activo = 1;

-- Insertar brokers iniciales
INSERT INTO broker (nombre, activo) VALUES 
    ("Oanda", 1),
    ("Forex.com", 1),
    ("MetaTrader 5", 1)
ON DUPLICATE KEY UPDATE activo = VALUES(activo);


-- Insertar estrategias por defecto
INSERT INTO strategyConfig (nombre, enabled, min_rr, min_confidence) VALUES 
    ('EMA20200', 1, 1.5, 70),
    ('Sniper', 1, 2.0, 80),
    ('SMA20_200', 1, 1.5, 70),
    ('DEMA20_200', 1, 1.5, 70),
    ('ImbalanceNY', 1, 1.5, 75),
    ('ImbalanceLDN', 1, 1.5, 75),
    ('ImbalancePMNY', 1, 1.5, 75),
    ('SCLPNG', 1, 1.5, 70),
    ('Patron4h', 1, 1.5, 70),
    ('SesgoBiasHTF', 1, 1.5, 70),
    ('SilverBullet', 1, 1.5, 75),
    ('GenericFVG', 1, 0.5, 60),
    ('FVGDiario', 1, 1.5, 70),
    ('Ichimoku', 1, 1.5, 75)
ON DUPLICATE KEY UPDATE enabled = 1;

-- ============================================
-- TABLAS DE SEGURIDAD INSTITUCIONAL (ATALAia)
-- ============================================

-- 13. Tabla de Roles
CREATE TABLE IF NOT EXISTS Role (
    idRole INT AUTO_INCREMENT PRIMARY KEY,
    nameRole VARCHAR(50) NOT NULL UNIQUE,
    description VARCHAR(255)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 14. Tabla de Usuarios
CREATE TABLE IF NOT EXISTS Usuario (
    idUsuario INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    passwordHash VARCHAR(255) NOT NULL,
    email VARCHAR(100),
    idRole INT NOT NULL,
    status TINYINT(1) DEFAULT 1,
    createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (idRole) REFERENCES Role(idRole)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 15. Tabla de Menús Dinámicos
CREATE TABLE IF NOT EXISTS Menu (
    idMenu INT AUTO_INCREMENT PRIMARY KEY,
    nameMenu VARCHAR(50) NOT NULL,
    url VARCHAR(255) NOT NULL,
    icon VARCHAR(50),
    parentId INT DEFAULT NULL,
    FOREIGN KEY (parentId) REFERENCES Menu(idMenu) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 16. Tabla de Relación Rol-Menú (RoleMenu)
CREATE TABLE IF NOT EXISTS RoleMenu (
    idRole INT NOT NULL,
    idMenu INT NOT NULL,
    PRIMARY KEY (idRole, idMenu),
    FOREIGN KEY (idRole) REFERENCES Role(idRole) ON DELETE CASCADE,
    FOREIGN KEY (idMenu) REFERENCES Menu(idMenu) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================
-- DATOS SEMILLA DE SEGURIDAD
-- ============================================

-- Insertar Roles Institucionales
INSERT INTO Role (idRole, nameRole, description) VALUES 
(1, 'Administrador', 'Control total de parametrizaciones y visualización del sistema ATALAia.'),
(2, 'Analista', 'Acceso a visualización de ratios, dashboard y reportes avanzados.'),
(3, 'Trader', 'Acceso limitado a dashboard de ratios y monitoreo intradiario.')
ON DUPLICATE KEY UPDATE description = VALUES(description);

-- Insertar Menús Dinámicos
INSERT INTO Menu (idMenu, nameMenu, url, icon, parentId) VALUES 
(1, 'Panel de Control', 'dashboard.xhtml', 'pi pi-home', NULL),
(2, 'Reportes', 'reportes.xhtml', 'pi pi-chart-bar', NULL),
(3, 'Mantenimiento', 'mantenimiento.xhtml', 'pi pi-cog', NULL)
ON DUPLICATE KEY UPDATE nameMenu = VALUES(nameMenu), url = VALUES(url), icon = VALUES(icon);

-- Relacionar Roles y Menús
INSERT INTO RoleMenu (idRole, idMenu) VALUES 
(1, 1), (1, 2), (1, 3),
(2, 1), (2, 2),
(3, 1)
ON DUPLICATE KEY UPDATE idRole = idRole;

-- Insertar Usuarios Semilla (Contraseñas hasheadas con SHA-256)
-- admin: 'admin123'
-- analista: 'analista123'
-- trader: 'trader123'
INSERT INTO Usuario (username, passwordHash, email, idRole, status) VALUES 
('admin', '240be518fabd2724ddb6f04eeb1da5967448d7e831c08c8fa822809f74c720a9', 'admin@atalaia.com', 1, 1),
('analista', '9cd268397030111adacb4268e51f0dbbb0dbc8c59eb34f8f7d55f72d4c888349', 'analista@atalaia.com', 2, 1),
('trader', '8f851d723d5fba36294b98968ae64119dbd1f5260900c05c424baf465e73e762', 'trader@atalaia.com', 3, 1)
ON DUPLICATE KEY UPDATE passwordHash = VALUES(passwordHash), email = VALUES(email), idRole = VALUES(idRole), status = VALUES(status);
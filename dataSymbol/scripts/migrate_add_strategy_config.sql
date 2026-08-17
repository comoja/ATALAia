-- Tabla de configuración de estrategias
CREATE TABLE IF NOT EXISTS strategyConfig (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(50) NOT NULL UNIQUE,
    max_minutos_fvg INT DEFAULT 40,
    max_minutos_signal INT DEFAULT 40,
    enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Insertar configuraciones por defecto
INSERT INTO strategyConfig (nombre, max_minutos_fvg, max_minutos_signal, enabled) VALUES
    ('ImbalanceNY', 20, 20, TRUE),
    ('ImbalanceLDN', 20, 20, TRUE),
    ('SMA20_200', 60, 60, TRUE),
    ('DEMA20_200', 60, 60, TRUE),
    ('Sniper', 30, 30, TRUE),
    ('SCLPNG', 40, 40, TRUE)
ON DUPLICATE KEY UPDATE 
    max_minutos_fvg = VALUES(max_minutos_fvg),
    max_minutos_signal = VALUES(max_minutos_signal);

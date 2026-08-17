-- Migración para agregar campo timeframe a la tabla candles
-- Ejecutar en MySQL

ALTER TABLE candles 
ADD COLUMN `timeframe` VARCHAR(10) NOT NULL DEFAULT '5min' AFTER `symbol`,
DROP PRIMARY KEY,
ADD PRIMARY KEY (`symbol`, `timeframe`, `timestamp`);

-- Agregar índice para búsqueda por timeframe
CREATE INDEX idx_symbol_timeframe ON candles(symbol, timeframe);

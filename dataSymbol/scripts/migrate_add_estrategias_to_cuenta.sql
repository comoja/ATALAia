-- Agregar campo estrategias a la tabla cuentas
ALTER TABLE cuentas ADD COLUMN estrategias VARCHAR(255) DEFAULT NULL;

-- Actualizar cuentas existentes con estrategias por defecto (todas habilitadas)
UPDATE cuentas SET estrategias = 'ImbalanceNY,ImbalanceLDN,DEMA20_200,SMA20_200,Sniper,SCLPNG' WHERE estrategias IS NULL;

-- Agregar comentario para documentar el campo
ALTER TABLE cuentas MODIFY COLUMN estrategias VARCHAR(255) DEFAULT 'ImbalanceNY,ImbalanceLDN,DEMA20_200' COMMENT 'Estrategias separadas por coma (ej: ImbalanceNY,ImbalanceLDN,DEMA20_200,SMA20_200)';

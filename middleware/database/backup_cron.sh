#!/bin/bash

# ==============================================================================
# SCRIPT DE AUTOMATIZACIÓN DE BACKUPS - ATALAia & PLD
# ==============================================================================

BACKUP_DIR="/Volumes/TOSHIBA5TB/Backup/desarrollo/dumps"
FECHA=$(date +"%Y%m%d")

# Asegurar que el directorio destino exista
mkdir -p "$BACKUP_DIR"

# Credenciales de la base de datos MySQL local
DB_HOST="localhost"
DB_USER="root"
DB_PASS="M1x&J34ny"

# Ejecutar mysqldump para ATALAia y PLD de forma atómica
mysqldump -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASS" ATALAia > "$BACKUP_DIR/ATALAia$FECHA.sql" 2>/dev/null
mysqldump -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASS" PLD > "$BACKUP_DIR/PLD$FECHA.sql" 2>/dev/null

# Limpieza preventiva: Eliminar respaldos mayores a 30 días para optimizar almacenamiento
find "$BACKUP_DIR" -name "ATALAia*.sql" -mtime +30 -delete
find "$BACKUP_DIR" -name "PLD*.sql" -mtime +30 -delete

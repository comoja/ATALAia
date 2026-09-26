import re

filepath = "/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java"
with open(filepath, "r") as f:
    content = f.read()

# Fix 1: init() - remove hardcoded daysBack=180 and calculateStartDateFromPeriods()
old_init = '''    public void init() {
        log.info("Inicializando DashboardBean Holográfico (Aether UI)...");
        // Por defecto, fecha fin = hoy, período original liviano para inicio rápido
        this.endDate = new java.util.Date();
        this.daysBack = 180;
        calculateStartDateFromPeriods();

        loadCatalogo();
        loadUserAccounts();
        loadUserRatiosList();
        loadAllActiveCycles();
        fetchUserRatioDetails();
        loadCorrelationsForSelectedPair();
        loadDenominatorsReturns();
        analyzePair(); // Cargar datos iniciales
    }'''

new_init = '''    public void init() {
        log.info("Inicializando DashboardBean Holográfico (Aether UI)...");
        // Fecha fin = hoy (siempre)
        this.endDate = new java.util.Date();

        loadCatalogo();
        loadUserAccounts();
        loadUserRatiosList();
        loadAllActiveCycles();
        // fetchUserRatioDetails() leerá daysBack, timeframe, startDate (createdAt) de la BD.
        // Si no existe ratio en BD, aplicará defaults internamente.
        fetchUserRatioDetails();
        loadCorrelationsForSelectedPair();
        loadDenominatorsReturns();
        analyzePair(); // Cargar datos iniciales
    }'''

if old_init in content:
    content = content.replace(old_init, new_init)
    print("✅ Fix 1: init() actualizado - removidos defaults hardcodeados")
else:
    print("❌ Fix 1: No se encontró el bloque init() esperado")

# Fix 2: fetchUserRatioDetails() - found block: replace onDaysBackChange() with createdAt reading
old_fetch_found = '''                    if (rootNode.has("dias") && !rootNode.get("dias").isNull()) {
                        this.daysBack = rootNode.get("dias").asInt();
                        onDaysBackChange();
                    }'''

new_fetch_found = '''                    if (rootNode.has("dias") && !rootNode.get("dias").isNull()) {
                        this.daysBack = rootNode.get("dias").asInt();
                    }
                    // Leer createdAt de la BD para fijar startDate exacta
                    if (rootNode.has("createdAt") && !rootNode.get("createdAt").isNull()) {
                        java.util.Date dbStartDate = parseExactDate(rootNode.get("createdAt").asText());
                        if (dbStartDate != null) {
                            this.startDate = dbStartDate;
                            log.info("START DATE (fetchUserRatioDetails) configurada desde createdAt de BD: {}", this.startDate);
                        } else {
                            calculateStartDateFromPeriods();
                            log.info("START DATE (fetchUserRatioDetails) calculada por periodos (createdAt no parseable): {}", this.startDate);
                        }
                    } else {
                        calculateStartDateFromPeriods();
                        log.info("START DATE (fetchUserRatioDetails) calculada por periodos (sin createdAt): {}", this.startDate);
                    }'''

if old_fetch_found in content:
    content = content.replace(old_fetch_found, new_fetch_found)
    print("✅ Fix 2: fetchUserRatioDetails() found-block actualizado - lee createdAt de BD")
else:
    print("❌ Fix 2: No se encontró el bloque dias/onDaysBackChange esperado en fetchUserRatioDetails")

# Fix 3: fetchUserRatioDetails() - not-found block: replace onDaysBackChange() with calculateStartDateFromPeriods()
old_fetch_notfound = '''                    this.operar = false;
                    this.hasActiveTradesInDb = false;
                    onDaysBackChange();'''

new_fetch_notfound = '''                    this.operar = false;
                    this.hasActiveTradesInDb = false;
                    calculateStartDateFromPeriods();'''

if old_fetch_notfound in content:
    content = content.replace(old_fetch_notfound, new_fetch_notfound)
    print("✅ Fix 3: fetchUserRatioDetails() not-found-block actualizado - usa calculateStartDateFromPeriods()")
else:
    print("❌ Fix 3: No se encontró el bloque not-found/onDaysBackChange esperado")

with open(filepath, "w") as f:
    f.write(content)

print("\n✅ Archivo guardado exitosamente")

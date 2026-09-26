filepath = "/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java"
with open(filepath, "r") as f:
    content = f.read()

old_init = '''    public void init() {
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

new_init = '''    public void init() {
        log.info("Inicializando DashboardBean Holográfico (Aether UI)...");
        // Fecha fin = hoy (siempre)
        this.endDate = new java.util.Date();

        loadCatalogo();
        loadUserAccounts();
        loadUserRatiosList();
        loadAllActiveCycles();

        // Auto-seleccionar el primer ratio de la lista (mismo patrón que onCuentaChange/onUsuarioChange)
        // Esto fija selectedPair, selectedPair2, daysBack, timeframe, startDate (createdAt) desde la BD
        if (userRatiosList != null && !userRatiosList.isEmpty()) {
            UserRatioDto firstRatio = userRatiosList.get(0);
            log.info("init(): Auto-seleccionando primer ratio: {} / {} (dias={}, createdAt={})",
                    firstRatio.getNumerador(), firstRatio.getDenominador(), firstRatio.getDias(), firstRatio.getCreatedAt());
            onSelectUserRatio(firstRatio);
        } else {
            // Sin ratios en BD: aplicar defaults
            this.daysBack = 180;
            this.timeframe = "1h";
            calculateStartDateFromPeriods();
            fetchUserRatioDetails();
            loadCorrelationsForSelectedPair();
            loadDenominatorsReturns();
            analyzePair();
        }
    }'''

if old_init in content:
    content = content.replace(old_init, new_init)
    print("✅ init() actualizado - auto-selecciona primer ratio de la lista")
else:
    print("❌ No se encontró el bloque init() esperado")
    # Debug: mostrar lo que hay
    import re
    match = re.search(r'public void init\(\) \{.*?\n    \}', content, re.DOTALL)
    if match:
        print("Contenido actual de init():")
        print(match.group(0)[:500])

with open(filepath, "w") as f:
    f.write(content)
print("✅ Archivo guardado")

import re

file_path = "/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java"

with open(file_path, "r", encoding="utf-8") as f:
    content = f.read()

# 1. Patch loadRatioDetails
old_load_details = """    public void loadRatioDetails(UserRatioDto dto) {
        log.info("Cargando detalles de Ratio ID {}: {} / {}", dto.getId(), dto.getNumerador(), dto.getDenominador());
        this.selectedPair = dto.getNumerador();
        this.selectedPair2 = dto.getDenominador();
        this.timeframe = dto.getPeriodo();
        this.daysBack = dto.getDias();
        this.smaPeriodParam = 2; // Default for now
        this.emaSlowPeriodParam = dto.getEmaLenta();
        this.currentRatioDto = dto;
        this.showStrategyConfiguration = true;
        this.showSubwindow = true;"""

new_load_details = """    public void loadRatioDetails(UserRatioDto dto) {
        log.info("Cargando detalles de Ratio ID {}: {} / {}", dto.getId(), dto.getNumerador(), dto.getDenominador());
        this.selectedPair = dto.getNumerador();
        this.selectedPair2 = dto.getDenominador();
        this.timeframe = dto.getPeriodo();
        this.daysBack = dto.getDias();
        this.smaPeriodParam = 2; // Default for now
        this.emaSlowPeriodParam = dto.getEmaLenta();
        this.currentRatioDto = dto;
        this.showStrategyConfiguration = true;
        this.showSubwindow = true;

        if (dto.getCreatedAt() != null && !dto.getCreatedAt().isEmpty()) {
            try {
                String cAt = dto.getCreatedAt().replace("T", " ");
                if (cAt.contains(".")) {
                    cAt = cAt.substring(0, cAt.indexOf("."));
                }
                this.startDate = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(cAt);
                log.info("START DATE configurada desde createdAt: {}", this.startDate);
            } catch (Exception e) {
                log.error("Error parsing createdAt for startDate: " + dto.getCreatedAt(), e);
                this.startDate = null;
            }
        } else {
            this.startDate = null;
        }"""

if old_load_details in content:
    content = content.replace(old_load_details, new_load_details)
else:
    print("Could not find old_load_details in DashboardBean.java")


# 2. Patch selectRatioFromActiveCycle
old_select_active = """    public void selectRatioFromActiveCycle(PositionCycleSummary cycle) {
        log.info("Seleccionado ratio desde Posiciones Activas: {} / {} (setup={})", cycle.getPairA(), cycle.getPairB(), cycle.getSetup());
        this.selectedPair = cycle.getPairA();
        this.selectedPair2 = cycle.getPairB();
        this.timeframe = cycle.getTimeframe() != null && !cycle.getTimeframe().isEmpty() ? cycle.getTimeframe() : "1h";
        
        // Restablecer ratio dto seleccionado"""

new_select_active = """    public void selectRatioFromActiveCycle(PositionCycleSummary cycle) {
        log.info("Seleccionado ratio desde Posiciones Activas: {} / {} (setup={})", cycle.getPairA(), cycle.getPairB(), cycle.getSetup());
        this.selectedPair = cycle.getPairA();
        this.selectedPair2 = cycle.getPairB();
        this.timeframe = cycle.getTimeframe() != null && !cycle.getTimeframe().isEmpty() ? cycle.getTimeframe() : "1h";
        
        this.startDate = null;
        if (this.userRatiosList != null) {
            for (UserRatioDto dto : this.userRatiosList) {
                if (dto.getNumerador().equals(cycle.getPairA()) && dto.getDenominador().equals(cycle.getPairB())) {
                    if (dto.getCreatedAt() != null && !dto.getCreatedAt().isEmpty()) {
                        try {
                            String cAt = dto.getCreatedAt().replace("T", " ");
                            if (cAt.contains(".")) {
                                cAt = cAt.substring(0, cAt.indexOf("."));
                            }
                            this.startDate = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(cAt);
                            log.info("START DATE (Posiciones Activas) configurada desde createdAt: {}", this.startDate);
                        } catch (Exception e) {
                            log.error("Error parsing createdAt for startDate: " + dto.getCreatedAt(), e);
                        }
                    }
                    break;
                }
            }
        }

        // Restablecer ratio dto seleccionado"""

if old_select_active in content:
    content = content.replace(old_select_active, new_select_active)
else:
    print("Could not find old_select_active in DashboardBean.java")

with open(file_path, "w", encoding="utf-8") as f:
    f.write(content)

print("DashboardBean patched successfully.")

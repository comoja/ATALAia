import re

file_path = "/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java"

with open(file_path, "r", encoding="utf-8") as f:
    content = f.read()

target1 = """        if (matchingRatio != null) {
            log.info("Encontrado matching en userRatiosList: periodo={}, dias={}, fast={}, slow={}",
                    matchingRatio.getPeriodo(), matchingRatio.getDias(), matchingRatio.getEmaRapida(), matchingRatio.getEmaLenta());
            this.selectedUserRatio = matchingRatio;
            if (matchingRatio.getPeriodo() != null && !matchingRatio.getPeriodo().trim().isEmpty()) {
                this.timeframe = matchingRatio.getPeriodo().trim();
            }
            if (matchingRatio.getDias() != null && matchingRatio.getDias() > 0) {
                this.daysBack = matchingRatio.getDias();
            }
            if (matchingRatio.getEmaRapida() != null && matchingRatio.getEmaRapida() > 0) {
                this.smaPeriodParam = matchingRatio.getEmaRapida();
            }
            if (matchingRatio.getEmaLenta() != null && matchingRatio.getEmaLenta() > 0) {
                this.emaSlowPeriodParam = matchingRatio.getEmaLenta();
            }"""

replacement1 = """        if (matchingRatio != null) {
            log.info("Encontrado matching en userRatiosList: periodo={}, dias={}, fast={}, slow={}",
                    matchingRatio.getPeriodo(), matchingRatio.getDias(), matchingRatio.getEmaRapida(), matchingRatio.getEmaLenta());
            this.selectedUserRatio = matchingRatio;
            if (matchingRatio.getPeriodo() != null && !matchingRatio.getPeriodo().trim().isEmpty()) {
                this.timeframe = matchingRatio.getPeriodo().trim();
            }
            if (matchingRatio.getDias() != null && matchingRatio.getDias() > 0) {
                this.daysBack = matchingRatio.getDias();
            }
            if (matchingRatio.getEmaRapida() != null && matchingRatio.getEmaRapida() > 0) {
                this.smaPeriodParam = matchingRatio.getEmaRapida();
            }
            if (matchingRatio.getEmaLenta() != null && matchingRatio.getEmaLenta() > 0) {
                this.emaSlowPeriodParam = matchingRatio.getEmaLenta();
            }
            
            if (matchingRatio.getCreatedAt() != null && !matchingRatio.getCreatedAt().isEmpty()) {
                try {
                    String cAt = matchingRatio.getCreatedAt().replace("T", " ");
                    if (cAt.contains(".")) {
                        cAt = cAt.substring(0, cAt.indexOf("."));
                    }
                    this.startDate = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(cAt);
                } catch (Exception e) {
                    log.error("Error parseando createdAt: " + matchingRatio.getCreatedAt(), e);
                    this.startDate = null;
                }
            } else {
                this.startDate = null;
            }"""

if target1 in content:
    content = content.replace(target1, replacement1)
else:
    print("Failed to find target1")

with open(file_path, "w", encoding="utf-8") as f:
    f.write(content)

print("DashboardBean updated via python again")

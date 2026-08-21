import re

# 1. Update DashboardBean.java
bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    bean_code = f.read()

# Add operar property if not present
if 'private Boolean operar = false;' not in bean_code:
    target_prop = 'private Integer daysBack = 180;'
    replacement_prop = '''private Integer daysBack = 180;
    private Boolean operar = false;

    public Boolean getOperar() {
        return operar;
    }

    public void setOperar(Boolean operar) {
        this.operar = operar;
    }'''
    bean_code = bean_code.replace(target_prop, replacement_prop)
    print("Added operar property in DashboardBean.java")

# Update guardarRatio method
old_guardar = '''    public void guardarRatio() {
        try {
            Integer userId = null;
            if (securityBean != null) {
                userId = securityBean.getSelectedUserId() != null ? securityBean.getSelectedUserId() : securityBean.getIdUsuario();
            }
            if (userId == null) {
                userId = 1;
            }

            RestTemplate restTemplate = new RestTemplate();
            org.springframework.http.HttpHeaders headers = new org.springframework.http.HttpHeaders();
            headers.setContentType(org.springframework.http.MediaType.APPLICATION_JSON);

            java.util.Map<String, Object> payload = new java.util.HashMap<>();
            payload.put("idUsuario", userId);
            payload.put("numerador", selectedPair);
            payload.put("denominador", selectedPair2);
            payload.put("periodo", timeframe != null ? timeframe : "1d");
            payload.put("EMARapida", smaPeriodParam != null ? smaPeriodParam : 3);
            payload.put("EMALenta", emaSlowPeriodParam != null ? emaSlowPeriodParam : 20);

            org.springframework.http.HttpEntity<java.util.Map<String, Object>> requestEntity = new org.springframework.http.HttpEntity<>(payload, headers);

            String url = backendUrl + "/api/v1/user-ratios/guardar";
            log.info("Guardando ratio para idUsuario {}: {}/{} ({}) [EMARapida={}, EMALenta={}] en {}", userId, selectedPair, selectedPair2, timeframe, smaPeriodParam, emaSlowPeriodParam, url);

            org.springframework.http.ResponseEntity<String> response = restTemplate.postForEntity(url, requestEntity, String.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                        new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_INFO,
                                "Ratio Guardado Exitosamente", "Se guardó la selección " + selectedPair + " / " + selectedPair2 + " (" + timeframe + ") con EMARapida=" + smaPeriodParam + ", EMALenta=" + emaSlowPeriodParam + "."));
            } else {
                javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                        new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_ERROR,
                                "Error al Guardar", "No se pudo registrar el ratio."));
            }
        } catch (Exception e) {
            log.error("Excepción al guardar ratio: {}", e.getMessage(), e);
            javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                    new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_ERROR,
                            "Error de Conexión", "Fallo al comunicar con el servidor backend: " + e.getMessage()));
        }
    }'''

new_guardar = '''    public void guardarRatio() {
        try {
            Integer userId = null;
            if (securityBean != null) {
                userId = securityBean.getSelectedUserId() != null ? securityBean.getSelectedUserId() : securityBean.getIdUsuario();
            }
            if (userId == null) {
                userId = 1;
            }

            RestTemplate restTemplate = new RestTemplate();
            org.springframework.http.HttpHeaders headers = new org.springframework.http.HttpHeaders();
            headers.setContentType(org.springframework.http.MediaType.APPLICATION_JSON);

            java.util.Map<String, Object> payload = new java.util.HashMap<>();
            payload.put("idUsuario", userId);
            payload.put("numerador", selectedPair);
            payload.put("denominador", selectedPair2);
            payload.put("periodo", timeframe != null ? timeframe : "1d");
            payload.put("dias", daysBack != null ? daysBack : 180);
            payload.put("EMARapida", smaPeriodParam != null ? smaPeriodParam : 3);
            payload.put("EMALenta", emaSlowPeriodParam != null ? emaSlowPeriodParam : 20);
            payload.put("operar", operar != null ? operar : false);

            org.springframework.http.HttpEntity<java.util.Map<String, Object>> requestEntity = new org.springframework.http.HttpEntity<>(payload, headers);

            String url = backendUrl + "/api/v1/user-ratios/guardar";
            log.info("Guardando ratio para idUsuario {}: {}/{} ({}, {} días) [EMARapida={}, EMALenta={}, Operar={}] en {}",
                    userId, selectedPair, selectedPair2, timeframe, daysBack, smaPeriodParam, emaSlowPeriodParam, operar, url);

            org.springframework.http.ResponseEntity<String> response = restTemplate.postForEntity(url, requestEntity, String.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                String operarDesc = Boolean.TRUE.equals(operar) ? "Activado (SI)" : "Desactivado (NO)";
                javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                        new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_INFO,
                                "Ratio Guardado Exitosamente",
                                "Se guardó la selección " + selectedPair + " / " + selectedPair2 +
                                " (" + timeframe + ", " + daysBack + " días) con EMARapida=" + smaPeriodParam +
                                ", EMALenta=" + emaSlowPeriodParam + ", Operar=" + operarDesc + "."));
            } else {
                javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                        new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_ERROR,
                                "Error al Guardar", "No se pudo registrar el ratio."));
            }
        } catch (Exception e) {
            log.error("Excepción al guardar ratio: {}", e.getMessage(), e);
            javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                    new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_ERROR,
                            "Error de Conexión", "Fallo al comunicar con el servidor backend: " + e.getMessage()));
        }
    }'''

if old_guardar in bean_code:
    bean_code = bean_code.replace(old_guardar, new_guardar)
    print("Updated guardarRatio in DashboardBean.java")

# Update fetchUserRatioDetails method
old_fetch = '''            if (responseStr != null && !responseStr.isEmpty()) {
                JsonNode rootNode = mapper.readTree(responseStr);
                if (rootNode.has("found") && rootNode.get("found").asBoolean()) {
                    if (rootNode.has("periodo") && !rootNode.get("periodo").isNull()) {
                        this.timeframe = rootNode.get("periodo").asText();
                    }
                    if (rootNode.has("EMARapida") && !rootNode.get("EMARapida").isNull()) {
                        this.smaPeriodParam = rootNode.get("EMARapida").asInt();
                    }
                    if (rootNode.has("EMALenta") && !rootNode.get("EMALenta").isNull()) {
                        this.emaSlowPeriodParam = rootNode.get("EMALenta").asInt();
                    }
                    log.info("✅ Configuración recuperada de BD para {}/{}: timeframe={}, EMARapida={}, EMALenta={}",
                            selectedPair, selectedPair2, timeframe, smaPeriodParam, emaSlowPeriodParam);

                    javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                            new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_INFO,
                                    "Configuración Cargada", "Se cargó la calibración guardada para " + selectedPair + " / " + selectedPair2));
                } else {
                    log.info("ℹ️ No existe registro en BD para {}/{}. Aplicando valores por defecto.", selectedPair, selectedPair2);
                    this.smaPeriodParam = 3;
                    this.emaSlowPeriodParam = 15;
                    this.timeframe = "1d";
                }
            }'''

new_fetch = '''            if (responseStr != null && !responseStr.isEmpty()) {
                JsonNode rootNode = mapper.readTree(responseStr);
                if (rootNode.has("found") && rootNode.get("found").asBoolean()) {
                    if (rootNode.has("periodo") && !rootNode.get("periodo").isNull()) {
                        this.timeframe = rootNode.get("periodo").asText();
                    }
                    if (rootNode.has("dias") && !rootNode.get("dias").isNull()) {
                        this.daysBack = rootNode.get("dias").asInt();
                        onDaysBackChange();
                    }
                    if (rootNode.has("EMARapida") && !rootNode.get("EMARapida").isNull()) {
                        this.smaPeriodParam = rootNode.get("EMARapida").asInt();
                    }
                    if (rootNode.has("EMALenta") && !rootNode.get("EMALenta").isNull()) {
                        this.emaSlowPeriodParam = rootNode.get("EMALenta").asInt();
                    }
                    if (rootNode.has("operar") && !rootNode.get("operar").isNull()) {
                        this.operar = rootNode.get("operar").asBoolean();
                    } else {
                        this.operar = false;
                    }
                    log.info("✅ Configuración recuperada de BD para {}/{}: timeframe={}, dias={}, EMARapida={}, EMALenta={}, operar={}",
                            selectedPair, selectedPair2, timeframe, daysBack, smaPeriodParam, emaSlowPeriodParam, operar);

                    javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                            new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_INFO,
                                    "Configuración Cargada", "Se cargó la calibración guardada para " + selectedPair + " / " + selectedPair2));
                } else {
                    log.info("ℹ️ No existe registro en BD para {}/{}. Aplicando valores por defecto.", selectedPair, selectedPair2);
                    this.smaPeriodParam = 3;
                    this.emaSlowPeriodParam = 15;
                    this.timeframe = "1d";
                    this.daysBack = 180;
                    this.operar = false;
                    onDaysBackChange();
                }
            }'''

if old_fetch in bean_code:
    bean_code = bean_code.replace(old_fetch, new_fetch)
    print("Updated fetchUserRatioDetails in DashboardBean.java")

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(bean_code)


# 2. Update dashboard.xhtml to include Operar checkbox
dashboard_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dashboard_path, 'r', encoding='utf-8') as f:
    dash_code = f.read()

operar_block = """                    <!-- Control Operar (Generar Órdenes) -->
                    <div style="margin-top: 14px; padding: 10px 14px; background: rgba(241, 245, 249, 0.6); border: 1px solid #e2e8f0; border-radius: 8px; display: flex; align-items: center; justify-content: space-between;">
                        <div style="display: flex; flex-direction: column;">
                            <span style="font-size: 0.85em; font-weight: 700; color: #1e293b;">Generar Órdenes (Operar)</span>
                            <span style="font-size: 0.72em; color: #64748b;">Habilita órdenes automáticas para este ratio</span>
                        </div>
                        <p:selectBooleanCheckbox id="operarSwitch" value="#{dashboardBean.operar}">
                            <p:ajax process="@this" />
                        </p:selectBooleanCheckbox>
                    </div>
                </h:panelGroup>"""

if 'id="operarSwitch"' not in dash_code:
    dash_code = dash_code.replace('                </h:panelGroup>', operar_block, 1)
    with open(dashboard_path, 'w', encoding='utf-8') as f:
        f.write(dash_code)
    print("Added operarSwitch to dashboard.xhtml")
else:
    print("operarSwitch already in dashboard.xhtml")

print("All frontend files updated successfully!")

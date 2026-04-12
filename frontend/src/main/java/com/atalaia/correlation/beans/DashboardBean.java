package com.atalaia.correlation.beans;

import javax.enterprise.context.RequestScoped;
import javax.inject.Named;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.client.RestTemplate;
import org.springframework.beans.factory.annotation.Value;

import java.io.Serializable;

@Named("dashboardBean")
@RequestScoped
@Getter
@Setter
@Slf4j
public class DashboardBean implements Serializable {

    private String selectedPair;
    private String analysisResult;

    @Value("${atalaia.backend.url}")
    private String backendUrl;

    public void analyzePair() {
        if (selectedPair == null || selectedPair.isEmpty()) {
            analysisResult = "Por favor, seleccione un par válido.";
            return;
        }

        try {
            log.info("Analizando par: {}", selectedPair);
            // Llamada síncrona simple a Python usando RestTemplate de Spring
            RestTemplate restTemplate = new RestTemplate();
            
            // Endpoint temporal, lo reemplazaremos cuando el backend esté programado
            String response = restTemplate.getForObject(backendUrl + "/api/v1/ping", String.class);
            
            analysisResult = "Conexión a Python Exitosa: " + response + " | Par seleccionado: " + selectedPair;
        } catch (Exception e) {
            log.error("Error al llamar al backend", e);
            analysisResult = "Error conectando al engine de Python: " + e.getMessage();
        }
    }
}

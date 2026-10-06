package com.atalaia.correlation.controllers;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

import javax.servlet.http.HttpServletRequest;
import java.net.URI;
import java.util.Enumeration;

@Slf4j
@RestController
@RequestMapping("/api/proxy")
public class MicroservicesProxyController {

    @Value("${forecast.service.url:http://127.0.0.1:8006}")
    private String forecastServiceUrl;

    @Value("${news.service.url:http://127.0.0.1:8007}")
    private String newsServiceUrl;

    private final RestTemplate restTemplate = new RestTemplate();

    @RequestMapping(value = "/forecast/**", method = {RequestMethod.GET, RequestMethod.POST, RequestMethod.OPTIONS})
    public ResponseEntity<String> proxyForecast(
            HttpServletRequest request,
            HttpMethod method,
            @RequestBody(required = false) String body) {
        return forwardRequest(request, method, body, forecastServiceUrl, "/api/proxy/forecast");
    }

    @RequestMapping(value = "/news/**", method = {RequestMethod.GET, RequestMethod.POST, RequestMethod.OPTIONS})
    public ResponseEntity<String> proxyNews(
            HttpServletRequest request,
            HttpMethod method,
            @RequestBody(required = false) String body) {
        return forwardRequest(request, method, body, newsServiceUrl, "/api/proxy/news");
    }

    private ResponseEntity<String> forwardRequest(
            HttpServletRequest request,
            HttpMethod method,
            String body,
            String targetBaseUrl,
            String prefixToRemove) {

        String requestUri = request.getRequestURI();
        int idx = requestUri.indexOf(prefixToRemove);
        String subPath = (idx != -1) ? requestUri.substring(idx + prefixToRemove.length()) : "";

        StringBuilder urlBuilder = new StringBuilder(targetBaseUrl).append(subPath);
        if (request.getQueryString() != null && !request.getQueryString().trim().isEmpty()) {
            urlBuilder.append("?").append(request.getQueryString());
        }

        String targetUrl = urlBuilder.toString();
        log.debug("[Proxy] Forwarding {} to {}", method, targetUrl);

        try {
            HttpHeaders headers = new HttpHeaders();
            Enumeration<String> headerNames = request.getHeaderNames();
            while (headerNames != null && headerNames.hasMoreElements()) {
                String headerName = headerNames.nextElement();
                if ("content-type".equalsIgnoreCase(headerName)) {
                    headers.set(headerName, request.getHeader(headerName));
                } else if ("accept".equalsIgnoreCase(headerName)) {
                    headers.set(headerName, request.getHeader(headerName));
                }
            }
            if (headers.getContentType() == null && body != null) {
                headers.setContentType(MediaType.APPLICATION_JSON);
            }

            HttpEntity<String> entity = new HttpEntity<>(body, headers);
            ResponseEntity<String> response = restTemplate.exchange(
                    URI.create(targetUrl),
                    method,
                    entity,
                    String.class
            );

            HttpHeaders respHeaders = new HttpHeaders();
            if (response.getHeaders().getContentType() != null) {
                respHeaders.setContentType(response.getHeaders().getContentType());
            } else {
                respHeaders.setContentType(MediaType.APPLICATION_JSON);
            }

            return new ResponseEntity<>(response.getBody(), respHeaders, response.getStatusCode());

        } catch (HttpStatusCodeException ex) {
            log.warn("[Proxy] Target {} returned HTTP {}: {}", targetUrl, ex.getStatusCode(), ex.getResponseBodyAsString());
            HttpHeaders errHeaders = new HttpHeaders();
            errHeaders.setContentType(MediaType.APPLICATION_JSON);
            return new ResponseEntity<>(ex.getResponseBodyAsString(), errHeaders, ex.getStatusCode());
        } catch (Exception ex) {
            log.error("[Proxy] Error connecting to {}: {}", targetUrl, ex.getMessage());
            HttpHeaders errHeaders = new HttpHeaders();
            errHeaders.setContentType(MediaType.APPLICATION_JSON);
            return new ResponseEntity<>("{\"error\": \"Proxy error: " + ex.getMessage() + "\"}", errHeaders, HttpStatus.BAD_GATEWAY);
        }
    }
}

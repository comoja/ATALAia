package com.atalaia.correlation.filters;

import com.atalaia.correlation.beans.SecurityBean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;

@Component
@Slf4j
public class SecurityFilter implements Filter {

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        log.info("Inicializando filtro de seguridad institucional de ATALAia...");
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {

        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;
        String requestUri = httpRequest.getRequestURI();
        String contextPath = httpRequest.getContextPath();

        // Eliminar contextPath para análisis relativo
        String relativeUri = requestUri.substring(contextPath.length());

        log.debug("Evaluando petición: {}", relativeUri);

        // Redirección inteligente de la ruta raíz (/) para evitar 404 Whitelabel Error
        if (relativeUri.equals("/")) {
            HttpSession session = httpRequest.getSession(false);
            SecurityBean securityBean = null;
            if (session != null) {
                securityBean = (SecurityBean) session.getAttribute("securityBean");
            }
            if (securityBean != null && securityBean.isLoggedIn()) {
                log.info("Redirección inteligente raíz (/): Usuario autenticado. Enviando a dashboard.xhtml");
                httpResponse.sendRedirect(contextPath + "/dashboard.xhtml");
            } else {
                log.info("Redirección inteligente raíz (/): Usuario no autenticado. Enviando a login.xhtml");
                httpResponse.sendRedirect(contextPath + "/login.xhtml");
            }
            return;
        }

        // 0. Endpoint ligero de verificación de sesión activa
        if (relativeUri.equals("/api/session-check")) {
            httpResponse.setContentType("application/json");
            httpResponse.setCharacterEncoding("UTF-8");
            httpResponse.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
            httpResponse.setHeader("Pragma", "no-cache");
            httpResponse.setDateHeader("Expires", 0);

            HttpSession session = httpRequest.getSession(false);
            SecurityBean securityBean = null;
            if (session != null) {
                securityBean = (SecurityBean) session.getAttribute("securityBean");
            }

            boolean isActive = (session != null && securityBean != null && securityBean.isLoggedIn());
            if (isActive) {
                httpResponse.setStatus(HttpServletResponse.SC_OK);
                httpResponse.getWriter().write("{\"active\":true,\"username\":\"" + (securityBean.getUsername() != null ? securityBean.getUsername() : "") + "\"}");
            } else {
                httpResponse.setStatus(HttpServletResponse.SC_OK);
                httpResponse.getWriter().write("{\"active\":false,\"redirectUrl\":\"" + contextPath + "/login.xhtml\"}");
            }
            httpResponse.getWriter().flush();
            return;
        }

        // 1. Permitir acceso a recursos estáticos (CSS, JS, imágenes de PrimeFaces, etc.)
        boolean isStaticResource = relativeUri.contains("/javax.faces.resource/") ||
                                   relativeUri.contains("/resources/") ||
                                   relativeUri.endsWith(".css") ||
                                   relativeUri.endsWith(".js") ||
                                   relativeUri.endsWith(".png") ||
                                   relativeUri.endsWith(".jpg") ||
                                   relativeUri.endsWith(".ico");

        // 2. Permitir acceso a la página de login
        boolean isLoginPage = relativeUri.equals("/login.xhtml") || 
                              relativeUri.equals("/login");

        // Si es recurso estático o página de login, continuar sin evaluar autenticación
        if (isStaticResource || isLoginPage) {
            chain.doFilter(request, response);
            return;
        }

        // 3. Evaluar sesión de usuario para páginas protegidas (.xhtml)
        if (relativeUri.endsWith(".xhtml")) {
            HttpSession session = httpRequest.getSession(false);
            SecurityBean securityBean = null;

            if (session != null) {
                securityBean = (SecurityBean) session.getAttribute("securityBean");
            }

            if (securityBean == null || !securityBean.isLoggedIn()) {
                log.warn("Acceso no autorizado bloqueado a: {}. Redirigiendo a login.xhtml", relativeUri);

                String facesRequest = httpRequest.getHeader("Faces-Request");
                boolean isAjax = "partial/ajax".equals(facesRequest) || 
                                 "XMLHttpRequest".equals(httpRequest.getHeader("X-Requested-With"));

                if (isAjax) {
                    httpResponse.setContentType("text/xml");
                    httpResponse.setCharacterEncoding("UTF-8");
                    httpResponse.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
                    httpResponse.getWriter().write(
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                        "<partial-response id=\"j_id1\"><redirect url=\"" + contextPath + "/login.xhtml\"></redirect></partial-response>"
                    );
                    httpResponse.getWriter().flush();
                    return;
                }

                // Redireccionar al login para peticiones estándar
                httpResponse.sendRedirect(contextPath + "/login.xhtml");
                return;
            }

            // 4. Validación de menús autorizados (Seguridad Dinámica de Grado Institucional)
            // Si el usuario está autenticado, verificar si la URL solicitada corresponde a sus menús asignados
            // Excepción: dashboard siempre está disponible para todos los autenticados.
            if (!relativeUri.contains("dashboard.xhtml")) {
                boolean hasAccess = false;
                for (SecurityBean.MenuDto menu : securityBean.getAuthorizedMenus()) {
                    if (relativeUri.contains(menu.getUrl())) {
                        hasAccess = true;
                        break;
                    }
                }

                if (!hasAccess) {
                    log.warn("Usuario {} intentó acceder a {} sin privilegios. Rol: {}. Redirigiendo a dashboard.",
                        securityBean.getUsername(), relativeUri, securityBean.getNameRole());
                    httpResponse.sendRedirect(contextPath + "/dashboard.xhtml");
                    return;
                }
            }
        }

        chain.doFilter(request, response);
    }

    @Override
    public void destroy() {
        log.info("Destruyendo filtro de seguridad...");
    }
}

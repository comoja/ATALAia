package com.atalaia.correlation.beans;

import javax.enterprise.context.SessionScoped;
import javax.faces.application.FacesMessage;
import javax.faces.context.FacesContext;
import javax.inject.Named;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.client.RestTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;

import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

@Named("securityBean")
@SessionScoped
@Getter
@Setter
@Slf4j
public class SecurityBean implements Serializable {

    private static final long serialVersionUID = 1L;

    private String username;
    private String password;
    private boolean loggedIn = false;

    private Integer idUsuario;
    private String email;
    private String nombre;
    private String apellidoPaterno;
    private String apellidoMaterno;
    private String nombreCompleto;
    private Integer idRole;
    private String nameRole;
    private List<MenuDto> authorizedMenus = new ArrayList<>();

    // Combo de usuarios — solo habilitado para admin
    private List<UsuarioComboItem> usuariosCombo = new ArrayList<>();
    private Integer selectedUserId;

    private final String backendApiUrl = "http://127.0.0.1:8004/api/v1";

    public String login() {
        log.info("Intento de login para usuario: {}", username);
        if (username == null || username.trim().isEmpty() || password == null || password.trim().isEmpty()) {
            FacesContext.getCurrentInstance().addMessage(null,
                new FacesMessage(FacesMessage.SEVERITY_WARN, "Campos Requeridos", "Por favor ingresa usuario y contraseña."));
            return null;
        }

        try {
            RestTemplate restTemplate = new RestTemplate();
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            LoginRequest requestPayload = new LoginRequest(username.trim(), password);
            HttpEntity<LoginRequest> requestEntity = new HttpEntity<>(requestPayload, headers);

            ResponseEntity<UserResponse> response = restTemplate.postForEntity(
                backendApiUrl + "/auth/login",
                requestEntity,
                UserResponse.class
            );

            if (response.getStatusCode().is2xxSuccessful()) {
                UserResponse user = response.getBody();
                if (user != null) {
                    this.idUsuario        = user.getIdUsuario();
                    this.email            = user.getEmail();
                    this.nombre           = user.getNombre();
                    this.apellidoPaterno  = user.getApellidoPaterno();
                    this.apellidoMaterno  = user.getApellidoMaterno();
                    this.idRole           = user.getIdRole();
                    this.nameRole         = user.getNameRole();
                    this.loggedIn         = true;

                    // Construir nombre completo con fallback al username
                    String fullName = buildNombreCompleto(nombre, apellidoPaterno, apellidoMaterno);
                    this.nombreCompleto = fullName.isEmpty() ? this.username : fullName;

                    log.info("Usuario {} autenticado correctamente. Rol: {}", username, nameRole);
                    
                    // Cargar menús autorizados
                    loadAuthorizedMenus();

                    // Cargar combo de usuarios (todos los roles lo reciben, el admin puede interactuar)
                    loadUsuariosCombo();

                    // Seleccionar al usuario actual como default en el combo
                    this.selectedUserId = this.idUsuario;

                    // Limpiar contraseña de la memoria
                    this.password = null;

                    // Redireccionar al dashboard
                    return "/dashboard.xhtml?faces-redirect=true";
                }
            }
        } catch (org.springframework.web.client.HttpClientErrorException.Unauthorized e) {
            log.warn("Credenciales incorrectas para usuario: {}", username);
            FacesContext.getCurrentInstance().addMessage(null,
                new FacesMessage(FacesMessage.SEVERITY_ERROR, "Acceso Denegado", "Usuario o contraseña incorrectos."));
        } catch (org.springframework.web.client.HttpClientErrorException.Forbidden e) {
            log.warn("Cuenta inactiva para usuario: {}", username);
            FacesContext.getCurrentInstance().addMessage(null,
                new FacesMessage(FacesMessage.SEVERITY_ERROR, "Cuenta Desactivada", "Esta cuenta está temporalmente inactiva."));
        } catch (Exception e) {
            log.error("Error al conectar con el backend de autenticación: {}", e.getMessage());
            FacesContext.getCurrentInstance().addMessage(null,
                new FacesMessage(FacesMessage.SEVERITY_FATAL, "Error del Servidor", "No se pudo establecer conexión con el servicio de seguridad."));
        }
        return null;
    }

    public String logout() {
        log.info("Cerrando sesión para el usuario: {}", username);
        
        // Invalidar sesión JSF y limpiar variables
        FacesContext.getCurrentInstance().getExternalContext().invalidateSession();
        this.loggedIn       = false;
        this.username       = null;
        this.password       = null;
        this.nombre         = null;
        this.apellidoPaterno = null;
        this.apellidoMaterno = null;
        this.nombreCompleto = null;
        this.idRole         = null;
        this.nameRole       = null;
        this.selectedUserId = null;
        this.authorizedMenus.clear();
        this.usuariosCombo.clear();

        return "/login.xhtml?faces-redirect=true";
    }

    private void loadAuthorizedMenus() {
        try {
            RestTemplate restTemplate = new RestTemplate();
            String url = backendApiUrl + "/auth/menu/" + this.idRole;
            
            MenuDto[] response = restTemplate.getForObject(url, MenuDto[].class);
            if (response != null) {
                this.authorizedMenus = new ArrayList<>(Arrays.asList(response));
                log.info("Menús cargados exitosamente para el rol {}: cantidad = {}", nameRole, authorizedMenus.size());
            }
        } catch (Exception e) {
            log.error("Error cargando menús del rol {}: {}", idRole, e.getMessage());
            this.authorizedMenus = new ArrayList<>();
        }
    }

    /** Carga la lista de usuarios activos para el combo selector del dashboard. */
    private void loadUsuariosCombo() {
        try {
            RestTemplate restTemplate = new RestTemplate();
            String url = backendApiUrl + "/auth/usuarios";
            UsuarioComboItem[] response = restTemplate.getForObject(url, UsuarioComboItem[].class);
            if (response != null) {
                this.usuariosCombo = new ArrayList<>(Arrays.asList(response));
                log.info("Combo de usuarios cargado: {} registros", usuariosCombo.size());
            }
        } catch (Exception e) {
            log.error("Error cargando combo de usuarios: {}", e.getMessage());
            this.usuariosCombo = new ArrayList<>();
        }
    }

    /** Helper: construye el nombre completo uniendo las partes no nulas. */
    private String buildNombreCompleto(String nombre, String apPat, String apMat) {
        StringBuilder sb = new StringBuilder();
        if (nombre != null && !nombre.trim().isEmpty())  sb.append(nombre.trim());
        if (apPat  != null && !apPat.trim().isEmpty())  { if (sb.length() > 0) sb.append(" "); sb.append(apPat.trim()); }
        if (apMat  != null && !apMat.trim().isEmpty())  { if (sb.length() > 0) sb.append(" "); sb.append(apMat.trim()); }
        return sb.toString();
    }

    /** Devuelve true si el usuario en sesión es administrador. */
    public boolean isAdmin() {
        return Integer.valueOf(1).equals(this.idRole);
    }

    public boolean getAdmin() {
        return isAdmin();
    }

    // --- DTOs estáticos ---

    @Getter
    @Setter
    public static class LoginRequest implements Serializable {
        private static final long serialVersionUID = 1L;
        private String username;
        private String password;

        public LoginRequest() {}

        public LoginRequest(String username, String password) {
            this.username = username;
            this.password = password;
        }
    }

    @Getter
    @Setter
    public static class UserResponse implements Serializable {
        private static final long serialVersionUID = 1L;
        private Integer idUsuario;
        private String  username;
        private String  email;
        private String  nombre;
        private String  apellidoPaterno;
        private String  apellidoMaterno;
        private Integer idRole;
        private String  nameRole;
        private Integer status;
    }

    @Getter
    @Setter
    public static class UsuarioComboItem implements Serializable {
        private static final long serialVersionUID = 1L;
        private Integer idUsuario;
        private String  nombreCompleto;
    }

    @Getter
    @Setter
    public static class MenuDto implements Serializable {
        private static final long serialVersionUID = 1L;
        private Integer idMenu;
        private String  nameMenu;
        private String  url;
        private String  icon;
        private Integer parentId;
    }
}

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
import org.springframework.web.client.HttpClientErrorException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

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

    // Campos para cambio de contraseña
    private String currentPassword;
    private String newPassword;
    private String confirmPassword;
    private boolean passwordExpired = false;
    private int daysSincePasswordUpdate = 0;
    private boolean mustChangePassword = false;
    private boolean showChangePasswordDialog = false;

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

            LoginRequest requestPayload = new LoginRequest(this.username, this.password);
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
                    this.passwordExpired  = Boolean.TRUE.equals(user.getPasswordExpired());
                    this.daysSincePasswordUpdate = user.getDaysSincePasswordUpdate() != null ? user.getDaysSincePasswordUpdate() : 0;
                    this.mustChangePassword = Boolean.TRUE.equals(user.getMustChangePassword());

                    // Construir nombre completo con fallback al username
                    String fullName = buildNombreCompleto(nombre, apellidoPaterno, apellidoMaterno);
                    this.nombreCompleto = fullName.isEmpty() ? this.username : fullName;

                    // Si la contraseña expiró (más de 120 días / 4 meses) o tiene cambio forzado
                    if (this.passwordExpired) {
                        log.warn("Usuario {} autenticado pero su contraseña ha expirado (hace {} días). Se requiere renovación.", 
                                 username, daysSincePasswordUpdate);
                        this.currentPassword = this.password;
                        this.newPassword = null;
                        this.confirmPassword = null;
                        this.showChangePasswordDialog = true;
                        this.loggedIn = false;
                        
                        FacesContext.getCurrentInstance().addMessage(null,
                            new FacesMessage(FacesMessage.SEVERITY_WARN, "Contraseña Expirada", 
                                "Su clave de acceso ha superado el límite de 4 meses (120 días). Por favor actualícela ahora."));
                        return null;
                    }

                    this.loggedIn = true;
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
        } catch (HttpClientErrorException.Unauthorized e) {
            log.warn("Credenciales incorrectas para usuario: {}", username);
            FacesContext.getCurrentInstance().addMessage(null,
                new FacesMessage(FacesMessage.SEVERITY_ERROR, "Acceso Denegado", "Usuario o contraseña incorrectos."));
        } catch (HttpClientErrorException.Forbidden e) {
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

    /**
     * Procesa el cambio de contraseña tanto por expiración periódica como por solicitud voluntaria.
     */
    public String changePassword() {
        log.info("Procesando cambio de contraseña para el usuario: {}", username);

        if (currentPassword == null || currentPassword.trim().isEmpty()) {
            FacesContext.getCurrentInstance().addMessage(null,
                new FacesMessage(FacesMessage.SEVERITY_ERROR, "Validación", "Debe ingresar la contraseña actual."));
            return null;
        }

        if (newPassword == null || newPassword.trim().isEmpty()) {
            FacesContext.getCurrentInstance().addMessage(null,
                new FacesMessage(FacesMessage.SEVERITY_ERROR, "Validación", "Debe ingresar la nueva contraseña."));
            return null;
        }

        if (confirmPassword == null || confirmPassword.trim().isEmpty()) {
            FacesContext.getCurrentInstance().addMessage(null,
                new FacesMessage(FacesMessage.SEVERITY_ERROR, "Validación", "Debe confirmar la nueva contraseña."));
            return null;
        }

        if (!newPassword.equals(confirmPassword)) {
            FacesContext.getCurrentInstance().addMessage(null,
                new FacesMessage(FacesMessage.SEVERITY_ERROR, "Validación", "La nueva contraseña y su confirmación no coinciden."));
            return null;
        }

        if (newPassword.equals(currentPassword)) {
            FacesContext.getCurrentInstance().addMessage(null,
                new FacesMessage(FacesMessage.SEVERITY_ERROR, "Validación", "La nueva contraseña debe ser diferente a la contraseña actual."));
            return null;
        }

        // Validación de Política de Contraseñas institucional (8+ caracteres, número, mayúscula, minúscula, símbolo)
        String policyError = validatePasswordPolicy(newPassword);
        if (policyError != null) {
            FacesContext.getCurrentInstance().addMessage(null,
                new FacesMessage(FacesMessage.SEVERITY_ERROR, "Política de Seguridad", policyError));
            return null;
        }

        try {
            RestTemplate restTemplate = new RestTemplate();
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            ChangePasswordRequest payload = new ChangePasswordRequest(
                this.username,
                this.currentPassword,
                this.newPassword,
                this.confirmPassword
            );

            HttpEntity<ChangePasswordRequest> requestEntity = new HttpEntity<>(payload, headers);

            ResponseEntity<ActionResponse> response = restTemplate.postForEntity(
                backendApiUrl + "/auth/change-password",
                requestEntity,
                ActionResponse.class
            );

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                ActionResponse actionResp = response.getBody();
                if (actionResp.isSuccess()) {
                    log.info("Contraseña actualizada exitosamente para: {}", username);
                    
                    this.passwordExpired = false;
                    this.mustChangePassword = false;
                    this.showChangePasswordDialog = false;
                    this.currentPassword = null;
                    this.newPassword = null;
                    this.confirmPassword = null;
                    this.password = null;

                    UserResponse user = actionResp.getUser();
                    if (user != null) {
                        this.idUsuario        = user.getIdUsuario();
                        this.email            = user.getEmail();
                        this.nombre           = user.getNombre();
                        this.apellidoPaterno  = user.getApellidoPaterno();
                        this.apellidoMaterno  = user.getApellidoMaterno();
                        this.idRole           = user.getIdRole();
                        this.nameRole         = user.getNameRole();
                        this.loggedIn         = true;

                        String fullName = buildNombreCompleto(nombre, apellidoPaterno, apellidoMaterno);
                        this.nombreCompleto = fullName.isEmpty() ? this.username : fullName;

                        loadAuthorizedMenus();
                        loadUsuariosCombo();
                        this.selectedUserId = this.idUsuario;
                    }

                    FacesContext.getCurrentInstance().getExternalContext().getFlash().setKeepMessages(true);
                    FacesContext.getCurrentInstance().addMessage(null,
                        new FacesMessage(FacesMessage.SEVERITY_INFO, "Éxito", "Contraseña actualizada exitosamente."));

                    return "/dashboard.xhtml?faces-redirect=true";
                }
            }
        } catch (HttpClientErrorException e) {
            String errorDetail = "Error al actualizar la contraseña.";
            try {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode root = mapper.readTree(e.getResponseBodyAsString());
                if (root.has("detail")) {
                    errorDetail = root.get("detail").asText();
                }
            } catch (Exception parseEx) {
                errorDetail = e.getResponseBodyAsString();
            }
            log.warn("Error devuelto por el backend al cambiar contraseña: {}", errorDetail);
            FacesContext.getCurrentInstance().addMessage(null,
                new FacesMessage(FacesMessage.SEVERITY_ERROR, "Error de Validación", errorDetail));
        } catch (Exception e) {
            log.error("Excepción inesperada al cambiar contraseña: {}", e.getMessage(), e);
            FacesContext.getCurrentInstance().addMessage(null,
                new FacesMessage(FacesMessage.SEVERITY_FATAL, "Error del Servidor", "No se pudo completar el cambio de contraseña."));
        }

        return null;
    }

    /**
     * Valida los 4 criterios institucionales de seguridad de contraseñas:
     * 1. Mínimo 8 caracteres.
     * 2. Mínimo 1 número (0-9).
     * 3. Letras mayúsculas (A-Z) y minúsculas (a-z).
     * 4. Mínimo 1 símbolo / caracter especial.
     */
    public static String validatePasswordPolicy(String pass) {
        if (pass == null || pass.length() < 8) {
            return "La contraseña debe tener al menos 8 caracteres.";
        }
        boolean hasDigit = false;
        boolean hasUpper = false;
        boolean hasLower = false;
        boolean hasSymbol = false;

        for (char ch : pass.toCharArray()) {
            if (Character.isDigit(ch)) {
                hasDigit = true;
            } else if (Character.isUpperCase(ch)) {
                hasUpper = true;
            } else if (Character.isLowerCase(ch)) {
                hasLower = true;
            } else if (!Character.isWhitespace(ch)) {
                hasSymbol = true;
            }
        }

        if (!hasDigit) {
            return "La contraseña debe contener al menos 1 número (0-9).";
        }
        if (!hasUpper) {
            return "La contraseña debe contener al menos 1 letra mayúscula (A-Z).";
        }
        if (!hasLower) {
            return "La contraseña debe contener al menos 1 letra minúscula (a-z).";
        }
        if (!hasSymbol) {
            return "La contraseña debe contener al menos 1 símbolo especial (!@#$%^&*...).";
        }
        return null; // Válida
    }

    public void openChangePasswordDialog() {
        this.currentPassword = null;
        this.newPassword = null;
        this.confirmPassword = null;
        this.showChangePasswordDialog = true;
    }

    public void closeChangePasswordDialog() {
        this.showChangePasswordDialog = false;
        this.currentPassword = null;
        this.newPassword = null;
        this.confirmPassword = null;
    }

    public String logout() {
        log.info("Cerrando sesión para el usuario: {}", username);
        
        // Invalidar sesión JSF y limpiar variables
        FacesContext.getCurrentInstance().getExternalContext().invalidateSession();
        this.loggedIn       = false;
        this.username       = null;
        this.password       = null;
        this.currentPassword = null;
        this.newPassword    = null;
        this.confirmPassword = null;
        this.nombre         = null;
        this.apellidoPaterno = null;
        this.apellidoMaterno = null;
        this.nombreCompleto = null;
        this.idRole         = null;
        this.nameRole       = null;
        this.selectedUserId = null;
        this.passwordExpired = false;
        this.showChangePasswordDialog = false;
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

    public boolean isCanEdit() {
        if (this.selectedUserId != null && this.idUsuario != null) {
            return this.selectedUserId.equals(this.idUsuario);
        }
        return true;
    }

    public boolean isCanCreateAccount() {
        return idRole != null && idRole < 3;
    }

    public boolean isReadOnly() {
        return !isCanEdit();
    }

    public String getSelectedUserName() {
        if (selectedUserId == null) return "";
        if (usuariosCombo != null) {
            for (UsuarioComboItem u : usuariosCombo) {
                if (selectedUserId.equals(u.getIdUsuario())) {
                    return u.getNombreCompleto();
                }
            }
        }
        return "Usuario #" + selectedUserId;
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
    public static class ChangePasswordRequest implements Serializable {
        private static final long serialVersionUID = 1L;
        private String username;
        private String currentPassword;
        private String newPassword;
        private String confirmPassword;

        public ChangePasswordRequest() {}

        public ChangePasswordRequest(String username, String currentPassword, String newPassword, String confirmPassword) {
            this.username = username;
            this.currentPassword = currentPassword;
            this.newPassword = newPassword;
            this.confirmPassword = confirmPassword;
        }
    }

    @Getter
    @Setter
    public static class ActionResponse implements Serializable {
        private static final long serialVersionUID = 1L;
        private boolean success;
        private String  message;
        private UserResponse user;
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
        private Boolean passwordExpired;
        private Integer daysSincePasswordUpdate;
        private Boolean mustChangePassword;
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

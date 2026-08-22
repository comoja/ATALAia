bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

target_pnl_b = '''        public String getFormattedPnlB() {
            return pnlB != null ? String.format(java.util.Locale.US, "%,.2f", pnlB) : "0.00";
        }
    }'''

new_helpers = '''        public String getFormattedPnlB() {
            return pnlB != null ? String.format(java.util.Locale.US, "%,.2f", pnlB) : "0.00";
        }
        public boolean isSquare() {
            return signalType != null && signalType.contains("CUADRO");
        }
        public boolean isTriangle() {
            return signalType != null && signalType.contains("TRIANGULO");
        }
        public boolean isTriangleUp() {
            return signalType != null && (signalType.contains("TRIANGULO_VERDE") || signalType.contains("UP"));
        }
        public String getSignalColor() {
            if (signalType == null) return "#1e293b";
            if (signalType.contains("VERDE")) return "#15803d";
            if (signalType.contains("ROJO")) return "#dc2626";
            return "#1e293b";
        }
        public String getSignalOriginPair() {
            if (signalType == null) return "";
            String s = signalType.replace("CUADRO_VERDE", "")
                                 .replace("CUADRO_ROJO", "")
                                 .replace("TRIANGULO_VERDE", "")
                                 .replace("TRIANGULO_ROJO", "")
                                 .replace("(EN CURSO)", "")
                                 .trim();
            if (s.startsWith("(") && s.endsWith(")")) {
                s = s.substring(1, s.length() - 1).trim();
            }
            return s.isEmpty() ? signalType : s;
        }
        public boolean isLongA() {
            if (direction == null) return true;
            return direction.startsWith("LONG");
        }
        public boolean isLongB() {
            if (direction == null) return false;
            return direction.contains("/ LONG");
        }
        public String getPairAColor() {
            return isLongA() ? "#15803d" : "#dc2626";
        }
        public String getPairBColor() {
            return isLongB() ? "#15803d" : "#dc2626";
        }
    }'''

code = code.replace(target_pnl_b, new_helpers, 1)

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Updated DashboardBean.java with visual shape and direction color helpers!")

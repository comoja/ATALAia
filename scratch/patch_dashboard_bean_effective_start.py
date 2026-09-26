import re

file_path = "/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java"

with open(file_path, "r", encoding="utf-8") as f:
    content = f.read()

method_code = """
    private java.util.Date calculateEffectiveStartDate(String createdAtStr, String timeframe, int dias) {
        if (createdAtStr == null || createdAtStr.trim().isEmpty()) return null;
        try {
            String cAt = createdAtStr.replace("T", " ");
            if (cAt.contains(".")) {
                cAt = cAt.substring(0, cAt.indexOf("."));
            }
            java.util.Date createdDate = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(cAt);
            java.util.Calendar cal = java.util.Calendar.getInstance();
            cal.setTime(createdDate);

            if (timeframe == null) timeframe = "1h";
            String tf = timeframe.toLowerCase().trim();
            int amountToSubtract = dias;

            if (tf.contains("m") && !tf.contains("mo")) {
                int minutesPerPeriod = 1;
                if (tf.contains("15")) minutesPerPeriod = 15;
                else if (tf.contains("30")) minutesPerPeriod = 30;
                else if (tf.contains("5")) minutesPerPeriod = 5;
                cal.add(java.util.Calendar.MINUTE, -(amountToSubtract * minutesPerPeriod));
            } else if (tf.contains("h")) {
                int hoursPerPeriod = 1;
                if (tf.contains("4")) hoursPerPeriod = 4;
                cal.add(java.util.Calendar.HOUR_OF_DAY, -(amountToSubtract * hoursPerPeriod));
            } else if (tf.contains("d")) {
                cal.add(java.util.Calendar.DAY_OF_YEAR, -amountToSubtract);
            } else if (tf.contains("w")) {
                cal.add(java.util.Calendar.WEEK_OF_YEAR, -amountToSubtract);
            } else if (tf.contains("mo")) {
                cal.add(java.util.Calendar.MONTH, -amountToSubtract);
            } else {
                cal.add(java.util.Calendar.HOUR_OF_DAY, -amountToSubtract);
            }
            return cal.getTime();
        } catch (Exception e) {
            log.error("Error calculando effectiveStartDate: ", e);
            return null;
        }
    }
"""

if "private java.util.Date calculateEffectiveStartDate" not in content:
    update_idx = content.find("public void onSelectUserRatio(UserRatioDto ratio)")
    if update_idx != -1:
        # Go back to the beginning of the line
        while update_idx > 0 and content[update_idx-1] != '\n':
            update_idx -= 1
        content = content[:update_idx] + method_code + "\n" + content[update_idx:]
    else:
        print("Could not find onSelectUserRatio")

with open(file_path, "w", encoding="utf-8") as f:
    f.write(content)

print("DashboardBean updated with calculateEffectiveStartDate.")

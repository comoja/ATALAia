bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

# Add getSelectedAccountCapital() helper if not present
capital_helper = '''    public Double getSelectedAccountCapital() {
        if (selectedAccountId != null && userAccountsCombo != null) {
            for (UserAccountDto a : userAccountsCombo) {
                if (a.getIdCuenta() != null && a.getIdCuenta().equals(selectedAccountId)) {
                    if (a.getCapital() != null && a.getCapital() > 0) {
                        return a.getCapital();
                    }
                }
            }
        }
        return 1000.0;
    }'''

if 'getSelectedAccountCapital' not in code:
    target_pos = '    public List<UserAccountDto> getUserAccountsCombo() {'
    code = code.replace(target_pos, capital_helper + '\n\n' + target_pos)

# Update loadQuantPairAnalysis URL
old_url = '''            String url = String.format(
                    "%s/api/v1/quant/pair-analysis/%s?pairB=%s&timeframe=%s&days=%d&start_date=%s&end_date=%s",
                    backendUrl,
                    java.net.URLEncoder.encode(selectedPair, "UTF-8"),
                    java.net.URLEncoder.encode(selectedPair2, "UTF-8"),
                    timeframe, (daysBack != null ? daysBack : 180), startStr, endStr);'''

new_url = '''            String url = String.format(
                    "%s/api/v1/quant/pair-analysis/%s?pairB=%s&timeframe=%s&days=%d&start_date=%s&end_date=%s" +
                    (selectedAccountId != null ? "&idCuenta=" + selectedAccountId : "") +
                    "&leverage=100.0",
                    backendUrl,
                    java.net.URLEncoder.encode(selectedPair, "UTF-8"),
                    java.net.URLEncoder.encode(selectedPair2, "UTF-8"),
                    timeframe, (daysBack != null ? daysBack : 180), startStr, endStr);'''

code = code.replace(old_url, new_url)

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Updated DashboardBean.java with account capital and leverage 1:100 URL parameters!")

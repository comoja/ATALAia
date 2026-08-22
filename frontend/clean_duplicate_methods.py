bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

target_dup = '''    private List<UserAccountDto> userAccountsCombo = new ArrayList<>();
    private Integer selectedAccountId;

    public Double getSelectedAccountCapital() {
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

code = code.replace(target_dup, '    private List<UserAccountDto> userAccountsCombo = new ArrayList<>();\n    private Integer selectedAccountId;')

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("SUCCESS: Cleaned up duplicate getSelectedAccountCapital in DashboardBean.java!")

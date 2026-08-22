routes_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/backend/api/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    code = f.read()

target_tri = '''        trianglesOnlyBt = quantEngine.runSignalBacktest(
            df_a_tf, df_b_tf,'''

new_tri = '''        trianglesOnlyBt = quantEngine.runSignalBacktest(
            df_a_tf, df_b_tf,
            pairA=pairA, pairB=pairB,'''

target_comb = '''        combinedBt = quantEngine.runSignalBacktest(
            df_a_tf, df_b_tf,'''

new_comb = '''        combinedBt = quantEngine.runSignalBacktest(
            df_a_tf, df_b_tf,
            pairA=pairA, pairB=pairB,'''

code = code.replace(target_tri, new_tri).replace(target_comb, new_comb)

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Updated routes.py with pairA and pairB arguments!")

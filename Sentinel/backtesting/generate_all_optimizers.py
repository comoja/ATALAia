import os
import sys

base_dir = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/Sentinel/backtesting'

def write_script(filename, content):
    path = os.path.join(base_dir, filename)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content.strip() + '\n')
    print(f"✅ Generated {filename}")

print("Generator script template ready.")

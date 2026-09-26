import re

file_path = "/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml"

with open(file_path, "r", encoding="utf-8") as f:
    content = f.read()

# We need to find the block in renderHechosRatioChart where datasets are pushed
# The push happens like this:
# datasets.push({
#     label: pairA,
#     data: priceASeries,
#     borderColor: '#007bff',
#     backgroundColor: 'transparent',
#     borderWidth: 2,
#     pointRadius: 0,
#     pointHoverRadius: 4,
#     tension: 0.1
# },
# 
# We'll just replace tension: 0.1 \n } with tension: 0.1, yAxisID: 'y' \n }
# Actually, let's do a more robust regex replacement inside the renderHechosRatioChart function
# We'll just replace all `tension: 0.1` inside the dataset definition for HechosRatioChart with `tension: 0.1, yAxisID: 'y'`
# And `pointHoverRadius: 0` for the mean with `pointHoverRadius: 0, yAxisID: 'y'`
# Or I can just replace `{ label:` with `{ yAxisID: 'y', label:` between lines 4370 and 4410.

lines = content.split('\n')
for i in range(4350, 4420):
    if '{' in lines[i] and 'label:' in lines[i+1]:
        # we can just add yAxisID: 'y', after {
        pass

# Better approach:
replacement_start = False
new_lines = []
for i, line in enumerate(lines):
    if "const datasets = [" in line and i > 4300 and i < 4400:
        replacement_start = True
    
    if replacement_start and "label:" in line:
        line = line.replace("label:", "yAxisID: 'y', label:")
    
    if replacement_start and "];" in line:
        replacement_start = False
        
    new_lines.append(line)

with open(file_path, "w", encoding="utf-8") as f:
    f.write('\n'.join(new_lines))

print("Fixed yAxisID in renderHechosRatioChart")

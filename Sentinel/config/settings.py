import numpy as np

SYMBOLS = np.array(["USD/MXN", "XAU/USD"])
timeframes = [6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23]

FESTIVOS = ["2026-01-01", "2026-12-25", "2026-05-01"]

API_KEYS = ["98c13fd2d0714dc984ca2791e9e3d521",
            "99cc3d9bead5422c99f5614131c9ba4c",
            "6ac737658dde42fc9874bec8200b01ca"
]
URLTDSERIES = "https://api.twelvedata.com/timeSeries"
MaxXminuto = 8
MaxXdia = 800
minutosXdia = 24 * 60 

INTERVAL = "15min"
INTERVALmax = "15min"

timeZone = "America/Mexico_City"

dbConfig = {
    "host": "localhost",
    "user": "root",
    "password": "M1x&J34ny",
    "database": "ATALAia"
}

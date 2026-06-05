import sys
import os
from datetime import datetime

# Configure project path
projectRoot = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if projectRoot not in sys.path:
    sys.path.insert(0, projectRoot)

from middleware.utils.imageGenerator import generateSignalCard

def testOverlapSignal():
    # Setup test data resembling the USD/JPY signal where layout overlapped
    signal = {
        "direction": "CORTO",
        "entryPrice": 159.94867,
        "stop_loss": 160.00627,
        "take_profit": 157.54944,
        "confidence": 65.0,
        "marketSentiment": 0.0,
        "setup": "QTrend Crossing",
        "candleTime": "2026-06-04 19:30:00",
        "profit": 14.77,
        "rr_ratio": 41.66,
        "expectedProfit": 615.40,
        "is_adjustment": False
    }
    trade = {
        "symbol": "USD/JPY",
        "intervalo": "15min",
        "accountName": "JAIME",
        "size": 41000, # Large size triggers split orders string: "41,000.00 (5 x 8,000.00)"
        "margin_used": 102.50
    }
    
    buf = generateSignalCard("QTrend", signal, trade)
    outputPath = os.path.join(projectRoot, "test_overlap_fixed.png")
    with open(outputPath, "wb") as f:
        f.write(buf.read())
    print(f"Overlap test image saved to: {outputPath}")

if __name__ == "__main__":
    testOverlapSignal()

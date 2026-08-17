import requests
import json

def test():
    url = "http://localhost:8000/api/v1/ratio/EUR%2FUSD?pairB=GBP%2FUSD&days=7&tf=1h"
    print("Invocando endpoint:", url)
    try:
        r = requests.get(url)
        res = r.json()
        print("Success:", res.get("success"))
        if res.get("success"):
            print("Keys en el JSON de respuesta:", res.keys())
            history = res.get("history")
            print("Total elementos en history:", len(history))
            if history:
                print("Primer elemento en history:")
                print(json.dumps(history[0], indent=2))
        else:
            print("Error del backend:", res.get("error"))
    except Exception as e:
        print("Error de conexión:", e)

if __name__ == '__main__':
    test()

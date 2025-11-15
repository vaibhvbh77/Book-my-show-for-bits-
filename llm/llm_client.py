import requests, sys, json

def ask(q):
    url = "http://127.0.0.1:8000/ask"
    r = requests.post(url, json={"q": q}, timeout=5)
    r.raise_for_status()
    print(json.dumps(r.json(), indent=2))

if __name__ == "__main__":
    q = " ".join(sys.argv[1:]) or "why did my booking fail?"
    ask(q)

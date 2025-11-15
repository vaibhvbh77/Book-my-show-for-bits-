# tools/stress_test.py
# Robust stress tester:
# - automatically logs in and refreshes token on auth failures
# - uses frontend /connected to discover leader (if available)
# - follows NOT_LEADER hints and retries
# - prints detailed summary

import time, threading, random, statistics, json, requests
import grpc, booking_pb2, booking_pb2_grpc
from collections import defaultdict

# CONFIG — tune for your environment
N_THREADS = 50
DURATION = 30
PEERS = ["127.0.0.1:60051", "127.0.0.1:60052", "127.0.0.1:60053"]
FRONTEND_CONNECTED = "http://127.0.0.1:8080/connected"
FRONTEND_LOGIN = "http://127.0.0.1:8080/login"
LOGIN_USER = "vaibhav"
LOGIN_PASS = "mysecret"

WORKLOAD = [("get", 0.8), ("reserve", 0.2)]

# runtime globals
stop_flag = False
results = defaultdict(list)
errors = defaultdict(int)
token_lock = threading.Lock()
TOKEN = None
TOKEN_TS = 0
TOKEN_TTL = 3600  # assume 1h, will re-login on auth fail

# ---- helpers ----
def create_stub(addr):
    ch = grpc.insecure_channel(addr)
    return booking_pb2_grpc.ClientAPIStub(ch)

def try_get_leader_from_frontend():
    try:
        r = requests.get(FRONTEND_CONNECTED, timeout=1.0)
        j = r.json()
        # prefer cached_leader / last_used_target then env_leader
        for key in ("cached_leader","last_used_target","env_leader"):
            val = j.get(key)
            if val:
                return val
        # fallback: scan peers for is_leader
        peers = j.get("peers", {})
        for addr, info in peers.items():
            if info.get("is_leader"):
                return addr
    except Exception:
        pass
    return None

def login_frontend():
    try:
        r = requests.post(FRONTEND_LOGIN, json={"username": LOGIN_USER, "password": LOGIN_PASS}, timeout=2)
        j = r.json()
        return j.get("token")
    except Exception:
        return None

def login_rpc_any():
    for p in PEERS:
        try:
            stub = create_stub(p)
            resp = stub.Login(booking_pb2.LoginRequest(username=LOGIN_USER, password=LOGIN_PASS), timeout=2)
            if getattr(resp, "status", 1) == 0:
                return getattr(resp, "token", None)
        except Exception:
            continue
    return None

def ensure_token(force=False):
    global TOKEN, TOKEN_TS
    with token_lock:
        if TOKEN and not force:
            # if token younger than 55 minutes, keep it (avoid frequent logins)
            if time.time() - TOKEN_TS < (TOKEN_TTL - 300):
                return TOKEN
        # try frontend login first (preferred)
        t = login_frontend()
        if t:
            TOKEN = t; TOKEN_TS = time.time(); return TOKEN
        # else direct RPC login
        t = login_rpc_any()
        if t:
            TOKEN = t; TOKEN_TS = time.time(); return TOKEN
        return None

def _leader_hint_from_msg(msg):
    if not msg or "NOT_LEADER:" not in msg:
        return None
    hint = msg.split("NOT_LEADER:",1)[1].strip()
    # if hint already looks like addr, return it
    if ":" in hint:
        return hint
    # map simple node id -> address by scanning peers
    for p in PEERS:
        if hint in p or p.endswith(hint):
            return p
    return None

def do_get(node, token):
    try:
        stub = create_stub(node)
        r = stub.GetSeats(booking_pb2.GetRequest(token=token), timeout=3)
        return True, getattr(r, "status", 1)
    except Exception as e:
        return False, str(e)

def do_reserve(node, token, seat):
    try:
        stub = create_stub(node)
        r = stub.ReserveSeat(booking_pb2.ReserveRequest(token=token, seat_id=seat, client_id="stress"), timeout=5)
        return True, getattr(r, "code", -1), getattr(r, "msg", "")
    except Exception as e:
        return False, "reserve_rpc_error", str(e)

# worker
def worker(idx):
    global stop_flag
    local_token = ensure_token()
    if not local_token:
        errors["no_token_at_start"] += 1

    while not stop_flag:
        op = random.random()
        p = 0
        op_name = None
        for k,prob in WORKLOAD:
            p += prob
            if op <= p:
                op_name = k; break

        # choose leader hint first
        leader = try_get_leader_from_frontend() or random.choice(PEERS)

        if op_name == "get":
            ok, info = do_get(leader, local_token or "")
            if ok:
                results["get_ok"].append(1 if info==0 else 0)
                results["get_count"].append(1)
            else:
                errors["get_rpc"] += 1
        else:  # reserve
            seat = f"S{random.randint(1,10)}"
            ok, code_or_err, msg = do_reserve(leader, local_token or "", seat)
            if not ok:
                # RPC-level issue -> count and continue
                errors[code_or_err] += 1
                continue

            # rpc returned a structured reply (code,msg)
            code = code_or_err
            if code == 0:
                results["res_ok"].append(1)
                results["res_count"].append(1)
                continue

            if code == 5:
                # UNAUTHENTICATED -> refresh token and retry once
                errors["res_5_before_refresh"] += 1
                local_token = ensure_token(force=True)
                if not local_token:
                    errors["res_5_no_token_after_refresh"] += 1
                    continue
                ok2, code2, msg2 = do_reserve(leader, local_token, seat)
                if ok2 and code2 == 0:
                    results["res_ok"].append(1)
                    results["res_count"].append(1)
                else:
                    errors[f"res_{code2}"] += 1
                continue

            if code == 1:
                # NOT_LEADER -> follow hint if present
                hint = _leader_hint_from_msg(msg)
                errors["res_1_before_hint"] += 1
                if hint:
                    ok2, code2, msg2 = do_reserve(hint, local_token or "", seat)
                    if ok2 and code2 == 0:
                        results["res_ok"].append(1)
                        results["res_count"].append(1)
                    else:
                        errors[f"res_{code2}"] += 1
                else:
                    errors["res_1_no_hint"] += 1
                continue

            # any other non-success code
            errors[f"res_{code}"] += 1

if __name__ == "__main__":
    print("Stress test starting:", N_THREADS, "threads for", DURATION, "s")
    tkn = ensure_token()
    if not tkn:
        print("WARN: token not obtained at start — reserves likely to fail until login works")

    threads = []
    for i in range(N_THREADS):
        t = threading.Thread(target=worker, args=(i,), daemon=True)
        t.start()
        threads.append(t)

    start = time.time()
    try:
        while time.time() - start < DURATION:
            time.sleep(1)
    finally:
        stop_flag = True
        time.sleep(0.5)

    def safe_pct(ls, p):
        if not ls: return None
        try:
            return statistics.quantiles(ls, n=100)[int(p)-1] if len(ls) >= 100 else sorted(ls)[max(0,int(len(ls)*p/100)-1)]
        except Exception:
            return None

    print("\n=== SUMMARY ===")
    print("Get requests:", sum(results.get("get_count",[])))
    if results.get("get_count"):
        print("Get success rate:", sum(results.get("get_ok",[]))/max(1,len(results.get("get_ok",[]))))
    print("Reserve attempts:", sum(results.get("res_count",[])) + sum([v for k,v in errors.items() if k.startswith("res_")]))
    print("Reserve success rate:", sum(results.get("res_ok",[]))/max(1, (sum(results.get("res_count",[])) + sum([v for k,v in errors.items() if k.startswith("res_")]))))
    print("Errors summary:", dict(errors))

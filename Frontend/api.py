# frontend/api.py
# Frontend shim for the ticket booking demo.
# - Cleaner /connected endpoint
# - Short gRPC error messages
# - In-memory cached leader + last-used target + hint following
# - Same client endpoints: /login, /get, /reserve, /cancel, /ask

from flask import Flask, request, jsonify, send_from_directory
import grpc
import booking_pb2, booking_pb2_grpc
import os, sys, time, threading

# allow imports from project root (so booking_pb2 can be found)
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

APP = Flask(__name__, static_folder="static", static_url_path="")

DEFAULT_LEADER = os.environ.get("LEADER_ADDR", "127.0.0.1:60051")
PEERS_SETTING = os.environ.get(
    "PEERS",
    "node1=127.0.0.1:60051,node2=127.0.0.1:60052,node3=127.0.0.1:60053",
)
PEERS_MAP = dict(p.split("=", 1) for p in PEERS_SETTING.split(","))

# ---------- runtime caches ----------
# cached leader address (best known)
CACHED_LEADER = os.environ.get("LEADER_ADDR", DEFAULT_LEADER)
# last address the UI used for a successful call
LAST_USED_TARGET = None
# store last NOT_LEADER hints we saw {addr: hinted_addr}
LAST_HINTS = {}  # e.g. {"127.0.0.1:60052": "127.0.0.1:60053"}

CACHE_LOCK = threading.Lock()

# ---------- helper grpc wrappers ----------
def create_client_stub(address=None):
    addr = address or CACHED_LEADER or DEFAULT_LEADER
    ch = grpc.insecure_channel(addr)
    return booking_pb2_grpc.ClientAPIStub(ch), addr

def sanitize_grpc_error(exc):
    """
    Return a short, user-friendly single-line description for a grpc.RpcError.
    """
    if not isinstance(exc, grpc.RpcError):
        s = str(exc)
        return s.splitlines()[0][:240]
    try:
        code = exc.code()
        details = exc.details() or ""
    except Exception:
        s = str(exc)
        return s.splitlines()[0][:240]
    if code == grpc.StatusCode.UNAVAILABLE:
        return "DOWN (connection refused / unavailable)"
    if code == grpc.StatusCode.DEADLINE_EXCEEDED:
        return "DOWN (timeout)"
    if code == grpc.StatusCode.UNAUTHENTICATED:
        return "UNAUTHENTICATED"
    if code == grpc.StatusCode.PERMISSION_DENIED:
        return "PERMISSION_DENIED"
    d = details.split("\n")[0][:200]
    return f"{code.name}: {d}" if d else f"{code.name}"

def probe_node(addr, timeout=1.0):
    """
    Lightweight probe to check if node is reachable. Uses GetSeats (no auth)
    because it's a safe read RPC. Returns (alive: bool, short_msg: str).
    """
    try:
        ch = grpc.insecure_channel(addr)
        stub = booking_pb2_grpc.ClientAPIStub(ch)
        req = booking_pb2.GetRequest(token="")  # unauth token -> reachable
        _ = stub.GetSeats(req, timeout=timeout)
        return True, "UP"
    except grpc.RpcError as e:
        return False, sanitize_grpc_error(e)
    except Exception as e:
        return False, str(e)[:200]

def _call_reserve_on(address, token, seat_id, client_id):
    try:
        ch = grpc.insecure_channel(address)
        stub = booking_pb2_grpc.ClientAPIStub(ch)
        response = stub.ReserveSeat(
            booking_pb2.ReserveRequest(token=token, seat_id=seat_id, client_id=client_id),
            timeout=5
        )
        return response, None
    except grpc.RpcError as exc:
        return None, sanitize_grpc_error(exc)
    except Exception as exc:
        return None, str(exc)[:240]

def _call_cancel_on(address, token, seat_id):
    try:
        ch = grpc.insecure_channel(address)
        stub = booking_pb2_grpc.ClientAPIStub(ch)
        response = stub.CancelSeat(booking_pb2.CancelRequest(token=token, seat_id=seat_id), timeout=5)
        return response, None
    except grpc.RpcError as exc:
        return None, sanitize_grpc_error(exc)
    except Exception as exc:
        return None, str(exc)[:240]

def _leader_hint_from_msg(msg_or_token):
    """Support different hint styles:
       - "NOT_LEADER:127.0.0.1:60052"
       - "127.0.0.1:60053"
       - "node2=127.0.0.1:60052"
    """
    if not msg_or_token:
        return None
    s = str(msg_or_token).strip()
    if s.startswith("NOT_LEADER:"):
        s = s.split("NOT_LEADER:", 1)[1].strip()
    # if already a peer value, return it
    if s in PEERS_MAP.values():
        return s
    # if hint provided as pid=addr
    if "=" in s:
        try:
            pid, addr = s.split("=", 1)
            addr = addr.strip()
            if ":" in addr:
                return addr
        except Exception:
            pass
    # if looks like host:port, return as-is
    if ":" in s:
        return s
    # unknown format -> None
    return None

# ---------- static file routes ----------
@APP.route("/")
def serve_index():
    return send_from_directory(APP.static_folder, "index.html")

@APP.route("/login.html")
def serve_login():
    return send_from_directory(APP.static_folder, "login.html")

@APP.route("/<path:fn>")
def serve_static(fn):
    safe = os.path.normpath(fn)
    if safe.startswith(".."):
        return "Invalid path", 400
    return send_from_directory(APP.static_folder, safe)

# ---------- main API endpoints ----------

@APP.route("/login", methods=["POST"])
def rpc_login():
    global LAST_USED_TARGET, CACHED_LEADER
    payload = request.json or {}
    user = payload.get("username", "")
    pw = payload.get("password", "")
    try:
        # try cached leader first, then peers
        candidates = []
        with CACHE_LOCK:
            if CACHED_LEADER:
                candidates.append(CACHED_LEADER)
        candidates.extend([addr for addr in PEERS_MAP.values() if addr not in candidates])
        last_err = None
        for addr in candidates:
            try:
                ch = grpc.insecure_channel(addr)
                stub = booking_pb2_grpc.ClientAPIStub(ch)
                resp = stub.Login(booking_pb2.LoginRequest(username=user, password=pw), timeout=3)
                status = getattr(resp, "status", 1)
                token = getattr(resp, "token", "") or ""
                if status == 0:
                    with CACHE_LOCK:
                        LAST_USED_TARGET = addr
                        CACHED_LEADER = addr
                    return jsonify({"status": status, "token": token})
                # follower hint: some nodes return status=2 and token contains hint (with or w/o NOT_LEADER:)
                if status == 2:
                    hint = _leader_hint_from_msg(token)
                    if hint:
                        with CACHE_LOCK:
                            LAST_HINTS[addr] = hint
                            CACHED_LEADER = hint
                        # try suggested leader immediately
                        try:
                            ch2 = grpc.insecure_channel(hint)
                            stub2 = booking_pb2_grpc.ClientAPIStub(ch2)
                            resp2 = stub2.Login(booking_pb2.LoginRequest(username=user, password=pw), timeout=3)
                            if getattr(resp2, "status", 1) == 0:
                                with CACHE_LOCK:
                                    LAST_USED_TARGET = hint
                                    CACHED_LEADER = hint
                                return jsonify({"status": 0, "token": getattr(resp2, "token", "")})
                            else:
                                last_err = "login failed on hinted leader"
                                # continue trying other candidates
                        except grpc.RpcError as exc2:
                            last_err = sanitize_grpc_error(exc2)
                        except Exception as exc2:
                            last_err = str(exc2)[:240]
                else:
                    last_err = "login failed"
            except grpc.RpcError as exc:
                last_err = sanitize_grpc_error(exc)
            except Exception as exc:
                last_err = str(exc)[:240]
        return jsonify({"error": last_err or "no node reachable"}), 500
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

@APP.route("/get", methods=["POST"])
def rpc_get():
    global LAST_USED_TARGET, CACHED_LEADER
    payload = request.json or {}
    token = payload.get("token", "")
    # Try last used -> cached leader -> all peers
    candidates = []
    if LAST_USED_TARGET:
        candidates.append(LAST_USED_TARGET)
    with CACHE_LOCK:
        if CACHED_LEADER and CACHED_LEADER not in candidates:
            candidates.append(CACHED_LEADER)
    candidates.extend([addr for addr in PEERS_MAP.values() if addr not in candidates])

    last_err = None
    for addr in candidates:
        try:
            ch = grpc.insecure_channel(addr)
            stub = booking_pb2_grpc.ClientAPIStub(ch)
            resp = stub.GetSeats(booking_pb2.GetRequest(token=token), timeout=3)
            seats = [{"seat_id": s.seat_id, "reserved": s.reserved, "reserved_by": s.reserved_by} for s in resp.seats]
            with CACHE_LOCK:
                LAST_USED_TARGET = addr
                CACHED_LEADER = addr
            return jsonify({"status": getattr(resp, "status", 0), "seats": seats})
        except grpc.RpcError as exc:
            last_err = sanitize_grpc_error(exc)
        except Exception as exc:
            last_err = str(exc)[:240]
    return jsonify({"error": last_err or "no node reachable"}), 500

@APP.route("/reserve", methods=["POST"])
def rpc_reserve():
    global LAST_USED_TARGET, CACHED_LEADER
    payload = request.json or {}
    token = payload.get("token", "")
    seat = payload.get("seat_id", "")
    client_id = payload.get("client_id", "web-user")

    tried = set()
    with CACHE_LOCK:
        start_leader = CACHED_LEADER or DEFAULT_LEADER

    # try cached leader first
    resp, err = _call_reserve_on(start_leader, token, seat, client_id)
    tried.add(start_leader)
    if resp is not None:
        code = getattr(resp, "code", None)
        msg = (getattr(resp, "msg", "") or "") or ""
        if code == 1 and "NOT_LEADER:" in msg or code == 1 and "NOT_LEADER" in msg:
            hint_addr = _leader_hint_from_msg(msg)
            if hint_addr and hint_addr not in tried:
                with CACHE_LOCK:
                    LAST_HINTS[start_leader] = hint_addr
                r2, e2 = _call_reserve_on(hint_addr, token, seat, client_id)
                tried.add(hint_addr)
                if r2 is not None:
                    if getattr(r2, "code", None) == 0:
                        with CACHE_LOCK:
                            CACHED_LEADER = hint_addr
                            LAST_USED_TARGET = hint_addr
                    return jsonify({"code": getattr(r2, "code", None), "msg": getattr(r2, "msg", None)})
        else:
            if code == 0:
                with CACHE_LOCK:
                    LAST_USED_TARGET = start_leader
                    CACHED_LEADER = start_leader
            return jsonify({"code": code, "msg": msg})

    # try all peers
    last_err = None
    for pid, addr in PEERS_MAP.items():
        if addr in tried:
            continue
        r, e = _call_reserve_on(addr, token, seat, client_id)
        tried.add(addr)
        if r is not None:
            code = getattr(r, "code", None)
            msg = (getattr(r, "msg", "") or "") or ""
            if code == 1 and "NOT_LEADER" in msg:
                hint_addr = _leader_hint_from_msg(msg)
                if hint_addr and hint_addr not in tried:
                    with CACHE_LOCK:
                        LAST_HINTS[addr] = hint_addr
                    r2, e2 = _call_reserve_on(hint_addr, token, seat, client_id)
                    tried.add(hint_addr)
                    if r2 is not None:
                        if getattr(r2, "code", None) == 0:
                            with CACHE_LOCK:
                                CACHED_LEADER = hint_addr
                                LAST_USED_TARGET = hint_addr
                        return jsonify({"code": getattr(r2, "code", None), "msg": getattr(r2, "msg", None)})
            else:
                if code == 0:
                    with CACHE_LOCK:
                        LAST_USED_TARGET = addr
                        CACHED_LEADER = addr
                return jsonify({"code": code, "msg": msg})
        else:
            last_err = e
    return jsonify({"error": last_err or "no node reachable"}), 500

@APP.route("/cancel", methods=["POST"])
def rpc_cancel():
    global LAST_USED_TARGET, CACHED_LEADER
    payload = request.json or {}
    token = payload.get("token", "")
    seat = payload.get("seat_id", "")

    tried = set()
    with CACHE_LOCK:
        start_leader = CACHED_LEADER or DEFAULT_LEADER

    resp, err = _call_cancel_on(start_leader, token, seat)
    tried.add(start_leader)
    if resp is not None:
        code = getattr(resp, "code", None)
        msg = (getattr(resp, "msg", "") or "") or ""
        if code == 1 and "NOT_LEADER" in msg:
            hint_addr = _leader_hint_from_msg(msg)
            if hint_addr and hint_addr not in tried:
                with CACHE_LOCK:
                    LAST_HINTS[start_leader] = hint_addr
                r2, e2 = _call_cancel_on(hint_addr, token, seat)
                tried.add(hint_addr)
                if r2 is not None:
                    if getattr(r2, "code", None) == 0:
                        with CACHE_LOCK:
                            CACHED_LEADER = hint_addr
                            LAST_USED_TARGET = hint_addr
                    return jsonify({"code": getattr(r2, "code", None), "msg": getattr(r2, "msg", None)})
        else:
            if code == 0:
                with CACHE_LOCK:
                    LAST_USED_TARGET = start_leader
                    CACHED_LEADER = start_leader
            return jsonify({"code": code, "msg": msg})

    last_err = None
    for pid, addr in PEERS_MAP.items():
        if addr in tried:
            continue
        r, e = _call_cancel_on(addr, token, seat)
        tried.add(addr)
        if r is not None:
            code = getattr(r, "code", None)
            msg = (getattr(r, "msg", "") or "") or ""
            if code == 1 and "NOT_LEADER" in msg:
                hint_addr = _leader_hint_from_msg(msg)
                if hint_addr and hint_addr not in tried:
                    with CACHE_LOCK:
                        LAST_HINTS[addr] = hint_addr
                    r2, e2 = _call_cancel_on(hint_addr, token, seat)
                    tried.add(hint_addr)
                    if r2 is not None:
                        if getattr(r2, "code", None) == 0:
                            with CACHE_LOCK:
                                CACHED_LEADER = hint_addr
                                LAST_USED_TARGET = hint_addr
                        return jsonify({"code": getattr(r2, "code", None), "msg": getattr(r2, "msg", None)})
            else:
                if code == 0:
                    with CACHE_LOCK:
                        LAST_USED_TARGET = addr
                        CACHED_LEADER = addr
                return jsonify({"code": code, "msg": msg})
        else:
            last_err = e
    return jsonify({"error": last_err or "no node reachable"}), 500

# ---------- cluster /connected endpoint (clean) ----------
@APP.route("/connected", methods=["GET"])
def connected():
    """
    Returns succinct cluster info suitable for the UI:
    {
      cached_leader: "127.0.0.1:60051",
      env_leader: "...",
      last_used_target: "...",
      peers: { ... }
    }
    """
    token = request.args.get("token", "")
    results = {}
    threads = []
    lock = threading.Lock()

    def probe(addr):
        alive, short = probe_node(addr, timeout=0.8)
        with lock:
            results[addr] = {"alive": alive, "last_response": short, "last_seen": time.time() if alive else None}

    for addr in PEERS_MAP.values():
        t = threading.Thread(target=probe, args=(addr,), daemon=True)
        t.start()
        threads.append(t)
    for t in threads:
        t.join(timeout=1.0)

    with CACHE_LOCK:
        cached = CACHED_LEADER
        last_used = LAST_USED_TARGET
        hints = dict(LAST_HINTS)

    peers_out = {}
    for addr, info in results.items():
        is_leader = (addr == cached) or (addr in hints.values())
        peers_out[addr] = {
            "alive": info["alive"],
            "is_leader": is_leader,
            "last_response": info["last_response"],
            "last_seen": info["last_seen"],
        }

    return jsonify({
        "cached_leader": cached,
        "env_leader": os.environ.get("LEADER_ADDR", DEFAULT_LEADER),
        "last_used_target": last_used,
        "hints": hints,
        "peers": peers_out
    })

@APP.route("/ask", methods=["POST"])
def proxy_ask():
    import requests
    q = (request.json or {}).get("q", "")
    try:
        r = requests.post("http://127.0.0.1:8000/ask", json={"q": q}, timeout=5)
        return jsonify(r.json())
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

# ---------- run ----------
if __name__ == "__main__":
    print("Frontend shim listening on http://127.0.0.1:8080")
    APP.run(host="127.0.0.1", port=8080, debug=True)
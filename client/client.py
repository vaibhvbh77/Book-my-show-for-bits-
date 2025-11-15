# client/client.py
# Robust client with automatic leader discovery / reconnection
import sys
import grpc
import requests
import time
import re
import booking_pb2, booking_pb2_grpc

# ---------- utils ----------
def make_channel(addr):
    return grpc.insecure_channel(addr)

def make_stub_and_chan(addr):
    ch = make_channel(addr)
    return booking_pb2_grpc.ClientAPIStub(ch), ch

def is_addr(s):
    return isinstance(s, str) and ":" in s and s.count(":") <= 2  # simple heuristic host:port

# ---------- Client class ----------
class AutoClient:
    def __init__(self, addrs):
        if not addrs:
            raise ValueError("need at least one addr")
        # unique order-preserving
        seen = set()
        self.peers = []
        for a in addrs:
            if a not in seen:
                seen.add(a); self.peers.append(a)

        # runtime connection state
        self.current_addr = self.peers[0]
        self.leader_addr = None   # best known leader address (host:port)
        self.stub, self.chan = make_stub_and_chan(self.current_addr)
        self.token = ""
        self.client_id = "cli-user"
        # mapping pid->addr if server returns node-id hints
        self.pid_map = {}

    def _update_pid_map_from_hint(self, hint):
        if not hint:
            return
        hint = hint.strip()
        # support hints like "node2=127.0.0.1:60052" or "node2:127.0.0.1:60052"
        if "=" in hint:
            try:
                pid, addr = hint.split("=",1)
                pid = pid.strip(); addr = addr.strip()
                if is_addr(addr):
                    self.pid_map[pid] = addr
            except Exception:
                pass

    def _set_leader_from_hint(self, hint):
        if not hint:
            return False
        hint = hint.strip()
        # if hint looks like host:port
        if is_addr(hint):
            if hint != self.leader_addr:
                self.leader_addr = hint
                print(f"[client] leader hint -> {self.leader_addr}")
            return True
        # if hint looks like "node2=127.0.0.1:60052"
        if "=" in hint:
            parts = hint.split("=")
            if len(parts) == 2 and is_addr(parts[1].strip()):
                pid = parts[0].strip(); addr = parts[1].strip()
                self.pid_map[pid] = addr
                if addr != self.leader_addr:
                    self.leader_addr = addr
                    print(f"[client] leader hint -> {self.leader_addr}")
                return True
        # if hint is pid we may have mapping
        if hint in self.pid_map:
            addr = self.pid_map[hint]
            if addr != self.leader_addr:
                self.leader_addr = addr
                print(f"[client] leader hint -> {self.leader_addr}")
            return True
        return False

    def _switch_to(self, addr):
        """Switch stub/channel to addr. If successful set current and leader address."""
        try:
            stub, chan = make_stub_and_chan(addr)
            # replace stub and channel (don't explicitly close old channel; gc will handle)
            self.stub, self.chan = stub, chan
            prev = self.current_addr
            self.current_addr = addr
            if self.leader_addr != addr:
                self.leader_addr = addr
                print(f"[client] switched -> {addr} (now treating as leader)")
            else:
                # still update current if only current changed
                print(f"[client] switched -> {addr}")
            return True
        except Exception as e:
            return False

    def _try_all_peers(self, timeout=1.0):
        """
        Probe peers in order, switch to first that responds, return that addr or None.
        Also sets leader_addr to the probed addr (so prompt stays consistent).
        """
        for addr in self.peers:
            try:
                stub, chan = make_stub_and_chan(addr)
                # quick probe: GetSeats with empty token to check connectivity (short timeout)
                stub.GetSeats(booking_pb2.GetRequest(token=""), timeout=timeout)
                self.stub, self.chan = stub, chan
                prev = self.current_addr
                self.current_addr = addr
                if self.leader_addr != addr:
                    self.leader_addr = addr
                    print(f"[client] probe -> switched to {addr} (set leader)")
                else:
                    print(f"[client] probe -> switched to {addr}")
                return addr
            except Exception:
                continue
        return None

    def _method_name(self, bound_fn):
        """
        Robustly infer the RPC method name from a bound method object.
        Returns a string like 'GetSeats', or None if unknown.
        """
        # common case: bound method with __func__
        try:
            if hasattr(bound_fn, "__func__") and hasattr(bound_fn.__func__, "__name__"):
                return bound_fn.__func__.__name__
            if hasattr(bound_fn, "__name__"):
                return bound_fn.__name__
            # fallback: try repr parsing
            r = repr(bound_fn)
            m = re.search(r"(?:ClientAPIStub\.|ClientAPIServicer\.)([A-Za-z_0-9]+)", r)
            if m:
                return m.group(1)
            m2 = re.search(r"bound method .*?\.([A-Za-z_0-9]+) of", r)
            if m2:
                return m2.group(1)
        except Exception:
            pass
        return None

    def _call_rpc(self, fn, *args, allow_leader_hint=True, retry_on_unavailable=True):
        """
        fn: bound method like self.stub.GetSeats
        Returns (ok, result_or_exception)
        On success, also mark leader_addr = current_addr (because client connects to leader)
        """
        method_name = self._method_name(fn)

        # Try current channel first
        try:
            res = fn(*args)
            # success: consider current connected node as leader (user requested this behavior)
            prev = self.leader_addr
            self.leader_addr = self.current_addr
            if prev != self.leader_addr:
                print(f"[client] leader updated -> {self.leader_addr}")
            return True, res
        except grpc.RpcError as e:
            code = e.code()
            # CONNECTIVITY / transient errors: attempt retries to leader or peers
            if retry_on_unavailable and code in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.CANCELLED, grpc.StatusCode.UNKNOWN):
                # 1) try switching to known leader_addr (if different)
                if self.leader_addr and self.leader_addr != self.current_addr:
                    if self._switch_to(self.leader_addr):
                        try:
                            if method_name and hasattr(self.stub, method_name):
                                method = getattr(self.stub, method_name)
                                res = method(*args)
                                prev = self.leader_addr
                                self.leader_addr = self.current_addr
                                if prev != self.leader_addr:
                                    print(f"[client] leader updated -> {self.leader_addr}")
                                return True, res
                        except Exception:
                            pass
                # 2) probe all peers and try the found one
                addr = self._try_all_peers(timeout=1.0)
                if addr:
                    if method_name and hasattr(self.stub, method_name):
                        try:
                            method = getattr(self.stub, method_name)
                            res = method(*args)
                            prev = self.leader_addr
                            self.leader_addr = self.current_addr
                            if prev != self.leader_addr:
                                print(f"[client] leader updated -> {self.leader_addr}")
                            return True, res
                        except Exception as e2:
                            return False, e2
                    else:
                        return False, RuntimeError("Cannot determine method for retry")
            # Not handled or retries exhausted
            return False, e

    # ---------- high-level operations ----------
    def login(self, username, password):
        ok, r = self._call_rpc(self.stub.Login, booking_pb2.LoginRequest(username=username, password=password))
        if not ok:
            # connection error; probe peers once
            addr = self._try_all_peers(timeout=1.0)
            if addr:
                ok2, r2 = self._call_rpc(self.stub.Login, booking_pb2.LoginRequest(username=username, password=password))
                if ok2:
                    r = r2
                else:
                    print("Login retry RPC error:", r2)
                    return False
            else:
                print("Login RPC error:", r)
                return False

        status = getattr(r, "status", 1)
        token = getattr(r, "token", "")
        # server might encode NOT_LEADER in token field like "NOT_LEADER:127.0.0.1:60052"
        if status != 0:
            if token and isinstance(token, str) and token.startswith("NOT_LEADER:"):
                hint = token.split(":",1)[1]
                # try to parse hints and switch
                if ":" in hint or "=" in hint:
                    # could be pid=addr or addr
                    if self._set_leader_from_hint(hint):
                        # switch to leader and retry
                        if self._switch_to(self.leader_addr):
                            ok2, r2 = self._call_rpc(self.stub.Login, booking_pb2.LoginRequest(username=username, password=password))
                            if ok2 and getattr(r2,"status",1)==0:
                                self.token = r2.token
                                print("Logged in (leader), token:", self.token)
                                return True
                            else:
                                print("Retry login failed:", r2 if ok2 else "rpc error")
                                return False
                # as fallback, if hint is pid without mapping, try all peers
                addr = self._try_all_peers(timeout=1.0)
                if addr:
                    ok2, r2 = self._call_rpc(self.stub.Login, booking_pb2.LoginRequest(username=username, password=password))
                    if ok2 and getattr(r2,"status",1)==0:
                        self.token = r2.token
                        print("Logged in (after probing), token:", self.token)
                        return True
                print("Login failed on this node; server said follower.")
                return False
            else:
                return False
        # status == 0 -> success
        self.token = token
        # mark connected as leader for UI consistency
        prev = self.leader_addr
        self.leader_addr = self.current_addr
        if prev != self.leader_addr:
            print(f"[client] leader updated -> {self.leader_addr}")
        return True

    def get_seats(self):
        ok, r = self._call_rpc(self.stub.GetSeats, booking_pb2.GetRequest(token=self.token))
        if not ok:
            print("GetSeats RPC error:", r)
            return None
        if getattr(r, "status", 1) != 0:
            print("GetSeats returned error status:", getattr(r,"status",None))
            return None
        return r.seats

    def reserve(self, seat):
        ok, r = self._call_rpc(self.stub.ReserveSeat, booking_pb2.ReserveRequest(token=self.token, seat_id=seat, client_id=self.client_id))
        if not ok:
            print("Reserve RPC error:", r)
            return None
        code = getattr(r, "code", None)
        msg = getattr(r, "msg", "") or ""
        if code == 1 and isinstance(msg, str) and msg.startswith("NOT_LEADER:"):
            hint = msg.split(":",1)[1]
            if self._set_leader_from_hint(hint) or is_addr(hint):
                if is_addr(hint):
                    self.leader_addr = hint
                print("Server says not leader. Leader id/addr:", hint)
                if self.leader_addr:
                    print("Retrying on leader address:", self.leader_addr)
                    if self._switch_to(self.leader_addr):
                        ok2, r2 = self._call_rpc(self.stub.ReserveSeat, booking_pb2.ReserveRequest(token=self.token, seat_id=seat, client_id=self.client_id))
                        if ok2:
                            print("Result:", getattr(r2,"code",None), getattr(r2,"msg",None))
                            return r2
            print("Leader hint could not be resolved. Please run client connecting to leader.")
            return r
        else:
            return r

    def cancel(self, seat):
        ok, r = self._call_rpc(self.stub.CancelSeat, booking_pb2.CancelRequest(token=self.token, seat_id=seat))
        if not ok:
            print("Cancel RPC error:", r)
            return None
        code = getattr(r, "code", None)
        msg = getattr(r, "msg", "") or ""
        if code == 1 and isinstance(msg, str) and msg.startswith("NOT_LEADER:"):
            hint = msg.split(":",1)[1]
            if self._set_leader_from_hint(hint) or is_addr(hint):
                if is_addr(hint):
                    self.leader_addr = hint
                print("Redirected to leader", hint)
                if self.leader_addr and self._switch_to(self.leader_addr):
                    ok2, r2 = self._call_rpc(self.stub.CancelSeat, booking_pb2.CancelRequest(token=self.token, seat_id=seat))
                    if ok2:
                        print(f"Result: code={getattr(r2,'code',None)} msg={getattr(r2,'msg',None)}")
                        return r2
            print("Cancel: leader hint unresolved.")
            return r
        else:
            return r

    def ask_llm(self, q):
        try:
            resp = requests.post("http://127.0.0.1:8000/ask", json={"q": q}, timeout=5)
            resp.raise_for_status()
            return resp.json().get("answer")
        except Exception as e:
            return f"LLM call failed: {e}"

# ---------- Interactive frontend ----------
def interactive(addrs):
    client = AutoClient(addrs)
    prompt_template = "[connected={connected} leader={leader}]> "

    print("Commands: login <name>, get, reserve <S#>, cancel <S#>, ask <question>, quit")
    while True:
        leader_display = client.leader_addr if client.leader_addr else client.current_addr
        prompt = prompt_template.format(connected=client.current_addr, leader=leader_display)
        try:
            text = input(prompt)
        except (EOFError, KeyboardInterrupt):
            print()
            break
        cmd = (text or "").strip().split()
        if not cmd:
            continue
        op = cmd[0].lower()

        if op == "quit":
            break

        if op == "login":
            name = cmd[1] if len(cmd) > 1 else input("Username: ")
            pw = input("Password: ")
            ok = client.login(name, pw)
            if ok:
                print("Logged in, token:", client.token)
            else:
                print("Login failed")

        elif op == "get":
            seats = client.get_seats()
            if seats is None:
                continue
            for s in seats:
                status = "reserved" if s.reserved else "free"
                print(s.seat_id, status, "by:"+s.reserved_by)

        elif op == "reserve":
            if len(cmd) < 2:
                print("usage: reserve S1")
                continue
            seat = cmd[1].upper()
            r = client.reserve(seat)
            if r is not None:
                print("Result:", getattr(r,"code",None), getattr(r,"msg",None))

        elif op == "cancel":
            if len(cmd) < 2:
                print("usage: cancel S1")
                continue
            seat = cmd[1].upper()
            r = client.cancel(seat)
            if r is not None:
                print("Result:", getattr(r,"code",None), getattr(r,"msg",None))

        elif op == "ask":
            q = " ".join(cmd[1:]) if len(cmd) > 1 else input("Question: ")
            ans = client.ask_llm(q)
            print("LLM:", ans)

        else:
            print("unknown command")

# ---------- run ----------
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: python client/client.py <addr1> [addr2] [addr3] ...")
        sys.exit(1)
    addrs = sys.argv[1:]
    interactive(addrs)

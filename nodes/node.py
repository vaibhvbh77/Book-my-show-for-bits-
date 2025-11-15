# ------------------ node.py (Version B2 – Clean, Stable, Dynamic Cluster) ------------------
# Fully working Raft (leader election + replication), 1-based indexing preserved.

import time
import json
import threading
import random
import sys
import os
import hashlib
import signal
from concurrent import futures
import grpc
import booking_pb2, booking_pb2_grpc


# ===============================================================
#  PERSISTENCE LAYER
# ===============================================================
class Persist:
    def __init__(self, filename):
        self.fn = filename
        # Attempt load; if missing or invalid create default state and persist
        try:
            with open(self.fn, "r") as f:
                data = json.load(f)
            # safe read of components
            self.log = data.get("log") if isinstance(data.get("log"), list) else []
            # seats must always be a dict with S1..S10 — recreate if missing or malformed
            seats_from_file = data.get("seats")
            if not isinstance(seats_from_file, dict) or len(seats_from_file) != 10:
                self.seats = {f"S{i}": {"reserved": False, "by": ""} for i in range(1, 11)}
            else:
                # normalize seat entries (ensure keys and fields exist)
                seats = {}
                for i in range(1, 11):
                    sid = f"S{i}"
                    info = seats_from_file.get(sid, {})
                    reserved = bool(info.get("reserved", False))
                    by = info.get("by", "") or ""
                    seats[sid] = {"reserved": reserved, "by": by}
                self.seats = seats

            self.sessions = data.get("sessions") if isinstance(data.get("sessions"), dict) else {}
            self.users = data.get("users") if isinstance(data.get("users"), dict) else {}
            self.currentTerm = int(data.get("currentTerm", 0)) if data.get("currentTerm") is not None else 0
            self.votedFor = data.get("votedFor", None)
            # If seats were missing and we created default, persist immediately so file is consistent
            self._persist()
        except Exception:
            # any load error -> initialize clean defaults and persist
            self.log = []
            self.seats = {f"S{i}": {"reserved": False, "by": ""} for i in range(1, 11)}
            self.sessions = {}
            self.users = {}
            self.currentTerm = 0
            self.votedFor = None
            self._persist()

    def _persist(self):
        # write atomic-ish by writing to temp then rename (reduces partial writes)
        tmp = self.fn + ".tmp"
        with open(tmp, "w") as f:
            json.dump({
                "log": self.log,
                "seats": self.seats,
                "sessions": self.sessions,
                "users": self.users,
                "currentTerm": self.currentTerm,
                "votedFor": self.votedFor
            }, f, indent=2)
        try:
            os.replace(tmp, self.fn)
        except Exception:
            # fallback
            with open(self.fn, "w") as f:
                json.dump({
                    "log": self.log,
                    "seats": self.seats,
                    "sessions": self.sessions,
                    "users": self.users,
                    "currentTerm": self.currentTerm,
                    "votedFor": self.votedFor
                }, f, indent=2)

    # Log operations
    def append_log(self, entry):
        # Ensure index consistency: if entry has index > len(log)+1, fix it
        try:
            expected_index = len(self.log) + 1
            if entry.get("index") != expected_index:
                entry["index"] = expected_index
        except Exception:
            entry["index"] = len(self.log) + 1
        self.log.append(entry)
        self._persist()

    def apply_log(self, entry):
        cmd = entry.get("command")
        data_raw = entry.get("data", "{}")
        try:
            data = json.loads(data_raw)
        except Exception:
            data = {}

        if cmd == "reserve":
            seat = data.get("seat_id")
            user = data.get("client_id", "")
            if seat in self.seats and not self.seats[seat]["reserved"]:
                self.seats[seat]["reserved"] = True
                self.seats[seat]["by"] = user

        elif cmd == "cancel":
            seat = data.get("seat_id")
            user = data.get("client_id", "")
            if seat in self.seats and self.seats[seat]["reserved"] and self.seats[seat]["by"] == user:
                self.seats[seat]["reserved"] = False
                self.seats[seat]["by"] = ""

        elif cmd == "session":
            # session entry: expects {"token":..., "username":..., "expiry":...}
            token = data.get("token")
            username = data.get("username")
            expiry = data.get("expiry")
            if token and username:
                self.sessions[token] = {"username": username, "expiry": expiry}

        # Keep users block for any future user replication entries (not used here)
        self._persist()

    # User system
    def add_user(self, user, pw):
        salt = os.urandom(8).hex()
        pw_hash = hashlib.sha256((salt + pw).encode()).hexdigest()
        self.users[user] = {"salt": salt, "pw_hash": pw_hash}
        self._persist()

    def verify_user(self, user, pw):
        u = self.users.get(user)
        if not u:
            return False
        if "salt" in u and "pw_hash" in u:
            return hashlib.sha256((u["salt"] + pw).encode()).hexdigest() == u["pw_hash"]
        return False


# ===============================================================
#  NODE (RAFT + BOOKING API)
# ===============================================================
class NodeServicer(booking_pb2_grpc.ClientAPIServicer,
                   booking_pb2_grpc.RaftServicer):

    def __init__(self, node_id, peers):
        self.node_id = node_id
        self.peers = peers                  # list: [(pid, addr), ...]
        self.persist = Persist(f"node_{node_id}.json")

        self.lock = threading.Lock()

        # Reconstruct state by applying log (ensure seat state & sessions are consistent)
        for e in list(self.persist.log):
            try:
                self.persist.apply_log(e)
            except Exception:
                pass

        # Raft volatile state
        self.commitIndex = 0
        self.lastApplied = 0
        self.state = "follower"
        self.votes_received = 0

        # track current leader id (pid) when AppendEntries arrives
        self.currentLeader = None

        # this servicer's externally reachable address (set by serve())
        self.self_addr = None

        # Dynamic election timeout
        self.election_min = 0.8
        self.election_max = 1.4
        self._reset_election_timer()

        # Leader heartbeat
        self.heartbeat_interval = 0.25

        # Replication indices
        last_index = len(self.persist.log)
        self.nextIndex = {pid: last_index + 1 for pid, _ in self.peers}
        self.matchIndex = {pid: 0 for pid, _ in self.peers}

        # Thread control
        self._stop = False
        self._threads = []

        # Start election loop
        t = threading.Thread(target=self._election_loop, daemon=True)
        t.start()
        self._threads.append(t)


    # -----------------------------------------------------------
    # Election timer
    # -----------------------------------------------------------
    def _reset_election_timer(self):
        self.election_deadline = time.time() + random.uniform(self.election_min, self.election_max)

    def _election_loop(self):
        while not self._stop:
            time.sleep(0.05)
            if self.state != "leader" and time.time() >= self.election_deadline:
                self._start_election()

    # -----------------------------------------------------------
    # Start Election
    # -----------------------------------------------------------
    def _start_election(self):
        with self.lock:
            self.state = "candidate"
            self.persist.currentTerm += 1
            self.persist.votedFor = self.node_id
            self.persist._persist()

            term = self.persist.currentTerm
            self.votes_received = 1
            self._reset_election_timer()

            lastLogIndex = len(self.persist.log)
            lastLogTerm = self.persist.log[-1]["term"] if self.persist.log else 0

            print(f"[RAFT] {self.node_id} starts election term={term}")

        # Send RequestVote to all peers
        for pid, addr in self.peers:
            if pid == self.node_id:
                continue
            try:
                chan = grpc.insecure_channel(addr)
                stub = booking_pb2_grpc.RaftStub(chan)
                req = booking_pb2.RequestVoteArgs(
                    term=term,
                    candidateId=self.node_id,
                    lastLogIndex=lastLogIndex,
                    lastLogTerm=lastLogTerm
                )
                resp = stub.RequestVote(req, timeout=1)

                if resp.term > term:
                    with self.lock:
                        self.persist.currentTerm = resp.term
                        self.persist.votedFor = None
                        self.persist._persist()
                        self.state = "follower"
                        self._reset_election_timer()
                    return

                if resp.voteGranted:
                    self.votes_received += 1

            except Exception:
                pass

        # Check majority
        if self.votes_received >= (len(self.peers) // 2) + 1:
            with self.lock:
                self.state = "leader"
                last = len(self.persist.log)
                for pid, _ in self.peers:
                    self.nextIndex[pid] = last + 1
                    self.matchIndex[pid] = 0

            print(f"[RAFT] {self.node_id} becomes LEADER term={term}")

            t = threading.Thread(target=self._leader_heartbeat, daemon=True)
            t.start()
            self._threads.append(t)


    # -----------------------------------------------------------
    # Leader heartbeat sending (AppendEntries with empty entries)
    # -----------------------------------------------------------
    def _leader_heartbeat(self):
        while not self._stop and self.state == "leader":
            for pid, addr in self.peers:
                if pid == self.node_id:
                    continue
                try:
                    chan = grpc.insecure_channel(addr)
                    stub = booking_pb2_grpc.RaftStub(chan)

                    prevIndex = len(self.persist.log)          # 1-based: prevIndex == last index
                    prevTerm = self.persist.log[prevIndex - 1]["term"] if prevIndex > 0 else 0

                    req = booking_pb2.AppendEntriesArgs(
                        term=self.persist.currentTerm,
                        leaderId=self.node_id,
                        prevLogIndex=prevIndex,
                        prevLogTerm=prevTerm,
                        entries=[],              # heartbeat
                        leaderCommit=self.commitIndex
                    )
                    resp = stub.AppendEntries(req, timeout=1)

                    # if follower has higher term, step down
                    if getattr(resp, "term", 0) > self.persist.currentTerm:
                        with self.lock:
                            self.persist.currentTerm = resp.term
                            self.persist.votedFor = None
                            self.persist._persist()
                            self.state = "follower"
                            self._reset_election_timer()
                        print(f"[RAFT] {self.node_id} stepping down (higher term {resp.term})")
                        return

                except Exception:
                    # ignore transient network errors for heartbeat
                    pass

            time.sleep(self.heartbeat_interval)


    # -----------------------------------------------------------
    # Token helpers
    # -----------------------------------------------------------
    def _create_token(self, username, ttl=3600):
        token = f"tok-{username}-{int(time.time())}"
        expiry = time.time() + ttl
        # NOTE: this writes to local persisted sessions only when applied via log or when leader commits
        return token, expiry

    def _validate_token(self, token):
        if not token:
            return None
        s = self.persist.sessions.get(token)
        if not s:
            return None
        if s["expiry"] < time.time():
            # expired -> remove
            try:
                del self.persist.sessions[token]
                self.persist._persist()
            except Exception:
                pass
            return None
        return s["username"]


    # -----------------------------------------------------------
    # Replication helper — replicate entry to followers (1-based)
    # returns True if majority replicated
    # -----------------------------------------------------------
    def _replicate_to_followers(self, entry):
        """
        entry: dict with 'index' (1-based), 'term', 'command', 'data'
        """
        successes = 1  # leader itself
        target_index = entry["index"]

        # iterate peers and try to bring them up
        for pid, addr in self.peers:
            if pid == self.node_id:
                continue

            # start from nextIndex for that follower
            ni = self.nextIndex.get(pid, len(self.persist.log) + 1)  # ni is 1-based next index to send
            # try backtracking until success or fall off
            while ni > 0:
                # prevIndex is ni - 1 (could be 0 meaning no previous)
                prevIndex = ni - 1
                prevTerm = self.persist.log[prevIndex - 1]["term"] if prevIndex > 0 and len(self.persist.log) >= prevIndex else 0

                # prepare entries to send: from ni..end (1-based)
                entries_to_send = []
                for e in self.persist.log[ni - 1:]:
                    le = booking_pb2.LogEntry(index=e["index"], term=e["term"], command=e["command"], data=e["data"])
                    entries_to_send.append(le)

                try:
                    chan = grpc.insecure_channel(addr)
                    stub = booking_pb2_grpc.RaftStub(chan)
                    req = booking_pb2.AppendEntriesArgs(
                        term=self.persist.currentTerm,
                        leaderId=self.node_id,
                        prevLogIndex=prevIndex,
                        prevLogTerm=prevTerm,
                        entries=entries_to_send,
                        leaderCommit=self.commitIndex
                    )
                    resp = stub.AppendEntries(req, timeout=2)
                except Exception:
                    # network error — break out for this follower, leave nextIndex unchanged
                    break

                # higher term -> step down
                if getattr(resp, "term", 0) > self.persist.currentTerm:
                    with self.lock:
                        self.persist.currentTerm = resp.term
                        self.persist.votedFor = None
                        self.persist._persist()
                        self.state = "follower"
                        self._reset_election_timer()
                    return False

                if getattr(resp, "success", False):
                    # follower matched entries, update indices
                    match_idx = getattr(resp, "matchIndex", len(self.persist.log))
                    self.matchIndex[pid] = match_idx
                    self.nextIndex[pid] = match_idx + 1
                    successes += 1
                    break
                else:
                    # follower rejected — resp.matchIndex may contain last index it has (0 means none)
                    mi = getattr(resp, "matchIndex", None)
                    if isinstance(mi, int) and mi >= 0:
                        ni = mi + 1
                    else:
                        ni = max(1, ni - 1)
                    self.nextIndex[pid] = ni
                    # continue loop to try smaller ni

        # majority?
        return successes >= (len(self.peers) // 2) + 1


    # -----------------------------------------------------------
    # Helper: map a node-id (pid) to address if known
    # -----------------------------------------------------------
    def _addr_for_pid(self, pid):
        for p, a in self.peers:
            if p == pid:
                return a
        return None


    # -----------------------------------------------------------
    # Client RPC: Login
    # -----------------------------------------------------------
    def Login(self, request, context):
        username = (request.username or "guest").strip()
        password = getattr(request, "password", "") or ""

        # check password against local persisted users
        if not self.persist.verify_user(username, password):
            # status=1 -> invalid credentials
            return booking_pb2.LoginResponse(status=1, token="")

        # Only leader issues new sessions — followers reply with a NOT_LEADER hint
        with self.lock:
            if self.state != "leader":
                # compute best hint address: prefer known currentLeader pid->addr mapping
                hint_addr = None
                if getattr(self, "currentLeader", None):
                    hint_addr = self._addr_for_pid(self.currentLeader)
                # fallback to explicit servicer address provided at startup
                if not hint_addr and getattr(self, "self_addr", None):
                    hint_addr = self.self_addr
                # final fallback: first peer address in peers list
                if not hint_addr and self.peers:
                    hint_addr = self.peers[0][1]
                hint_addr = hint_addr or self.node_id
                # return status=2 to indicate follower and give canonical NOT_LEADER:<host:port>
                return booking_pb2.LoginResponse(status=2, token=f"NOT_LEADER:{hint_addr}")

            # leader: create token and replicate session creation
            token, expiry = self._create_token(username, ttl=3600)
            entry = {
                "index": len(self.persist.log) + 1,
                "term": self.persist.currentTerm,
                "command": "session",
                "data": json.dumps({"token": token, "username": username, "expiry": expiry})
            }
            # append locally (persisted)
            self.persist.append_log(entry)

        # replicate to followers
        ok = self._replicate_to_followers(entry)
        if not ok:
            # replication failed — return replication error
            return booking_pb2.LoginResponse(status=3, token="")

        # commit & apply locally
        with self.lock:
            self.commitIndex = entry["index"]
            while self.lastApplied < self.commitIndex:
                self.persist.apply_log(self.persist.log[self.lastApplied])
                self.lastApplied += 1

        return booking_pb2.LoginResponse(status=0, token=token)


    # -----------------------------------------------------------
    # Client RPC: GetSeats
    # -----------------------------------------------------------
    def GetSeats(self, request, context):
        if not self._validate_token(request.token):
            return booking_pb2.GetResponse(status=1, seats=[])
        seats = []
        for sid, info in sorted(self.persist.seats.items()):
            seats.append(booking_pb2.Seat(seat_id=sid, reserved=info["reserved"], reserved_by=info["by"]))
        return booking_pb2.GetResponse(status=0, seats=seats)


    # -----------------------------------------------------------
    # Client RPC: ReserveSeat
    # -----------------------------------------------------------
    def ReserveSeat(self, request, context):
        user = self._validate_token(request.token)
        if not user:
            return booking_pb2.ReserveReply(code=5, msg="UNAUTHENTICATED")

        with self.lock:
            # must be leader
            if self.state != "leader":
                # resolve canonical hint address
                hint_addr = None
                if getattr(self, "currentLeader", None):
                    hint_addr = self._addr_for_pid(self.currentLeader)
                if not hint_addr and getattr(self, "self_addr", None):
                    hint_addr = self.self_addr
                if not hint_addr and self.peers:
                    hint_addr = self.peers[0][1]
                hint_addr = hint_addr or self.node_id
                return booking_pb2.ReserveReply(code=1, msg=f"NOT_LEADER:{hint_addr}")

            seat_id = request.seat_id
            if seat_id not in self.persist.seats:
                return booking_pb2.ReserveReply(code=2, msg="NO_SUCH_SEAT")
            if self.persist.seats[seat_id]["reserved"]:
                return booking_pb2.ReserveReply(code=3, msg=f"ALREADY_RESERVED_BY:{self.persist.seats[seat_id]['by']}")

            # create entry with 1-based index
            entry = {
                "index": len(self.persist.log) + 1,
                "term": self.persist.currentTerm,
                "command": "reserve",
                "data": json.dumps({"seat_id": seat_id, "client_id": user})
            }
            # append to local log (persisted)
            self.persist.append_log(entry)

        # replicate (outside lock)
        ok = self._replicate_to_followers(entry)

        if not ok:
            return booking_pb2.ReserveReply(code=4, msg="REPLICATION_FAILED")

        # commit and apply
        with self.lock:
            self.commitIndex = entry["index"]
            # apply logs from lastApplied+1 up to commitIndex (1-based)
            while self.lastApplied < self.commitIndex:
                # lastApplied is 0-based count of applied entries; to index into log use lastApplied (0..)
                self.persist.apply_log(self.persist.log[self.lastApplied])
                self.lastApplied += 1

        return booking_pb2.ReserveReply(code=0, msg="RESERVED")


    # -----------------------------------------------------------
    # Client RPC: CancelSeat
    # -----------------------------------------------------------
    def CancelSeat(self, request, context):
        user = self._validate_token(request.token)
        if not user:
            return booking_pb2.Status(code=5, msg="UNAUTHENTICATED")

        with self.lock:
            if self.state != "leader":
                hint_addr = None
                if getattr(self, "currentLeader", None):
                    hint_addr = self._addr_for_pid(self.currentLeader)
                if not hint_addr and getattr(self, "self_addr", None):
                    hint_addr = self.self_addr
                if not hint_addr and self.peers:
                    hint_addr = self.peers[0][1]
                hint_addr = hint_addr or self.node_id
                return booking_pb2.Status(code=1, msg=f"NOT_LEADER:{hint_addr}")

            seat_id = request.seat_id
            if seat_id not in self.persist.seats:
                return booking_pb2.Status(code=2, msg="NO_SUCH_SEAT")
            if not self.persist.seats[seat_id]["reserved"]:
                return booking_pb2.Status(code=3, msg="SEAT_NOT_RESERVED")
            if self.persist.seats[seat_id]["by"] != user:
                return booking_pb2.Status(code=4, msg=f"NOT_OWNER:{self.persist.seats[seat_id]['by']}")

            entry = {
                "index": len(self.persist.log) + 1,
                "term": self.persist.currentTerm,
                "command": "cancel",
                "data": json.dumps({"seat_id": seat_id, "client_id": user})
            }
            self.persist.append_log(entry)

        ok = self._replicate_to_followers(entry)
        if not ok:
            return booking_pb2.Status(code=6, msg="REPLICATION_FAILED")

        with self.lock:
            self.commitIndex = entry["index"]
            while self.lastApplied < self.commitIndex:
                self.persist.apply_log(self.persist.log[self.lastApplied])
                self.lastApplied += 1

        return booking_pb2.Status(code=0, msg="CANCEL_OK")


    # -----------------------------------------------------------
    # Raft RPC: RequestVote (follower handles)
    # -----------------------------------------------------------
    def RequestVote(self, request, context):
        with self.lock:
            # reject if candidate term < currentTerm
            if request.term < self.persist.currentTerm:
                return booking_pb2.RequestVoteReply(term=self.persist.currentTerm, voteGranted=False)

            # if candidate term > currentTerm -> update
            if request.term > self.persist.currentTerm:
                self.persist.currentTerm = request.term
                self.persist.votedFor = None
                self.persist._persist()
                self.state = "follower"

            # if already voted for someone else this term, deny
            if self.persist.votedFor not in (None, request.candidateId):
                return booking_pb2.RequestVoteReply(term=self.persist.currentTerm, voteGranted=False)

            # up-to-date check (1-based indexing)
            local_last_index = len(self.persist.log)
            local_last_term = self.persist.log[-1]["term"] if self.persist.log else 0

            # candidate's log up-to-date?
            if (request.lastLogTerm < local_last_term) or (request.lastLogTerm == local_last_term and request.lastLogIndex < local_last_index):
                return booking_pb2.RequestVoteReply(term=self.persist.currentTerm, voteGranted=False)

            # grant vote
            self.persist.votedFor = request.candidateId
            self.persist._persist()
            self._reset_election_timer()
            return booking_pb2.RequestVoteReply(term=self.persist.currentTerm, voteGranted=True)


    # -----------------------------------------------------------
    # Raft RPC: AppendEntries (follower)
    # -----------------------------------------------------------
    def AppendEntries(self, request, context):
        with self.lock:
            # reject outdated leader
            if request.term < self.persist.currentTerm:
                return booking_pb2.AppendEntriesReply(term=self.persist.currentTerm, success=False, matchIndex=len(self.persist.log))

            # if leader's term newer, accept and convert to follower
            if request.term > self.persist.currentTerm:
                self.persist.currentTerm = request.term
                self.persist.votedFor = None
                self.persist._persist()
                self.state = "follower"

            # reset election timer because we heard from leader
            self._reset_election_timer()

            # update currentLeader info (pid)
            try:
                self.currentLeader = request.leaderId
            except Exception:
                self.currentLeader = None

            # prevLogIndex check (1-based)
            local_last_index = len(self.persist.log)
            if request.prevLogIndex > local_last_index:
                return booking_pb2.AppendEntriesReply(term=self.persist.currentTerm, success=False, matchIndex=local_last_index)

            # if prevLogIndex >0 then check term match
            if request.prevLogIndex > 0:
                local_prev_term = self.persist.log[request.prevLogIndex - 1]["term"]
                if local_prev_term != request.prevLogTerm:
                    # conflict: truncate to prevLogIndex - 1
                    self.persist.log = self.persist.log[:request.prevLogIndex - 1]
                    self.persist._persist()
                    return booking_pb2.AppendEntriesReply(term=self.persist.currentTerm, success=False, matchIndex=len(self.persist.log))

            # append entries (avoid duplicates)
            for e in request.entries:
                # if we already have an entry at e.index, check term
                if len(self.persist.log) >= e.index:
                    # compare term; if mismatch truncate and append
                    local_term = self.persist.log[e.index - 1]["term"]
                    if local_term != e.term:
                        self.persist.log = self.persist.log[:e.index - 1]
                        self.persist._persist()
                        self.persist.append_log({"index": e.index, "term": e.term, "command": e.command, "data": e.data})
                else:
                    # append new entry (preserving 1-based index)
                    self.persist.append_log({"index": e.index, "term": e.term, "command": e.command, "data": e.data})

            # apply entries up to leaderCommit
            while self.lastApplied < request.leaderCommit and self.lastApplied < len(self.persist.log):
                self.persist.apply_log(self.persist.log[self.lastApplied])
                self.lastApplied += 1

            return booking_pb2.AppendEntriesReply(term=self.persist.currentTerm, success=True, matchIndex=len(self.persist.log))


    # -----------------------------------------------------------
    # Shutdown — stop threads and join
    # -----------------------------------------------------------
    def shutdown(self, timeout=5.0):
        print(f"[SHUTDOWN] node {self.node_id} shutting down...")
        self._stop = True
        deadline = time.time() + timeout
        for t in self._threads:
            try:
                t.join(max(0, deadline - time.time()))
            except:
                pass
        print(f"[SHUTDOWN] node {self.node_id} threads joined")


# -----------------------------------------------------------
# Server bootstrap
# -----------------------------------------------------------
def serve(node_id, hostport, peers):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=12))
    servicer = NodeServicer(node_id, peers)
    # let servicer know its externally reachable address (used to create canonical NOT_LEADER hints)
    servicer.self_addr = hostport
    booking_pb2_grpc.add_ClientAPIServicer_to_server(servicer, server)
    booking_pb2_grpc.add_RaftServicer_to_server(servicer, server)
    server.add_insecure_port(hostport)
    server.start()
    print(f"[BOOT] Node {node_id} started at {hostport} state={servicer.state} peers={peers}")

    stop_event = threading.Event()

    def _signal(signum, frame):
        print(f"[SIGNAL] Node {node_id} got signal {signum}")
        stop_event.set()

    signal.signal(signal.SIGINT, _signal)
    signal.signal(signal.SIGTERM, _signal)

    try:
        while not stop_event.is_set():
            time.sleep(0.2)
    finally:
        try:
            servicer.shutdown(timeout=5)
        except:
            pass
        try:
            server.stop(0)
            server.wait_for_termination(timeout=5)
        except:
            pass
        print(f"[BOOT] Node {node_id} stopped")


# -----------------------------------------------------------
# Main
# -----------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("usage: python nodes/node.py <node_id> <host:port> [peerlist]")
        print("example: python nodes/node.py n1 127.0.0.1:60051 \"n1=127.0.0.1:60051,n2=127.0.0.1:60052,n3=127.0.0.1:60053\"")
        sys.exit(1)

    node_id = sys.argv[1]
    hostport = sys.argv[2]
    peers_arg = sys.argv[3] if len(sys.argv) > 3 else ""
    peers = []
    if peers_arg:
        for p in peers_arg.split(","):
            pid, addr = p.split("=")
            peers.append((pid, addr))
    if not any(p[0] == node_id for p in peers):
        peers.append((node_id, hostport))

    serve(node_id, hostport, peers)
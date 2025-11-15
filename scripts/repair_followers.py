# scripts/fix_followers.py
# Example usage:

import sys
import json
import time
import grpc
import booking_pb2, booking_pb2_grpc

def probe_match_index(target_addr, timeout=3):
    """
    Send a lightweight AppendEntries (no entries) to a follower to learn its current matchIndex.
    Returns an integer matchIndex on success, None on unreachable.
    """
    channel = grpc.insecure_channel(target_addr)
    stub = booking_pb2_grpc.RaftStub(channel)
    probe = booking_pb2.AppendEntriesArgs(
        term=0,
        leaderId="repair-probe",
        prevLogIndex=0,
        prevLogTerm=0,
        entries=[],
        leaderCommit=0
    )
    try:
        resp = stub.AppendEntries(probe, timeout=timeout)
        return getattr(resp, "matchIndex", 0)
    except Exception as exc:
        print(f"[PROBE ERROR] {target_addr} -> {exc}")
        return None

def send_log_entry(target_addr, entry_index, entry_term, entry_cmd, entry_data, timeout=5):
    """
    Push a single log entry to the follower using AppendEntries RPC.
    Returns the protobuf response on success, or the exception instance on failure.
    """
    channel = grpc.insecure_channel(target_addr)
    stub = booking_pb2_grpc.RaftStub(channel)
    log_entry = booking_pb2.LogEntry(
        index=entry_index,
        term=entry_term,
        command=entry_cmd,
        data=entry_data
    )
    args = booking_pb2.AppendEntriesArgs(
        term=0,
        leaderId="repair-pusher",
        prevLogIndex=entry_index - 1,
        prevLogTerm=0,
        entries=[log_entry],
        leaderCommit=entry_index
    )
    try:
        resp = stub.AppendEntries(args, timeout=timeout)
        return resp
    except Exception as exc:
        return exc

def load_leader_state(path):
    with open(path, "r") as fh:
        return json.load(fh)

def main():
    if len(sys.argv) < 3:
        print("Usage: fix_followers.py <leader_json> <follower_addr1> [follower_addr2 ...]")
        sys.exit(1)

    leader_file = sys.argv[1]
    follower_addrs = sys.argv[2:]

    leader_state = load_leader_state(leader_file)
    leader_log = leader_state.get("log", [])
    if not leader_log:
        print("Leader has no log entries. Nothing to sync.")
        return

    leader_len = len(leader_log)
    print(f"[INFO] Leader log length = {leader_len}")

    # Build a lookup by index for quick access
    index_map = {entry.get("index"): entry for entry in leader_log}

    for addr in follower_addrs:
        print(f"\n[REPAIR] Target follower: {addr}")
        match_idx = probe_match_index(addr)
        if match_idx is None:
            print(f"[SKIP] Cannot contact follower {addr}.")
            continue

        print(f"[INFO] Follower reported matchIndex = {match_idx}")
        # send missing entries from match_idx+1 .. leader_len
        for i in range(match_idx + 1, leader_len + 1):
            entry = index_map.get(i)
            if entry is None:
                print(f"[ERROR] Leader missing entry with index {i}; aborting this follower.")
                break
            print(f"  -> Pushing entry {i} (cmd={entry.get('command')})")
            resp = send_log_entry(
                addr,
                entry_index=entry.get("index"),
                entry_term=entry.get("term", 0),
                entry_cmd=entry.get("command"),
                entry_data=entry.get("data")
            )
            print(f"     Reply: {resp}")
            # tiny sleep so follower can persist and update state
            time.sleep(0.15)

        print(f"[DONE] Finished repairing {addr}")

if __name__ == "__main__":
    main()
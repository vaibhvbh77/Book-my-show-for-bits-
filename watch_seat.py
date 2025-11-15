# watch_seat.py
# Simple watcher that polls node state files and prints the status of a chosen seat.

import json
import time
from pathlib import Path

files = [Path("node_node1.json"), Path("node_node2.json"), Path("node_node3.json")]

seat_id = input("Enter seat to monitor (example: S5): ").strip().upper()
print(f"Monitoring {seat_id} — press Ctrl+C to exit\n")

try:
    while True:
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}]")
        for p in files:
            if not p.exists():
                print(f"{p.name}: file not found")
                continue
            try:
                data = json.loads(p.read_text())
                seat_info = data.get("seats", {}).get(seat_id, {"reserved": None, "by": ""})
                reserved = seat_info.get("reserved")
                owner = seat_info.get("by") or "none"
                if reserved is True:
                    status = f"RESERVED by {owner}"
                elif reserved is False:
                    status = "FREE"
                else:
                    status = "UNKNOWN"
                print(f"{p.name}: {status}")
            except Exception as exc:
                print(f"{p.name}: error reading file ({exc})")
        print("-" * 50)
        time.sleep(1)
except KeyboardInterrupt:
    print("\nStopped monitoring.")
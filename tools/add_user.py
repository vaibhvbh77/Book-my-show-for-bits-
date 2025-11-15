#!/usr/bin/env python3
# tools/add_user.py
import json, hashlib, os, sys

FILES = ["node_node1.json", "node_node2.json", "node_node3.json"]

def add_user_to_file(fn, username, password):
    try:
        d = json.load(open(fn))
    except Exception:
        d = {}

    users = d.get("users", {})
    # generate salt + hash
    salt = os.urandom(8).hex()
    pw_hash = hashlib.sha256((salt + password).encode()).hexdigest()
    users[username] = {"salt": salt, "pw_hash": pw_hash}
    d["users"] = users

    with open(fn, "w") as f:
        json.dump(d, f, indent=2)
    print(f"[OK] Added/updated user '{username}' in {fn}")

def main():
    if len(sys.argv) >= 3:
        username = sys.argv[1]
        password = sys.argv[2]
    else:
        username = input("Username: ").strip()
        password = input("Password: ").strip()

    for fn in FILES:
        # ensure file exists (creates a minimal JSON if missing)
        if not os.path.exists(fn):
            print(f"[INFO] {fn} not found — creating fresh file.")
            with open(fn, "w") as f:
                json.dump({}, f)
        add_user_to_file(fn, username, password)

if __name__ == "__main__":
    main()

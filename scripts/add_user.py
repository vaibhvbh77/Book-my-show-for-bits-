# scripts/add_user.py
import sys
from nodes.node import Persist

if len(sys.argv) < 3:
    print("usage: python3 scripts/add_user.py <username> <password> [node_file]")
    print("node_file default: node_node1.json")
    sys.exit(1)

username = sys.argv[1]
password = sys.argv[2]
node_file = sys.argv[3] if len(sys.argv) > 3 else "node_node1.json"

p = Persist(node_file)
p.add_user(username, password)
print("Added user", username, "to", node_file)

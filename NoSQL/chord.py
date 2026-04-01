# Simple Chord DHT Implementation (Multi-node via TCP sockets)
# ---------------------------------------------------------------
# Features:
# - Multiple nodes
# - TCP socket communication
# - Join existing ring
# - Successor-based lookup
# - Store / retrieve key-value pairs
# - Minimal finger-table (successor only for simplicity)
#
# Run multiple nodes like:
#   python chord.py 5000
#   python chord.py 5001 127.0.0.1 5000
#   python chord.py 5002 127.0.0.1 5000
#
# Then interact using:
#   put key value
#   get key

# What is simplified compared to full Chord?
# Real Chord systems also include:
# - finger tables
# - predecessor pointers
# - stabilization protocol
# - failure recovery
# - replication




import socket
import threading
import hashlib
import json
import sys

M = 8
RING_SIZE = 2 ** M


def hash_key(key):
    return int(hashlib.sha1(key.encode()).hexdigest(), 16) % RING_SIZE


class Node:

    def __init__(self, host, port, bootstrap=None):
        self.host = host
        self.port = port
        self.node_id = hash_key(f"{host}:{port}")

        self.successor = (host, port)
        self.successor_id = self.node_id

        self.data = {}

        print(f"Node started: {self.node_id} @ {host}:{port}")

        threading.Thread(target=self.server_loop, daemon=True).start()

        if bootstrap:
            self.join(bootstrap)

    # ---------------- NETWORK ----------------

    def send(self, addr, msg):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(addr)
            s.send(json.dumps(msg).encode())
            response = s.recv(4096).decode()
            s.close()

            if response:
                return json.loads(response)

        except Exception as e:
            print("Send error:", e)

        return None

    def server_loop(self):

        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(5)

        while True:
            conn, _ = server.accept()
            threading.Thread(target=self.handle_client,
                             args=(conn,), daemon=True).start()

    # ---------------- JOIN ----------------

    def join(self, bootstrap):

        print("Joining network via", bootstrap)

        response = self.send(bootstrap, {
            "type": "find_successor",
            "id": self.node_id
        })

        if response:
            self.successor = tuple(response["successor"])
            self.successor_id = response["successor_id"]

        print("Successor set to", self.successor_id)

    # ---------------- LOOKUP ----------------

    def is_between(self, key, start, end):

        if start < end:
            return start < key <= end

        return key > start or key <= end

    def find_successor(self, key):

        if self.node_id == self.successor_id:
            return self.host, self.port, self.node_id

        if self.is_between(key, self.node_id, self.successor_id):
            return (*self.successor, self.successor_id)

        response = self.send(self.successor, {
            "type": "find_successor",
            "id": key
        })

        return tuple(response.values())

    # ---------------- STORAGE ----------------

    def put(self, key, value):

        key_id = hash_key(key)

        succ_host, succ_port, succ_id = self.find_successor(key_id)

        if succ_id == self.node_id:
            self.data[key] = value
            print("Stored locally")
            return

        self.send((succ_host, succ_port), {
            "type": "store",
            "key": key,
            "value": value
        })

    def get(self, key):

        key_id = hash_key(key)

        succ_host, succ_port, succ_id = self.find_successor(key_id)

        if succ_id == self.node_id:
            return self.data.get(key)

        response = self.send((succ_host, succ_port), {
            "type": "retrieve",
            "key": key
        })

        return response

    # ---------------- MESSAGE HANDLER ----------------

    def handle_client(self, conn):

        data = conn.recv(4096).decode()

        if not data:
            conn.close()
            return

        msg = json.loads(data)

        msg_type = msg["type"]

        if msg_type == "find_successor":

            key = msg["id"]

            host, port, nid = self.find_successor(key)

            conn.send(json.dumps({
                "successor": (host, port),
                "successor_id": nid
            }).encode())

        elif msg_type == "store":

            key = msg["key"]
            value = msg["value"]

            self.data[key] = value

            conn.send(b"OK")

        elif msg_type == "retrieve":

            key = msg["key"]

            value = self.data.get(key)

            conn.send(json.dumps(value).encode())

        conn.close()


# ---------------- CLI ----------------


def cli(node):

    while True:

        cmd = input("> ")

        parts = cmd.split()

        if not parts:
            continue

        if parts[0] == "put":

            key = parts[1]
            value = parts[2]

            node.put(key, value)

        elif parts[0] == "get":

            key = parts[1]

            print(node.get(key))


# ---------------- MAIN ----------------


if __name__ == "__main__":

    host = "127.0.0.1"

    port = int(sys.argv[1])

    bootstrap = None

    if len(sys.argv) == 4:

        bootstrap = (sys.argv[2], int(sys.argv[3]))

    node = Node(host, port, bootstrap)

    cli(node)

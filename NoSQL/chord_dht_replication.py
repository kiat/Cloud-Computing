# Simple Chord DHT Implementation with Replication (Educational, Multi-node via TCP sockets)
# ------------------------------------------------------------------
# Features:
# - Multiple nodes
# - TCP socket communication
# - Join existing ring
# - Successor-based lookup
# - Store / retrieve key-value pairs
# - Successor-chain replication (Replication Factor configurable)
#
# Example usage:
#   python chord.py 5000
#   python chord.py 5001 127.0.0.1 5000
#   python chord.py 5002 127.0.0.1 5000
#
# Commands inside CLI:
#   put key value
#   get key
#   info

import socket
import threading
import hashlib
import json
import sys
import time

M = 8
RING_SIZE = 2 ** M
REPLICATION_FACTOR = 3


# ---------------- HASH FUNCTION ----------------

def hash_key(key):
    return int(hashlib.sha1(key.encode()).hexdigest(), 16) % RING_SIZE


# ---------------- NODE CLASS ----------------

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
            time.sleep(1)
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

    # ---------------- INTERVAL CHECK ----------------

    def is_between(self, key, start, end):

        if start < end:
            return start < key <= end

        return key > start or key <= end

    # ---------------- LOOKUP ----------------

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

    # ---------------- REPLICATION ----------------

    def replicate_to_successors(self, key, value):

        current = self.successor

        for _ in range(REPLICATION_FACTOR - 1):

            if current == (self.host, self.port):
                return

            response = self.send(current, {
                "type": "replica_store",
                "key": key,
                "value": value
            })

            if not response:
                return

            current = tuple(response["next_successor"])

    # ---------------- STORAGE ----------------

    def put(self, key, value):

        key_id = hash_key(key)

        succ_host, succ_port, succ_id = self.find_successor(key_id)

        if succ_id == self.node_id:

            self.data[key] = value

            print("Stored locally (primary)")

            self.replicate_to_successors(key, value)

            return

        self.send((succ_host, succ_port), {
            "type": "store",
            "key": key,
            "value": value
        })

    def get(self, key):

        if key in self.data:
            return self.data[key]

        key_id = hash_key(key)

        succ_host, succ_port, succ_id = self.find_successor(key_id)

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

        # -------- FIND SUCCESSOR --------

        if msg_type == "find_successor":

            key = msg["id"]

            host, port, nid = self.find_successor(key)

            conn.send(json.dumps({
                "successor": (host, port),
                "successor_id": nid
            }).encode())

        # -------- PRIMARY STORE --------

        elif msg_type == "store":

            key = msg["key"]
            value = msg["value"]

            self.data[key] = value

            print(f"Stored replica-primary: {key}")

            self.replicate_to_successors(key, value)

            conn.send(b"OK")

        # -------- REPLICA STORE --------

        elif msg_type == "replica_store":

            key = msg["key"]
            value = msg["value"]

            self.data[key] = value

            conn.send(json.dumps({
                "next_successor": self.successor
            }).encode())

        # -------- RETRIEVE --------

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

        elif parts[0] == "info":

            print("Node ID:", node.node_id)
            print("Successor:", node.successor_id)
            print("Stored keys:", list(node.data.keys()))


# ---------------- MAIN ----------------


if __name__ == "__main__":

    host = "127.0.0.1"

    port = int(sys.argv[1])

    bootstrap = None

    if len(sys.argv) == 4:

        bootstrap = (sys.argv[2], int(sys.argv[3]))

    node = Node(host, port, bootstrap)

    cli(node)

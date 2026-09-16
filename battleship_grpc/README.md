# Battleship over gRPC

A two-player Battleship game with a Python gRPC server and CLI client.

- **Board**: 50 x 50 array per player (`common.BOARD_SIZE = 50`)
- **Ships**: 4 ships, sizes `[5, 4, 3, 2]` (largest ship is 5 cells, as required)
- **Transport**: gRPC (protobuf messages, one unary RPC per action, plus a
  server-streaming RPC for real-time turn/shot notifications)

## Project layout

```
battleship_grpc/
  proto/battleship.proto   # service + message definitions
  common.py                # shared constants (board size, ship sizes, cell states)
  server.py                # BattleshipService implementation + game logic
  client.py                # CLI client
  requirements.txt
  generate_grpc.sh         # regenerates the *_pb2*.py files from the .proto
```

## How it works

1. **`Connect`** (unary) — a client registers with a name and is matched into
   a game. The first client waits; the second client to connect is paired
   with it automatically (simple 1-game matchmaking queue).
2. **`PlaceShips`** (unary) — each client sends 4 `ShipPlacement`s (start
   row/col, size, orientation). The server validates bounds, overlap, and
   that the sizes match `[5,4,3,2]`, then marks the cells `SHIP` in that
   player's private 50x50 board array.
3. **`WatchGame`** (server-streaming) — each client opens a long-lived stream.
   The server pushes `GameUpdate` events down it: whose turn it is, shot
   results (yours and the opponent's), and the final game-over result. This
   is what makes the game feel real-time instead of polling.
4. **`Fire`** (unary) — on your turn, you send target coordinates. The server
   checks it's really your turn, marks the opponent's board cell `HIT` or
   `MISS`, checks whether that sinks a ship / ends the game, flips the turn,
   and fans out `GameUpdate`s to both players via their `WatchGame` streams.

Game/board state lives entirely on the server, keyed by `game_id`, protected
by a per-game lock so concurrent RPCs from both players are handled safely.

## Setup

```bash
cd battleship_grpc
pip3 install -r requirements.txt      # grpcio, grpcio-tools
./generate_grpc.sh                   # generates battleship_pb2.py / battleship_pb2_grpc.py
```

(If you're on Windows or don't have bash, just run the command inside
`generate_grpc.sh` directly:
`python -m grpc_tools.protoc -I proto --python_out=. --grpc_python_out=. proto/battleship.proto`)

## Running

Start the server (defaults to port 50051):

```bash
python3 server.py
```

Start two clients (in two terminals, or on two machines pointed at the
server's host):

```bash
python3 client.py --name Alice --host localhost --port 50051
python3 client.py --name Bob   --host localhost --port 50051
```

Each client will ask whether to place ships manually (`m`) or randomly (`r`).
For manual placement you'll be prompted per ship:

```
Ship of size 5 - enter 'row col orientation' (orientation = H or V): 10 10 H
```

Once both players have placed all 4 ships, the game starts. Whoever's turn
it is gets prompted for `row col` to fire; the other client just watches
updates stream in until it's their turn.

## Staying connected

The client opens its `WatchGame` stream **immediately after `Connect`**, and
keeps it open for the whole session — through waiting for an opponent, ship
placement (yours and theirs), every turn, and until the match actually ends.
It only exits when:

- the game genuinely finishes (someone wins/loses),
- the opponent disconnects,
- the connection to the server is actually lost, or
- you type `quit`.

A rejected `PlaceShips` call (e.g. invalid placement) no longer ends the
client — it just prompts you to place ships again. You're also allowed to
place your ships before an opponent has even connected; the server holds
your placement and starts the match as soon as both sides are ready.

## Notes / possible extensions

- Matchmaking is a simple 1-waiting-game queue; for multiple simultaneous
  matches this already works (each pair gets its own `Game`), it just always
  pairs the next connector with whoever is currently waiting.
- Turns strictly alternate (no "go again on hit" bonus) — easy to change in
  `server.py`'s `Fire` handler if you want that variant.
- The client renders the full 50x50 grid to the terminal each turn; for a
  nicer UX you'd typically build a GUI (e.g., a web frontend calling the same
  gRPC service via grpc-web, or a curses-based TUI) on top of the same
  server/proto unchanged.
- To run client and server on different machines, just point `--host`/`--port`
  at the server's reachable address and make sure the port is open.

"""
Battleship gRPC client.

Run with:
    python client.py --name Alice [--host localhost --port 50051]

Requires battleship_pb2.py and battleship_pb2_grpc.py to have been generated
from proto/battleship.proto (see README.md).
"""

import argparse
import random
import threading

import grpc

import battleship_pb2 as pb2
import battleship_pb2_grpc as pb2_grpc
from common import BOARD_SIZE, REQUIRED_SHIP_SIZES, CELL_DISPLAY, EMPTY, HIT, MISS


class ClientState:
    """Thread-safe shared state between the main thread and the watcher thread."""

    def __init__(self):
        self.lock = threading.Lock()
        self.my_turn = False
        self.game_over = False
        self.you_win = None
        self.connection_lost = False   # true only on a real transport error
        self.placed = False            # true once PlaceShips has succeeded
        # What I know about the opponent's board, from my own shots.
        self.opponent_view = [[EMPTY for _ in range(BOARD_SIZE)] for _ in range(BOARD_SIZE)]
        # What has landed on my own board (for display only; the source of
        # truth for my ships lives on the server).
        self.my_incoming = [[EMPTY for _ in range(BOARD_SIZE)] for _ in range(BOARD_SIZE)]


def print_board(grid, title, highlight_ships=None):
    """Print a compact view of a BOARD_SIZE x BOARD_SIZE grid."""
    print(f"\n--- {title} ---")
    header = "    " + " ".join(f"{c % 10}" for c in range(BOARD_SIZE))
    print(header)
    for r in range(BOARD_SIZE):
        row_chars = []
        for c in range(BOARD_SIZE):
            row_chars.append(CELL_DISPLAY.get(grid[r][c], "?"))
        print(f"{r:2d}  " + " ".join(row_chars))


def watch_thread_fn(stub, player_id, game_id, state: ClientState):
    """Runs in the background for the entire session (connect -> placement ->
    play -> game over), printing server-pushed updates and keeping
    ClientState in sync. Only sets connection_lost on a genuine transport
    failure -- normal waiting states never end this thread."""
    try:
        req = pb2.WatchRequest(player_id=player_id, game_id=game_id)
        for update in stub.WatchGame(req):
            handle_update(update, state)
    except grpc.RpcError as e:
        print(f"\n[connection to server lost: {e.code()}]")
        with state.lock:
            state.connection_lost = True
            state.game_over = True


def handle_update(update, state: ClientState):
    t = pb2.GameUpdate.UpdateType
    with state.lock:
        if update.type == t.WAITING_FOR_OPPONENT:
            print(f"\n[server] {update.message}")
        elif update.type == t.OPPONENT_JOINED:
            print(f"\n[server] {update.message}")
        elif update.type == t.WAITING_FOR_PLACEMENT:
            print(f"\n[server] {update.message}")
        elif update.type == t.GAME_START:
            print(f"\n[server] {update.message}")
        elif update.type == t.YOUR_TURN:
            state.my_turn = True
            print(f"\n[server] {update.message}")
        elif update.type == t.OPPONENT_TURN:
            state.my_turn = False
            print(f"\n[server] {update.message}")
        elif update.type == t.SHOT_RESULT:
            r, c, result = update.row, update.col, update.result
            mark = HIT if result in (pb2.HIT, pb2.SUNK, pb2.WIN) else MISS
            if update.shot_by_me:
                state.opponent_view[r][c] = mark
                print(f"\n[you fired] {update.message}")
            else:
                state.my_incoming[r][c] = mark
                print(f"\n[incoming!] {update.message}")
        elif update.type == t.GAME_OVER:
            state.game_over = True
            state.you_win = update.you_win
            print(f"\n[server] {update.message}")
        elif update.type == t.OPPONENT_DISCONNECTED:
            state.game_over = True
            print(f"\n[server] {update.message}")


# ---------------------------------------------------------------------------
# Ship placement helpers
# ---------------------------------------------------------------------------

def random_placement():
    """Generate a valid random placement for REQUIRED_SHIP_SIZES."""
    occupied = set()
    placements = []
    for size in REQUIRED_SHIP_SIZES:
        while True:
            horizontal = random.choice([True, False])
            if horizontal:
                row = random.randint(0, BOARD_SIZE - 1)
                col = random.randint(0, BOARD_SIZE - size)
            else:
                row = random.randint(0, BOARD_SIZE - size)
                col = random.randint(0, BOARD_SIZE - 1)

            cells = [(row + (0 if horizontal else i), col + (i if horizontal else 0))
                     for i in range(size)]
            if any(cell in occupied for cell in cells):
                continue
            occupied.update(cells)
            placements.append(pb2.ShipPlacement(
                row=row, col=col, size=size, horizontal=horizontal))
            break
    return placements


def manual_placement():
    placements = []
    occupied = set()
    print(f"\nPlace your {len(REQUIRED_SHIP_SIZES)} ships on the "
          f"{BOARD_SIZE}x{BOARD_SIZE} board.")
    print("Rows/columns are 0-based (0.." + str(BOARD_SIZE - 1) + ").")

    for size in REQUIRED_SHIP_SIZES:
        while True:
            try:
                raw = input(f"Ship of size {size} - enter 'row col orientation' "
                            f"(orientation = H or V): ").strip().split()
                row, col, orientation = int(raw[0]), int(raw[1]), raw[2].upper()
                horizontal = orientation == "H"
            except (ValueError, IndexError):
                print("Invalid input, expected: <row> <col> <H|V>. Try again.")
                continue

            cells = [(row + (0 if horizontal else i), col + (i if horizontal else 0))
                     for i in range(size)]
            if any(not (0 <= rr < BOARD_SIZE and 0 <= cc < BOARD_SIZE) for rr, cc in cells):
                print("That placement goes out of bounds. Try again.")
                continue
            if any(cell in occupied for cell in cells):
                print("That overlaps another ship. Try again.")
                continue

            occupied.update(cells)
            placements.append(pb2.ShipPlacement(
                row=row, col=col, size=size, horizontal=horizontal))
            break

    return placements


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", default="Player")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=50051)
    args = parser.parse_args()

    channel = grpc.insecure_channel(f"{args.host}:{args.port}")
    stub = pb2_grpc.BattleshipServiceStub(channel)

    connect_resp = stub.Connect(pb2.ConnectRequest(player_name=args.name))
    if not connect_resp.success:
        print(f"Failed to connect: {connect_resp.message}")
        return

    player_id = connect_resp.player_id
    game_id = connect_resp.game_id
    print(f"Connected as player {connect_resp.player_number} "
          f"(game '{game_id}'). {connect_resp.message}")

    # --- Start the background stream watcher right away ------------------
    # This is the single long-lived connection that keeps the client "in"
    # the game through every phase: waiting for an opponent, ship placement,
    # play, and game over. It's started before placement so status updates
    # (e.g. "waiting for opponent", "opponent joined") show up immediately,
    # and so the client never has a gap where it isn't listening.
    state = ClientState()
    watcher = threading.Thread(
        target=watch_thread_fn, args=(stub, player_id, game_id, state), daemon=True)
    watcher.start()

    # --- Ship placement ----------------------------------------------------
    # A player can place ships even before an opponent has connected; the
    # server just holds the placement and starts the match once both sides
    # are ready. Retry on a rejected placement instead of exiting the
    # process -- a bad placement (or a transient issue) is never a reason
    # to disconnect.
    print(f"\nPlace your {len(REQUIRED_SHIP_SIZES)} ships "
          f"(sizes {REQUIRED_SHIP_SIZES}) on the {BOARD_SIZE}x{BOARD_SIZE} board.")
    while True:
        with state.lock:
            if state.connection_lost:
                print("Lost connection to the server before ships could be placed.")
                return

        mode = input("Place ships (m)anually or (r)andomly? [r]: ").strip().lower()
        ships = manual_placement() if mode == "m" else random_placement()
        if mode != "m":
            print("Randomly placed your ships.")

        place_resp = stub.PlaceShips(pb2.PlaceShipsRequest(
            player_id=player_id, game_id=game_id, ships=ships))
        if place_resp.success:
            print(f"[server] {place_resp.message}")
            with state.lock:
                state.placed = True
            break
        print(f"Placement rejected: {place_resp.message}\nLet's try again.")

    # --- Main gameplay loop -------------------------------------------------
    # Stays running through "waiting for opponent", ship placement on their
    # side, and every turn, until the game genuinely ends (win/lose, the
    # opponent disconnects, we lose our own connection, or the user quits).
    print("\nWaiting for the game to start...")
    while True:
        with state.lock:
            game_over = state.game_over
            my_turn = state.my_turn
            connection_lost = state.connection_lost

        if connection_lost:
            print("\nConnection to the server was lost. Exiting.")
            break

        if game_over:
            with state.lock:
                you_win = state.you_win
            if you_win is True:
                print("\n*** YOU WIN! ***")
            elif you_win is False:
                print("\n*** YOU LOSE. ***")
            else:
                print("\nGame ended (opponent disconnected).")
            break

        if not my_turn:
            threading.Event().wait(0.3)
            continue

        with state.lock:
            print_board(state.opponent_view, "Opponent board (your shots)")

        raw = input("\nYour turn! Enter 'row col' to fire "
                     "(or 'board' to reprint, 'quit' to exit): ").strip()
        if raw.lower() == "quit":
            print("Leaving the game.")
            break
        if raw.lower() == "board":
            continue

        try:
            row_s, col_s = raw.split()
            row, col = int(row_s), int(col_s)
        except ValueError:
            print("Invalid input, expected: <row> <col>")
            continue

        fire_resp = stub.Fire(pb2.FireRequest(
            player_id=player_id, game_id=game_id, row=row, col=col))
        if not fire_resp.success:
            print(f"[server] {fire_resp.message}")
            continue
        # The watcher thread will also print the SHOT_RESULT / turn updates.
        with state.lock:
            state.my_turn = False

    print("Game finished. Goodbye!")


if __name__ == "__main__":
    main()

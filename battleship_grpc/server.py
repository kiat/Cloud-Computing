"""
Battleship gRPC server.

Run with:
    python server.py [--port 50051]

Requires battleship_pb2.py and battleship_pb2_grpc.py to have been generated
from proto/battleship.proto (see README.md).
"""

import argparse
import queue
import threading
import uuid
from concurrent import futures

import grpc

import battleship_pb2 as pb2
import battleship_pb2_grpc as pb2_grpc
from common import BOARD_SIZE, REQUIRED_SHIP_SIZES, EMPTY, SHIP, HIT, MISS


# =============================================================================
# Game state
# =============================================================================

class Ship:
    """A single ship: the set of cells it occupies and which have been hit."""

    def __init__(self, cells):
        self.cells = set(cells)     # {(row, col), ...}
        self.hits = set()

    def register_hit(self, row, col):
        if (row, col) in self.cells:
            self.hits.add((row, col))
            return True
        return False

    def is_sunk(self):
        return self.cells == self.hits


class PlayerState:
    """Everything the server tracks for one player within a game."""

    def __init__(self, player_id, name):
        self.player_id = player_id
        self.name = name
        # The player's own 50x50 board: where THEIR ships sit, and where the
        # opponent has hit/missed against them.
        self.board = [[EMPTY for _ in range(BOARD_SIZE)] for _ in range(BOARD_SIZE)]
        self.ships = []          # list[Ship]
        self.placed = False
        self.update_queue = queue.Queue()  # GameUpdate objects for WatchGame

    def all_ships_sunk(self):
        return all(ship.is_sunk() for ship in self.ships)

    def ship_at(self, row, col):
        for ship in self.ships:
            if (row, col) in ship.cells:
                return ship
        return None


class Game:
    """A single match between exactly two players."""

    STATE_WAITING_FOR_PLAYER = "WAITING_FOR_PLAYER"
    STATE_PLACING = "PLACING"
    STATE_PLAYING = "PLAYING"
    STATE_FINISHED = "FINISHED"

    def __init__(self, game_id):
        self.game_id = game_id
        self.players = {}          # player_id -> PlayerState
        self.player_order = []     # [player_id_1, player_id_2]
        self.turn_index = 0
        self.state = Game.STATE_WAITING_FOR_PLAYER
        self.winner = None
        self.lock = threading.RLock()

    def add_player(self, player_id, name):
        self.players[player_id] = PlayerState(player_id, name)
        self.player_order.append(player_id)
        if len(self.player_order) == 2:
            self.state = Game.STATE_PLACING

    def opponent_id(self, player_id):
        for pid in self.player_order:
            if pid != player_id:
                return pid
        return None

    def current_turn_player_id(self):
        if not self.player_order:
            return None
        return self.player_order[self.turn_index]

    def broadcast(self, update, exclude=None):
        for pid, player in self.players.items():
            if pid == exclude:
                continue
            player.update_queue.put(update)

    def send_to(self, player_id, update):
        if player_id in self.players:
            self.players[player_id].update_queue.put(update)


def make_update(update_type, message="", shot_by_me=False, row=-1, col=-1,
                 result=pb2.UNKNOWN, you_win=False):
    return pb2.GameUpdate(
        type=update_type,
        message=message,
        shot_by_me=shot_by_me,
        row=row,
        col=col,
        result=result,
        you_win=you_win,
    )


# =============================================================================
# Validation helpers
# =============================================================================

def validate_placement(ships):
    """Validate a proposed list of ShipPlacement messages.

    Returns (ok: bool, message: str, occupied_cells: set[(r,c)] or None,
             ship_objects: list[Ship] or None)
    """
    if sorted(s.size for s in ships) != sorted(REQUIRED_SHIP_SIZES):
        return (False,
                f"You must place exactly ships of sizes {REQUIRED_SHIP_SIZES} "
                f"(got {[s.size for s in ships]}).",
                None, None)

    occupied = set()
    ship_objects = []

    for s in ships:
        if s.size < 1 or s.size > 5:
            return False, f"Invalid ship size {s.size} (must be 1-5).", None, None

        cells = []
        for i in range(s.size):
            r = s.row + (0 if s.horizontal else i)
            c = s.col + (i if s.horizontal else 0)
            if not (0 <= r < BOARD_SIZE and 0 <= c < BOARD_SIZE):
                return False, f"Ship out of bounds at ({r},{c}).", None, None
            if (r, c) in occupied:
                return False, f"Ships overlap at ({r},{c}).", None, None
            cells.append((r, c))

        occupied.update(cells)
        ship_objects.append(Ship(cells))

    return True, "OK", occupied, ship_objects


# =============================================================================
# Servicer
# =============================================================================

class BattleshipServicer(pb2_grpc.BattleshipServiceServicer):

    def __init__(self):
        self.lock = threading.RLock()
        self.games = {}            # game_id -> Game
        self.waiting_game_id = None

    # -------------------------------------------------------------------
    def Connect(self, request, context):
        player_name = request.player_name or "Anonymous"
        player_id = str(uuid.uuid4())

        with self.lock:
            if self.waiting_game_id is not None and \
                    self.waiting_game_id in self.games and \
                    len(self.games[self.waiting_game_id].player_order) == 1:
                game = self.games[self.waiting_game_id]
                game.add_player(player_id, player_name)
                player_number = 2
                self.waiting_game_id = None

                # Let player 1 know someone joined.
                other_id = game.opponent_id(player_id)
                game.send_to(other_id, make_update(
                    pb2.GameUpdate.OPPONENT_JOINED,
                    message=f"{player_name} joined. Place your ships!"))
            else:
                game_id = str(uuid.uuid4())[:8]
                game = Game(game_id)
                game.add_player(player_id, player_name)
                self.games[game_id] = game
                self.waiting_game_id = game_id
                player_number = 1
                # Queue this now; it'll be waiting for them as soon as they
                # open the WatchGame stream (queues buffer, so ordering is
                # safe even though they haven't called WatchGame yet).
                game.send_to(player_id, make_update(
                    pb2.GameUpdate.WAITING_FOR_OPPONENT,
                    message="Waiting for an opponent to connect. "
                            "You can place your ships now while you wait."))

        print(f"[Connect] {player_name} ({player_id[:8]}) joined game "
              f"{game.game_id} as player {player_number}")

        return pb2.ConnectResponse(
            success=True,
            message="Connected. Waiting for opponent..." if player_number == 1
                     else "Connected. Opponent is ready.",
            player_id=player_id,
            game_id=game.game_id,
            player_number=player_number,
            board_size=BOARD_SIZE,
            required_ship_sizes=REQUIRED_SHIP_SIZES,
        )

    # -------------------------------------------------------------------
    def PlaceShips(self, request, context):
        game = self.games.get(request.game_id)
        if game is None:
            return pb2.PlaceShipsResponse(success=False, message="Unknown game_id.")

        with game.lock:
            player = game.players.get(request.player_id)
            if player is None:
                return pb2.PlaceShipsResponse(success=False, message="Unknown player_id.")

            if player.placed:
                return pb2.PlaceShipsResponse(success=False, message="Ships already placed.")

            ok, msg, occupied, ship_objects = validate_placement(list(request.ships))
            if not ok:
                return pb2.PlaceShipsResponse(success=False, message=msg)

            for (r, c) in occupied:
                player.board[r][c] = SHIP
            player.ships = ship_objects
            player.placed = True

            print(f"[PlaceShips] game {game.game_id}: {player.name} placed ships.")

            # A player is allowed to place ships even before an opponent has
            # connected -- they just wait once done. opponent_id() is None
            # in that case, so guard for it explicitly.
            opponent_id = game.opponent_id(request.player_id)
            opponent = game.players.get(opponent_id) if opponent_id else None

            if opponent is None:
                game.send_to(request.player_id, make_update(
                    pb2.GameUpdate.WAITING_FOR_OPPONENT,
                    message="Ships placed. Waiting for an opponent to connect..."))
            elif opponent.placed:
                # Both ready -> start the game.
                game.state = Game.STATE_PLAYING
                game.turn_index = 0
                first_player_id = game.current_turn_player_id()

                for pid in game.player_order:
                    game.send_to(pid, make_update(
                        pb2.GameUpdate.GAME_START,
                        message="Both players ready. Game starting!"))
                    if pid == first_player_id:
                        game.send_to(pid, make_update(
                            pb2.GameUpdate.YOUR_TURN,
                            message="Your turn - fire away!"))
                    else:
                        game.send_to(pid, make_update(
                            pb2.GameUpdate.OPPONENT_TURN,
                            message="Opponent's turn. Please wait."))
            else:
                game.send_to(request.player_id, make_update(
                    pb2.GameUpdate.WAITING_FOR_PLACEMENT,
                    message="Waiting for opponent to place their ships..."))

        return pb2.PlaceShipsResponse(success=True, message="Ships placed successfully.")

    # -------------------------------------------------------------------
    def Fire(self, request, context):
        game = self.games.get(request.game_id)
        if game is None:
            return pb2.FireResponse(success=False, message="Unknown game_id.",
                                     result=pb2.INVALID)

        with game.lock:
            attacker_id = request.player_id
            if attacker_id not in game.players:
                return pb2.FireResponse(success=False, message="Unknown player_id.",
                                         result=pb2.INVALID)

            if game.state != Game.STATE_PLAYING:
                return pb2.FireResponse(success=False, message="Game is not in progress.",
                                         result=pb2.INVALID)

            if game.current_turn_player_id() != attacker_id:
                return pb2.FireResponse(success=False, message="It is not your turn.",
                                         result=pb2.INVALID)

            r, c = request.row, request.col
            if not (0 <= r < BOARD_SIZE and 0 <= c < BOARD_SIZE):
                return pb2.FireResponse(success=False, message="Coordinates out of bounds.",
                                         result=pb2.INVALID)

            defender_id = game.opponent_id(attacker_id)
            defender = game.players[defender_id]

            if defender.board[r][c] in (HIT, MISS):
                return pb2.FireResponse(success=False, message="You already fired there.",
                                         result=pb2.INVALID)

            if defender.board[r][c] == SHIP:
                defender.board[r][c] = HIT
                ship = defender.ship_at(r, c)
                ship.register_hit(r, c)
                if ship.is_sunk():
                    result = pb2.SUNK
                    msg = f"Hit and sunk a ship at ({r},{c})!"
                else:
                    result = pb2.HIT
                    msg = f"Hit at ({r},{c})!"
            else:
                defender.board[r][c] = MISS
                result = pb2.MISS
                msg = f"Miss at ({r},{c})."

            game_over = defender.all_ships_sunk()
            if game_over:
                result = pb2.WIN
                game.state = Game.STATE_FINISHED
                game.winner = attacker_id

            # Notify the defender of the incoming shot.
            game.send_to(defender_id, make_update(
                pb2.GameUpdate.SHOT_RESULT,
                message=msg, shot_by_me=False, row=r, col=c, result=result))

            # Notify the attacker too (in addition to the direct RPC reply),
            # so a UI driven purely off the stream also sees it.
            game.send_to(attacker_id, make_update(
                pb2.GameUpdate.SHOT_RESULT,
                message=msg, shot_by_me=True, row=r, col=c, result=result))

            if game_over:
                game.send_to(attacker_id, make_update(
                    pb2.GameUpdate.GAME_OVER,
                    message="You sank all enemy ships. You win!", you_win=True))
                game.send_to(defender_id, make_update(
                    pb2.GameUpdate.GAME_OVER,
                    message="All your ships have been sunk. You lose.", you_win=False))
            else:
                game.turn_index = 1 - game.turn_index
                next_player_id = game.current_turn_player_id()
                waiting_player_id = game.opponent_id(next_player_id)
                game.send_to(next_player_id, make_update(
                    pb2.GameUpdate.YOUR_TURN, message="Your turn - fire away!"))
                game.send_to(waiting_player_id, make_update(
                    pb2.GameUpdate.OPPONENT_TURN,
                    message="Opponent's turn. Please wait."))

            print(f"[Fire] game {game.game_id}: {game.players[attacker_id].name} "
                  f"fired at ({r},{c}) -> {pb2.ShotResult.Name(result)}")

        return pb2.FireResponse(success=True, message=msg, result=result)

    # -------------------------------------------------------------------
    def WatchGame(self, request, context):
        game = self.games.get(request.game_id)
        if game is None or request.player_id not in game.players:
            return

        player = game.players[request.player_id]
        print(f"[WatchGame] {player.name} started watching game {game.game_id}")

        try:
            while context.is_active():
                try:
                    update = player.update_queue.get(timeout=1.0)
                except queue.Empty:
                    continue
                yield update
                if update.type == pb2.GameUpdate.GAME_OVER:
                    break
        finally:
            # This runs whether the loop ended normally (GAME_OVER) or the
            # client's connection dropped. Only notify the opponent in the
            # latter case, and only if the match hadn't already finished.
            with game.lock:
                still_active = game.state in (Game.STATE_WAITING_FOR_PLAYER,
                                               Game.STATE_PLACING, Game.STATE_PLAYING)
                opponent_id = game.opponent_id(request.player_id)
            if still_active and not context.is_active() and opponent_id:
                game.send_to(opponent_id, make_update(
                    pb2.GameUpdate.OPPONENT_DISCONNECTED,
                    message=f"{player.name} disconnected."))
            print(f"[WatchGame] {player.name} stopped watching game {game.game_id}")


# =============================================================================
# Entrypoint
# =============================================================================

def serve(port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    pb2_grpc.add_BattleshipServiceServicer_to_server(BattleshipServicer(), server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"Battleship gRPC server listening on port {port} "
          f"(board {BOARD_SIZE}x{BOARD_SIZE}, ships {REQUIRED_SHIP_SIZES})")
    server.wait_for_termination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)
    args = parser.parse_args()
    serve(args.port)

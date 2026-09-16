"""Shared constants for the Battleship gRPC game."""

BOARD_SIZE = 10               # 10 x 10 board, as required
REQUIRED_SHIP_SIZES = [3, 2]   # 2 ships, largest is 3 cells

# Cell states used inside the server's 50x50 board arrays
EMPTY = 0
SHIP = 1
HIT = 2
MISS = 3

CELL_DISPLAY = {
    EMPTY: ".",
    SHIP: "S",
    HIT: "X",
    MISS: "o",
}

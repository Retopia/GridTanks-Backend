import json
import logging
import random
import time

from fastapi import APIRouter, HTTPException, Depends, WebSocket, WebSocketDisconnect, Body
from fastapi.responses import PlainTextResponse

from pathlib import Path
from typing import Dict
from uuid import uuid4
from pydantic import BaseModel, Field, field_validator

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc

from .database import get_db
from .models import LeaderboardEntry, CoopLeaderboardEntry, EndlessLeaderboardEntry, CoopEndlessLeaderboardEntry, ContactInfo


router = APIRouter()
logger = logging.getLogger("uvicorn")

BASE_DIR = Path(__file__).parent.parent
MAPS_DIR = BASE_DIR / "maps"

# Tank type mapping
TANK_TYPES = {
    3: "player",   # Player Tank - Speed: 2, Bullets: 5, Type: Normal
    4: "brown",    # Brown Tank - Speed: 0, Bullets: 1, Type: Normal
    5: "grey",     # Grey Tank - Speed: 1, Bullets: 2, Type: Normal
    6: "green",    # Green Tank - Speed: 1, Bullets: 1, Type: Fire
    7: "pink",     # Pink Tank - Speed: 2, Bullets: 5, Type: Normal
    8: "black",    # Black Tank - Speed: 3, Bullets: 5, Type: Fire
    9: "red",      # Red Tank - Speed: 3, Bullets: 5, Type: Both
}

ACTIVE_RUNS: Dict[str, Dict] = {}  # run_id -> game_state
ACTIVE_ROOMS: Dict[str, Dict] = {}  # room_code -> room_state
LEVEL_METADATA: Dict[int, Dict] = {}
ROOM_CODE_CHARSET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
ROOM_CODE_LENGTH = 6
ROOM_TTL_SECONDS = 60 * 60 * 2
CLEARED_TRANSITION_PAUSE_MS = 1333
FAILED_TRANSITION_PAUSE_MS = 1167


class RoomCreateRequest(BaseModel):
    display_name: str | None = Field(default=None, max_length=20)

    @field_validator("display_name")
    @classmethod
    def clean_display_name(cls, value):
        if value is None:
            return None
        cleaned = value.strip()
        return cleaned or None


class RoomJoinRequest(RoomCreateRequest):
    room_code: str = Field(min_length=1, max_length=32)


class StartGameRequest(BaseModel):
    mode: str = Field(default="solo", max_length=20)


class RunRequest(BaseModel):
    run_id: str = Field(min_length=1)

    @field_validator("run_id")
    @classmethod
    def clean_run_id(cls, value):
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("Run ID is required")
        return cleaned


class GameEventRequest(RunRequest):
    tank_type: int


class SubmitScoreRequest(RunRequest):
    username: str = Field(min_length=1, max_length=20)
    email: str | None = Field(default=None, max_length=255)
    mode: str = Field(default="solo", max_length=20)

    @field_validator("username")
    @classmethod
    def clean_username(cls, value):
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("Username is required")
        return cleaned

    @field_validator("email")
    @classmethod
    def clean_email(cls, value):
        if value is None:
            return None
        cleaned = value.strip()
        return cleaned or None


def cleanup_old_rooms():
    """Remove stale rooms older than 2 hours."""
    cutoff = time.time() - ROOM_TTL_SECONDS
    to_remove = [
        room_code for room_code, state in ACTIVE_ROOMS.items()
        if state["updated_at"] < cutoff
    ]
    for room_code in to_remove:
        del ACTIVE_ROOMS[room_code]


def normalize_room_code(room_code):
    if not room_code:
        return ""
    cleaned = "".join(ch for ch in str(room_code).upper() if ch.isalnum())
    return cleaned[:ROOM_CODE_LENGTH]


def sanitize_display_name(display_name, fallback):
    if display_name is None:
        return fallback
    cleaned = str(display_name).strip()
    if not cleaned:
        return fallback
    return cleaned[:20]


LEADERBOARD_MODELS = {
    "solo": LeaderboardEntry,
    "coop": CoopLeaderboardEntry,
    "endless": EndlessLeaderboardEntry,
    "coop_endless": CoopEndlessLeaderboardEntry,
}


def normalize_run_mode(raw_mode):
    mode = str(raw_mode or "solo").strip().lower()
    if mode in LEADERBOARD_MODELS:
        return mode
    return "solo"


def is_endless_mode(mode):
    return mode in ("endless", "coop_endless")


def is_coop_mode(mode):
    return mode in ("coop", "coop_endless")


def leaderboard_name_key(username):
    return str(username or "").strip().lower()


def serialize_leaderboard_entry(entry):
    return {
        "username": entry.username,
        "completed_levels": entry.completed_levels,
        "time": entry.formatted_time,
        "date_submitted": entry.date_submitted.strftime("%m/%d/%Y")
    }


def add_backend_pause(game_state, pause_ms):
    game_state["paused_ms"] = int(game_state.get("paused_ms", 0)) + int(pause_ms or 0)


def get_elapsed_time_seconds(game_state):
    paused_seconds = int(game_state.get("paused_ms", 0)) / 1000
    elapsed_seconds = game_state["end_time"] - game_state["start_time"] - paused_seconds
    return max(0, elapsed_seconds)


def generate_room_code():
    return "".join(random.choice(ROOM_CODE_CHARSET) for _ in range(ROOM_CODE_LENGTH))


def find_room_member(room, token):
    if not token:
        return None, None

    host = room["host"]
    if host["token"] == token:
        return host, "host"

    guest = room.get("guest")
    if guest and guest["token"] == token:
        return guest, "guest"

    return None, None


def serialize_room_state(room):
    host = room["host"]
    guest = room.get("guest")

    host_connected = bool(host.get("connected"))
    guest_connected = bool(guest and guest.get("connected"))
    host_ready = bool(host.get("ready"))
    guest_ready = bool(guest and guest.get("ready"))

    both_connected = host_connected and guest_connected
    both_ready = host_ready and guest_ready

    run_mode = ACTIVE_RUNS.get(room.get("run_id"), {}).get("mode")

    return {
        "room_code": room["room_code"],
        "run_id": room.get("run_id"),
        "mode": run_mode,
        "game_started": room["game_started"],
        "host": {
            "name": host["name"],
            "connected": host_connected,
            "ready": host_ready
        },
        "guest": {
            "name": guest["name"],
            "connected": guest_connected,
            "ready": guest_ready
        } if guest else None,
        "both_connected": both_connected,
        "both_ready": both_ready,
        "can_start": both_connected and both_ready and not room["game_started"]
    }


async def send_ws_error(websocket: WebSocket, message: str):
    try:
        await websocket.send_text(json.dumps({
            "type": "error",
            "message": message
        }))
    except Exception:
        return


async def send_ws_message(websocket: WebSocket, payload: dict):
    try:
        await websocket.send_text(json.dumps(payload))
    except Exception:
        return


async def broadcast_room_state(room, message_type="room_state"):
    payload = json.dumps({
        "type": message_type,
        "room": serialize_room_state(room)
    })

    for role in ("host", "guest"):
        member = room["host"] if role == "host" else room.get("guest")
        if not member:
            continue

        websocket = member.get("websocket")
        if websocket is None:
            continue

        try:
            await websocket.send_text(payload)
        except Exception:
            member["websocket"] = None
            member["connected"] = False
            member["ready"] = False


async def forward_to_role(room, target_role, payload):
    member = room["host"] if target_role == "host" else room.get("guest")
    if not member:
        return

    websocket = member.get("websocket")
    if websocket is None:
        return

    try:
        await websocket.send_text(json.dumps(payload))
    except Exception:
        member["websocket"] = None
        member["connected"] = False
        member["ready"] = False

def cleanup_old_runs():
    """Remove runs older than 1 hour"""
    cutoff = time.time() - 3600  # 1 hour ago
    to_remove = [
        run_id for run_id, state in ACTIVE_RUNS.items()
        if state["start_time"] < cutoff
    ]
    for run_id in to_remove:
        del ACTIVE_RUNS[run_id]

def preprocess_levels():
    """Extract tank counts from all level files on startup"""
    global LEVEL_METADATA
    
    for map_file in MAPS_DIR.glob("level_*.txt"):
        number_part = map_file.stem.replace("level_", "")
        if not number_part.isdigit():
            continue
            
        level_num = int(number_part)
        content = map_file.read_text()
        
        # Split into map section and collision lines section
        sections = content.strip().split('\n\n')
        map_section = sections[0]  # Only process the map grid
        
        # Count tanks by type
        tank_counts = {}
        player_spawn = None
        
        for row_idx, line in enumerate(map_section.split('\n')):
            if line.strip() and ' ' in line:
                for col_idx, cell in enumerate(line.split()):
                    if cell.isdigit():
                        cell_type = int(cell)
                        if cell_type == 3:  # Player spawn
                            player_spawn = {"x": col_idx, "y": row_idx}
                        elif cell_type > 3:  # Enemy tanks
                            tank_counts[cell_type] = tank_counts.get(cell_type, 0) + 1
        
        LEVEL_METADATA[level_num] = {
            "total_enemy_tanks": sum(tank_counts.values()),
            "enemy_tank_types": tank_counts,
            "player_spawn": player_spawn,
            "tank_type_names": {
                tank_type: TANK_TYPES.get(tank_type, f"unknown_{tank_type}")
                for tank_type in tank_counts.keys()
            }
        }
        
    logger.info(f"Level Metadata: {LEVEL_METADATA}")

# Call on startup
preprocess_levels()

# --- Endless mode ---
# Waves are fully procedurally generated: a fresh wall layout every wave,
# plus an enemy set whose difficulty scales with the wave number.

# How many "difficulty points" each tank type costs to spawn.
ENDLESS_TANK_COSTS = {4: 1, 5: 2, 6: 3, 7: 4, 8: 5, 9: 6}
# Wave at which each tank type starts appearing.
ENDLESS_TANK_UNLOCK_WAVE = {4: 1, 5: 2, 6: 4, 7: 6, 8: 8, 9: 10}
# Hard cap so late waves stay playable (and performant). Below the cap the
# picker always spends the full wave budget (browns cost 1 and are always
# unlocked), so every wave is strictly harder until all-red saturation at
# 16 * 6 = 96 points (~wave 95).
ENDLESS_MAX_TANKS = 16
# Minimum distance (in cells) between the player spawn and enemy spawns.
ENDLESS_MIN_SPAWN_DISTANCE = 8
# Minimum distance (in cells) between any two enemy spawns.
ENDLESS_MIN_TANK_SPACING = 3

# Procedural map parameters (must match the frontend's 40x30 grid of 20px cells).
ENDLESS_MAP_ROWS = 30
ENDLESS_MAP_COLS = 40
ENDLESS_CELL_SIZE = 20
# Every corridor must be at least this many open cells wide.
ENDLESS_MIN_GAP = 3


def endless_wave_budget(wave):
    # Gentle ramp: wave 1 is one or two weak tanks, roughly +1 point per wave.
    return 1 + wave


def pick_endless_tanks(wave):
    """Spend the wave's difficulty budget on a random mix of tank types,
    weighted toward stronger tanks so later waves skew dangerous."""
    budget = endless_wave_budget(wave)
    available = [t for t, unlock in ENDLESS_TANK_UNLOCK_WAVE.items() if wave >= unlock]

    counts = {}
    total = 0
    while budget > 0 and total < ENDLESS_MAX_TANKS:
        affordable = [t for t in available if ENDLESS_TANK_COSTS[t] <= budget]
        if not affordable:
            break
        tank_type = random.choices(
            affordable,
            weights=[ENDLESS_TANK_COSTS[t] for t in affordable]
        )[0]
        counts[tank_type] = counts.get(tank_type, 0) + 1
        budget -= ENDLESS_TANK_COSTS[tank_type]
        total += 1

    if not counts:
        counts = {4: 1}

    return counts


def is_clear_of_obstacles(grid, cell, radius=1):
    """True if no wall/hole cell lies within `radius` cells (Chebyshev) of
    `cell` — used to keep spawn points from touching walls."""
    rows = len(grid)
    cols = len(grid[0])
    r0, c0 = cell
    for r in range(r0 - radius, r0 + radius + 1):
        for c in range(c0 - radius, c0 + radius + 1):
            if r < 0 or r >= rows or c < 0 or c >= cols:
                return False
            if grid[r][c] in ("1", "2"):
                return False
    return True


def has_line_of_sight(grid, start, end):
    """Bresenham walk between two cells; walls ("1") block sight."""
    r0, c0 = start
    r1, c1 = end
    dr = abs(r1 - r0)
    dc = abs(c1 - c0)
    sr = 1 if r0 < r1 else -1
    sc = 1 if c0 < c1 else -1
    err = dc - dr
    r, c = r0, c0

    while True:
        if grid[r][c] == "1":
            return False
        if r == r1 and c == c1:
            return True
        e2 = 2 * err
        if e2 > -dr:
            err -= dr
            c += sc
        if e2 < dc:
            err += dc
            r += sr


def generate_endless_map():
    """Generate a random arena: bordered grid with scattered wall/hole bars.

    Every bar is placed with ENDLESS_MIN_GAP cells of clearance from all other
    obstacles (border included), so corridors are always at least 3 cells wide.
    Because bars never touch each other, they can't enclose a region — the map
    is always fully connected. Returns (grid, collision_lines, player_pos).
    """
    rows, cols = ENDLESS_MAP_ROWS, ENDLESS_MAP_COLS
    grid = [["0"] * cols for _ in range(rows)]
    for c in range(cols):
        grid[0][c] = "1"
        grid[rows - 1][c] = "1"
    for r in range(rows):
        grid[r][0] = "1"
        grid[r][cols - 1] = "1"

    def area_clear(r0, c0, r1, c1, pad):
        for r in range(max(0, r0 - pad), min(rows, r1 + pad + 1)):
            for c in range(max(0, c0 - pad), min(cols, c1 + pad + 1)):
                if grid[r][c] != "0":
                    return False
        return True

    wall_rects = []
    target_segments = random.randint(6, 10)
    placed = 0
    attempts = 0
    while placed < target_segments and attempts < 400:
        attempts += 1
        length = random.randint(4, 12)
        if random.random() < 0.5:
            height, width = 1, length
        else:
            height, width = length, 1

        r0 = random.randint(1, rows - 1 - height)
        c0 = random.randint(1, cols - 1 - width)
        r1, c1 = r0 + height - 1, c0 + width - 1

        if not area_clear(r0, c0, r1, c1, ENDLESS_MIN_GAP):
            continue

        is_hole = random.random() < 0.15
        fill = "2" if is_hole else "1"
        for r in range(r0, r1 + 1):
            for c in range(c0, c1 + 1):
                grid[r][c] = fill
        if not is_hole:
            # Holes don't block bullets, so only walls get collision lines.
            wall_rects.append((r0, c0, r1, c1))
        placed += 1

    cs = ENDLESS_CELL_SIZE
    collision_lines = [
        (cs, cs, (cols - 1) * cs, cs),
        (cs, (rows - 1) * cs, (cols - 1) * cs, (rows - 1) * cs),
        (cs, cs, cs, (rows - 1) * cs),
        ((cols - 1) * cs, cs, (cols - 1) * cs, (rows - 1) * cs),
    ]
    for r0, c0, r1, c1 in wall_rects:
        x0, y0 = c0 * cs, r0 * cs
        x1, y1 = (c1 + 1) * cs, (r1 + 1) * cs
        collision_lines.extend([
            (x0, y0, x1, y0),
            (x0, y1, x1, y1),
            (x0, y0, x0, y1),
            (x1, y0, x1, y1),
        ])

    spawn_candidates = [
        (r, c)
        for r in range(2, rows - 2)
        for c in range(2, cols - 2)
        if grid[r][c] == "0" and is_clear_of_obstacles(grid, (r, c))
    ]
    player_pos = random.choice(spawn_candidates)
    grid[player_pos[0]][player_pos[1]] = "3"

    return grid, collision_lines, player_pos


def generate_endless_wave(wave):
    """Build a wave: a fresh procedural arena + procedurally placed enemies.
    Returns (map_text, tank_counts)."""
    grid, collision_lines, player_pos = generate_endless_map()
    lines_section = "\n".join(
        " ".join(str(v) for v in line) for line in collision_lines
    )

    tank_counts = pick_endless_tanks(wave)
    total_tanks = sum(tank_counts.values())

    open_cells = [
        (r, c)
        for r, row in enumerate(grid)
        for c, cell in enumerate(row)
        if cell == "0" and is_clear_of_obstacles(grid, (r, c))
    ]
    if len(open_cells) < total_tanks:
        # Defensive fallback — with the 3-cell gap rule there are always
        # hundreds of clear cells, but never fail outright.
        open_cells = [
            (r, c)
            for r, row in enumerate(grid)
            for c, cell in enumerate(row)
            if cell == "0"
        ]

    def far_enough(cell, min_distance):
        dr = cell[0] - player_pos[0]
        dc = cell[1] - player_pos[1]
        return (dr * dr + dc * dc) >= min_distance * min_distance

    spawn_cells = [cell for cell in open_cells if far_enough(cell, ENDLESS_MIN_SPAWN_DISTANCE)]
    if len(spawn_cells) < total_tanks:
        spawn_cells = [cell for cell in open_cells if far_enough(cell, 4)]
    if len(spawn_cells) < total_tanks:
        spawn_cells = open_cells

    # Prefer cells the player spawn can't be shot from directly, so waves
    # never open with an immediate snipe. Fall back to sighted cells only if
    # the map doesn't have enough cover.
    covered = [cell for cell in spawn_cells if not has_line_of_sight(grid, cell, player_pos)]
    sighted = [cell for cell in spawn_cells if cell not in set(covered)]
    random.shuffle(covered)
    random.shuffle(sighted)
    spawn_pool = covered + sighted

    # Greedily pick spawns so no two tanks start within
    # ENDLESS_MIN_TANK_SPACING cells of each other.
    min_spacing_sq = ENDLESS_MIN_TANK_SPACING * ENDLESS_MIN_TANK_SPACING

    def spaced_from_chosen(cell, chosen):
        return all(
            (cell[0] - other[0]) ** 2 + (cell[1] - other[1]) ** 2 >= min_spacing_sq
            for other in chosen
        )

    chosen_cells = []
    for cell in spawn_pool:
        if len(chosen_cells) == total_tanks:
            break
        if spaced_from_chosen(cell, chosen_cells):
            chosen_cells.append(cell)

    if len(chosen_cells) < total_tanks:
        # Not enough spaced cells (shouldn't happen on a 40x30 grid) —
        # relax the spacing rather than under-spawn the wave.
        chosen_set = set(chosen_cells)
        for cell in spawn_pool:
            if len(chosen_cells) == total_tanks:
                break
            if cell not in chosen_set:
                chosen_cells.append(cell)
                chosen_set.add(cell)

    placed_counts = {}
    cell_iter = iter(chosen_cells)
    for tank_type, count in tank_counts.items():
        for _ in range(count):
            cell = next(cell_iter, None)
            if cell is None:
                break
            grid[cell[0]][cell[1]] = str(tank_type)
            placed_counts[tank_type] = placed_counts.get(tank_type, 0) + 1

    map_text = "\n".join(" ".join(row) for row in grid)
    map_text += "\n\n" + lines_section

    return map_text, placed_counts

@router.get("/health")
async def health_check():
    return {"status": "ok"}


@router.post("/rooms/create")
async def create_room(data: RoomCreateRequest):
    cleanup_old_rooms()

    display_name = sanitize_display_name(data.display_name, "Host")

    room_code = None
    for _ in range(50):
        candidate = generate_room_code()
        if candidate not in ACTIVE_ROOMS:
            room_code = candidate
            break

    if room_code is None:
        raise HTTPException(status_code=500, detail="Failed to allocate a room code")

    player_token = str(uuid4())
    now = time.time()

    room = {
        "room_code": room_code,
        "created_at": now,
        "updated_at": now,
        "run_id": None,
        "game_started": False,
        "host": {
            "name": display_name,
            "token": player_token,
            "connected": False,
            "ready": False,
            "websocket": None
        },
        "guest": None
    }

    ACTIVE_ROOMS[room_code] = room

    return {
        "room_code": room_code,
        "role": "host",
        "player_name": display_name,
        "player_token": player_token,
        "room_state": serialize_room_state(room)
    }


@router.post("/rooms/join")
async def join_room(data: RoomJoinRequest):
    cleanup_old_rooms()

    room_code = normalize_room_code(data.room_code)
    if len(room_code) != ROOM_CODE_LENGTH:
        raise HTTPException(status_code=400, detail="Invalid room code")

    room = ACTIVE_ROOMS.get(room_code)
    if not room:
        raise HTTPException(status_code=404, detail="Room not found")

    if room["game_started"]:
        raise HTTPException(status_code=409, detail="Room has already started")

    if room.get("guest"):
        raise HTTPException(status_code=409, detail="Room is full")

    display_name = sanitize_display_name(data.display_name, "Guest")
    player_token = str(uuid4())

    room["guest"] = {
        "name": display_name,
        "token": player_token,
        "connected": False,
        "ready": False,
        "websocket": None
    }
    room["updated_at"] = time.time()

    return {
        "room_code": room_code,
        "role": "guest",
        "player_name": display_name,
        "player_token": player_token,
        "room_state": serialize_room_state(room)
    }


@router.websocket("/ws/rooms/{room_code}")
async def room_socket(websocket: WebSocket, room_code: str, token: str):
    normalized_code = normalize_room_code(room_code)
    token = str(token or "").strip()

    await websocket.accept()

    room = ACTIVE_ROOMS.get(normalized_code)
    if not room:
        await send_ws_error(websocket, "Room not found.")
        await websocket.close(code=1008)
        return

    member, _ = find_room_member(room, token)
    if member is None:
        await send_ws_error(websocket, "Invalid room token.")
        await websocket.close(code=1008)
        return

    previous_socket = member.get("websocket")
    if previous_socket and previous_socket != websocket:
        try:
            await previous_socket.close(code=1012)
        except Exception:
            pass

    member["websocket"] = websocket
    member["connected"] = True
    room["updated_at"] = time.time()

    await broadcast_room_state(room)

    try:
        while True:
            raw_message = await websocket.receive_text()

            try:
                parsed = json.loads(raw_message)
            except json.JSONDecodeError:
                await send_ws_error(websocket, "Invalid message format.")
                continue

            message_type = parsed.get("type")

            latest_room = ACTIVE_ROOMS.get(normalized_code)
            if latest_room is None:
                await send_ws_error(websocket, "Room no longer exists.")
                await websocket.close(code=1008)
                return

            latest_member, latest_role = find_room_member(latest_room, token)
            if latest_member is None:
                await send_ws_error(websocket, "Invalid room token.")
                await websocket.close(code=1008)
                return

            if message_type == "set_ready":
                latest_member["ready"] = bool(parsed.get("ready"))
                latest_room["updated_at"] = time.time()
                await broadcast_room_state(latest_room)
                continue

            if message_type == "start_game":
                if latest_role != "host":
                    await send_ws_error(websocket, "Only host can start the game.")
                    continue

                requested_run_id = parsed.get("run_id")
                if requested_run_id:
                    latest_room["run_id"] = str(requested_run_id).strip()

                if not latest_room.get("run_id"):
                    await send_ws_error(websocket, "Missing run id for co-op game start.")
                    continue

                active_run = ACTIVE_RUNS.get(latest_room["run_id"])
                if active_run is None:
                    await send_ws_error(websocket, "Run not found. Create a new run and try again.")
                    continue
                # Preserve an endless co-op run; otherwise mark it a co-op run.
                if not is_coop_mode(active_run.get("mode")):
                    active_run["mode"] = "coop"

                host = latest_room["host"]
                guest = latest_room.get("guest")
                can_start = (
                    host.get("connected")
                    and host.get("ready")
                    and guest is not None
                    and guest.get("connected")
                    and guest.get("ready")
                )

                if not can_start:
                    await send_ws_error(websocket, "Both players must be connected and ready.")
                    continue

                latest_room["game_started"] = True
                latest_room["updated_at"] = time.time()
                await broadcast_room_state(latest_room)
                await broadcast_room_state(latest_room, "game_started")
                continue

            if message_type == "finish_run":
                if latest_role != "host":
                    await send_ws_error(websocket, "Only host can finish the run.")
                    continue

                latest_room["game_started"] = False
                latest_room["run_id"] = None
                latest_room["host"]["ready"] = False
                if latest_room.get("guest"):
                    latest_room["guest"]["ready"] = False
                latest_room["updated_at"] = time.time()

                finish_payload = {
                    "type": "run_finished",
                    "reason": "host_finished"
                }
                await send_ws_message(websocket, finish_payload)
                await forward_to_role(latest_room, "guest", finish_payload)
                await broadcast_room_state(latest_room)
                continue

            if message_type == "kick_guest":
                if latest_role != "host":
                    await send_ws_error(websocket, "Only host can remove the other player.")
                    continue

                guest = latest_room.get("guest")
                if not guest:
                    continue

                guest_socket = guest.get("websocket")
                # Free the guest slot so a new player can join.
                latest_room["guest"] = None
                latest_room["updated_at"] = time.time()

                if guest_socket is not None:
                    await send_ws_message(guest_socket, {"type": "kicked", "reason": "host_removed"})
                    try:
                        await guest_socket.close(code=1000)
                    except Exception:
                        pass

                await broadcast_room_state(latest_room)
                continue

            if message_type == "coop_input":
                if latest_role != "guest":
                    await send_ws_error(websocket, "Only guest can send co-op input.")
                    continue

                payload = {
                    "type": "coop_input",
                    "payload": parsed.get("payload", {}),
                    "room_code": latest_room["room_code"],
                    "run_id": latest_room.get("run_id")
                }
                await forward_to_role(latest_room, "host", payload)
                continue

            if message_type == "coop_snapshot":
                if latest_role != "host":
                    await send_ws_error(websocket, "Only host can send co-op snapshots.")
                    continue

                payload = {
                    "type": "coop_snapshot",
                    "payload": parsed.get("payload", {}),
                    "room_code": latest_room["room_code"],
                    "run_id": latest_room.get("run_id")
                }
                await forward_to_role(latest_room, "guest", payload)
                continue

            if message_type == "ping":
                await send_ws_message(websocket, {"type": "pong"})
                continue

            await send_ws_error(websocket, "Unknown room message type.")

    except WebSocketDisconnect:
        pass
    finally:
        active_room = ACTIVE_ROOMS.get(normalized_code)
        if not active_room:
            return

        active_member, _ = find_room_member(active_room, token)
        if active_member and active_member.get("websocket") == websocket:
            active_member["websocket"] = None
            active_member["connected"] = False
            active_member["ready"] = False
            active_room["updated_at"] = time.time()

        await broadcast_room_state(active_room)

@router.post("/start-game")
async def start_game(data: StartGameRequest | None = Body(default=None)):
    # Clean up old runs occasionally
    if len(ACTIVE_RUNS) > 100:  # Arbitrary threshold
        cleanup_old_runs()

    run_mode = normalize_run_mode(data.mode if data else None)
    run_id = str(uuid4())

    ACTIVE_RUNS[run_id] = {
        "current_level": 1,
        "tanks_eliminated": {},  # level -> {tank_type: count}
        "total_eliminated": 0,
        "start_time": time.time(),
        "deaths": 0,
        "completed_levels": [],
        "mode": run_mode,
        "paused_ms": 0,
        # Endless-only state: the current wave's generated map and the enemy
        # counts used to validate eliminations server-side.
        "ended": False,
        "endless_wave_map": None,
        "endless_wave_counts": None
    }
    
    logger.info(f"Created new run with ID: {run_id}")
    
    return {"run_id": run_id, "message": "Game started", "level": 1, "mode": run_mode}

def handle_endless_game_event(run_id, game_state, tank_type):
    current_wave = game_state["current_level"]

    if game_state.get("ended"):
        return {"message": "Run already over", "game_complete": True, "pause_ms": 0}

    if TANK_TYPES[tank_type] == "player":
        # Endless is one life: dying ends the run.
        logger.info(f"{run_id} endless run over at wave {current_wave}")
        game_state["deaths"] += 1
        game_state["ended"] = True
        game_state["end_time"] = time.time()

        return {
            "message": "Run over - you were eliminated",
            "game_complete": True,
            "pause_ms": FAILED_TRANSITION_PAUSE_MS,
            "waves_completed": len(game_state["completed_levels"])
        }

    wave_counts = game_state.get("endless_wave_counts")
    if not wave_counts:
        raise HTTPException(status_code=400, detail="No active wave")

    if tank_type not in wave_counts:
        del ACTIVE_RUNS[run_id]
        raise HTTPException(status_code=400, detail="Run invalidated - Invalid tank type for current wave")

    if current_wave not in game_state["tanks_eliminated"]:
        game_state["tanks_eliminated"][current_wave] = {}

    wave_eliminations = game_state["tanks_eliminated"][current_wave]
    wave_eliminations[tank_type] = wave_eliminations.get(tank_type, 0) + 1

    if wave_eliminations[tank_type] > wave_counts[tank_type]:
        del ACTIVE_RUNS[run_id]
        raise HTTPException(status_code=400, detail="Run invalidated - Too many tanks eliminated")

    total_eliminated = sum(wave_eliminations.values())
    total_in_wave = sum(wave_counts.values())
    wave_complete = total_eliminated == total_in_wave

    logger.info(f"{run_id} endless wave {current_wave}: eliminated tank {tank_type} - {total_eliminated} of {total_in_wave}")

    response = {
        "message": "Tank elimination recorded",
        "tank_type": tank_type,
        "level_complete": wave_complete,
        "pause_ms": 0
    }

    if wave_complete:
        add_backend_pause(game_state, CLEARED_TRANSITION_PAUSE_MS)
        game_state["completed_levels"].append(current_wave)
        next_wave = current_wave + 1
        game_state["current_level"] = next_wave
        # Clear the cached wave so the next /level call generates a new one.
        game_state["endless_wave_map"] = None
        game_state["endless_wave_counts"] = None
        response["next_level"] = next_wave
        response["message"] = "Wave cleared! Advancing to next wave."
        response["pause_ms"] = CLEARED_TRANSITION_PAUSE_MS

    return response


@router.post("/game-event")
async def game_event(data: GameEventRequest):
    run_id = data.run_id
    tank_type = data.tank_type

    if run_id not in ACTIVE_RUNS:
        raise HTTPException(status_code=404, detail="Run not found")

    if tank_type not in TANK_TYPES:
        raise HTTPException(status_code=400, detail="Invalid tank type")

    game_state = ACTIVE_RUNS[run_id]
    current_level = game_state["current_level"]

    if is_endless_mode(game_state.get("mode")):
        return handle_endless_game_event(run_id, game_state, tank_type)

    if TANK_TYPES[tank_type] == "player":
      logger.info(f"{run_id} was eliminated")
      game_state["deaths"] += 1
      add_backend_pause(game_state, FAILED_TRANSITION_PAUSE_MS)
      
      # Reset tank eliminations for current level
      if current_level in game_state["tanks_eliminated"]:
          del game_state["tanks_eliminated"][current_level]
      
      return {
          "message": "Player eliminated - level reset",
          "level_reset": True,
          "current_level": current_level,
          "pause_ms": FAILED_TRANSITION_PAUSE_MS
      }
    else:
      # Validate level exists and tank type is valid
      if current_level not in LEVEL_METADATA:
          raise HTTPException(status_code=400, detail="Invalid level")
      
      level_info = LEVEL_METADATA[current_level]
      if tank_type not in level_info["enemy_tank_types"]:
          del ACTIVE_RUNS[run_id]
          raise HTTPException(status_code=400, detail="Run invalidated - Invalid tank type for current level")
      
      # Track elimination
      if current_level not in game_state["tanks_eliminated"]:
          game_state["tanks_eliminated"][current_level] = {}
      
      level_eliminations = game_state["tanks_eliminated"][current_level]
      level_eliminations[tank_type] = level_eliminations.get(tank_type, 0) + 1
      
      # Check if player eliminated more tanks than exist
      if level_eliminations[tank_type] > level_info["enemy_tank_types"][tank_type]:
          del ACTIVE_RUNS[run_id]
          raise HTTPException(status_code=400, detail="Run invalidated - Too many tanks eliminated")
      
      # Check if level is complete
      total_eliminated_this_level = sum(level_eliminations.values())
      level_complete = total_eliminated_this_level == level_info["total_enemy_tanks"]
      
      logger.info(f"{run_id} eliminated tank with ID {tank_type} - {total_eliminated_this_level} out of {level_info['total_enemy_tanks']}")
      
      response = {
          "message": "Tank elimination recorded",
          "tank_type": tank_type,
          "level_complete": level_complete,
          "pause_ms": 0
      }
      
      if level_complete:
        add_backend_pause(game_state, CLEARED_TRANSITION_PAUSE_MS)
        game_state["completed_levels"].append(current_level)
        
        # Check if next level exists before incrementing
        next_level = current_level + 1
        next_map_file = MAPS_DIR / f"level_{next_level}.txt"
        
        if next_map_file.exists():
            game_state["current_level"] = next_level
            response["next_level"] = next_level
            response["message"] = "Level complete! Advancing to next level."
            response["pause_ms"] = CLEARED_TRANSITION_PAUSE_MS
        else:
            response["game_complete"] = True
            response["message"] = "Congratulations! Game completed!"
            response["pause_ms"] = CLEARED_TRANSITION_PAUSE_MS

    return response

@router.post("/level")
async def get_current_level(data: RunRequest):
    run_id = data.run_id
    if run_id not in ACTIVE_RUNS:
        raise HTTPException(status_code=404, detail="Run not found")
    
    game_state = ACTIVE_RUNS[run_id]
    current_level = game_state["current_level"]

    if is_endless_mode(game_state.get("mode")):
        if game_state.get("ended"):
            return {"game_complete": True, "final_level": len(game_state["completed_levels"])}

        # Generate the wave once and cache it so retries get the same layout.
        if not game_state.get("endless_wave_map"):
            map_text, tank_counts = generate_endless_wave(current_level)
            game_state["endless_wave_map"] = map_text
            game_state["endless_wave_counts"] = tank_counts

        return PlainTextResponse(game_state["endless_wave_map"])

    map_file = MAPS_DIR / f"level_{current_level}.txt"
    if not map_file.exists():
        return {"game_complete": True, "final_level": current_level - 1}

    return PlainTextResponse(map_file.read_text())

@router.post("/get-final-stats")
async def get_final_stats(data: RunRequest):
    run_id = data.run_id
    
    if run_id not in ACTIVE_RUNS:
        raise HTTPException(status_code=404, detail="Run not found")
    
    game_state = ACTIVE_RUNS[run_id]
    
    # Freeze the time when final stats are requested
    if "end_time" not in game_state:
        game_state["end_time"] = time.time()
    
    # Calculate stats using frozen time
    current_level = game_state["current_level"]
    completed_levels = len(game_state["completed_levels"])
    total_time_seconds = get_elapsed_time_seconds(game_state)
    
    # Format time as MM:SS
    minutes = int(total_time_seconds // 60)
    seconds = int(total_time_seconds % 60)
    formatted_time = f"{minutes}:{seconds:02d}"
    
    # Determine completion status (endless runs never "complete" the game)
    if is_endless_mode(game_state.get("mode")):
        game_complete = False
    else:
        next_map_file = MAPS_DIR / f"level_{current_level + 1}.txt"
        game_complete = not next_map_file.exists() and current_level in game_state["completed_levels"]

    return {
        "stages_completed": completed_levels,
        "game_complete": game_complete,
        "time": formatted_time
    }

@router.post("/submit-score")
async def submit_score(data: SubmitScoreRequest, db: AsyncSession = Depends(get_db)):
    run_id = data.run_id
    username = data.username
    email = data.email
    
    if run_id not in ACTIVE_RUNS:
        raise HTTPException(status_code=404, detail="Run not found")
    
    game_state = ACTIVE_RUNS[run_id]
    run_mode = normalize_run_mode(game_state.get("mode") or data.mode)
    
    # Use frozen time
    if "end_time" not in game_state:
        game_state["end_time"] = time.time()
    
    completed_levels = len(game_state["completed_levels"])
    total_time_seconds = int(get_elapsed_time_seconds(game_state))
    deaths = game_state["deaths"]
    
    # Format time as MM:SS
    minutes = total_time_seconds // 60
    seconds = total_time_seconds % 60
    formatted_time = f"{minutes}:{seconds:02d}"
    
    leaderboard_model = LEADERBOARD_MODELS[run_mode]

    leaderboard_entry = leaderboard_model(
        username=username,
        completed_levels=completed_levels,
        time_seconds=total_time_seconds,
        formatted_time=formatted_time,
        deaths = deaths
    )
    
    db.add(leaderboard_entry)
    
    # Store email separately if provided
    if email and email.strip():
        contact_entry = ContactInfo(
            username=username,
            email=email.strip().lower()
        )
        db.add(contact_entry)
    
    await db.commit()
    
    # Clean up run
    del ACTIVE_RUNS[run_id]
    
    return {
        "message": "Score submitted successfully",
        "final_level": completed_levels,
        "time": formatted_time,
        "username": username,
        "mode": run_mode
    }
    
@router.get("/leaderboard")
async def get_leaderboard(
        page: int = 1, 
        limit: int = 50,
        mode: str = "solo",
        db: AsyncSession = Depends(get_db)
    ):
        page = max(page, 1)
        limit = max(1, min(limit, 100))
        offset = (page - 1) * limit
        run_mode = normalize_run_mode(mode)
        leaderboard_model = LEADERBOARD_MODELS[run_mode]
        
        # Order by stage (desc), then by time (asc) for same stage
        query = select(leaderboard_model).order_by(
            desc(leaderboard_model.completed_levels),
            leaderboard_model.time_seconds.asc(),
            leaderboard_model.date_submitted.asc()
        )
        
        result = await db.execute(query)
        entries = result.scalars().all()
        grouped_entries_by_name = {}
        grouped_entries = []

        for entry in entries:
            name_key = leaderboard_name_key(entry.username)
            serialized_entry = serialize_leaderboard_entry(entry)

            if name_key not in grouped_entries_by_name:
                grouped_entry = {
                    **serialized_entry,
                    "entry_count": 0,
                    "entries": []
                }
                grouped_entries_by_name[name_key] = grouped_entry
                grouped_entries.append(grouped_entry)

            grouped_entry = grouped_entries_by_name[name_key]
            grouped_entry["entry_count"] += 1
            grouped_entry["entries"].append(serialized_entry)

        paged_entries = grouped_entries[offset:offset + limit]
        
        return {
            "entries": paged_entries,
            "page": page,
            "limit": limit,
            "total_players": len(grouped_entries),
            "has_more": offset + limit < len(grouped_entries),
            "mode": run_mode
        }

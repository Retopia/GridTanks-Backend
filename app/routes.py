import json
import logging
import random
import time

from fastapi import APIRouter, HTTPException, Depends, WebSocket, WebSocketDisconnect, Body
from fastapi.responses import PlainTextResponse

from pathlib import Path
from typing import Dict
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc

from .database import get_db
from .models import LeaderboardEntry, CoopLeaderboardEntry, ContactInfo


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


def normalize_run_mode(raw_mode):
    mode = str(raw_mode or "solo").strip().lower()
    if mode == "coop":
        return "coop"
    return "solo"


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

    return {
        "room_code": room["room_code"],
        "run_id": room.get("run_id"),
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

@router.get("/health")
async def health_check():
    return {"status": "ok"}


@router.post("/rooms/create")
async def create_room(data: dict):
    cleanup_old_rooms()

    display_name = sanitize_display_name(data.get("display_name"), "Host")

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
async def join_room(data: dict):
    cleanup_old_rooms()

    room_code = normalize_room_code(data.get("room_code"))
    if len(room_code) != ROOM_CODE_LENGTH:
        raise HTTPException(status_code=400, detail="Invalid room code")

    room = ACTIVE_ROOMS.get(room_code)
    if not room:
        raise HTTPException(status_code=404, detail="Room not found")

    if room["game_started"]:
        raise HTTPException(status_code=409, detail="Room has already started")

    if room.get("guest"):
        raise HTTPException(status_code=409, detail="Room is full")

    display_name = sanitize_display_name(data.get("display_name"), "Guest")
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
async def start_game(data: dict | None = Body(default=None)):
    # Clean up old runs occasionally
    if len(ACTIVE_RUNS) > 100:  # Arbitrary threshold
        cleanup_old_runs()

    run_mode = normalize_run_mode((data or {}).get("mode"))
    run_id = str(uuid4())
    
    ACTIVE_RUNS[run_id] = {
        "current_level": 1,
        "tanks_eliminated": {},  # level -> {tank_type: count}
        "total_eliminated": 0,
        "start_time": time.time(),
        "deaths": 0,
        "completed_levels": [],
        "mode": run_mode
    }
    
    logger.info(f"Created new run with ID: {run_id}")
    
    return {"run_id": run_id, "message": "Game started", "level": 1, "mode": run_mode}

@router.post("/game-event")
async def game_event(data: dict):
    run_id = data.get("run_id") # str
    tank_type = data.get("tank_type") # int
    game_state = ACTIVE_RUNS[run_id]
    current_level = game_state["current_level"]
    
    if TANK_TYPES[tank_type] == "player":
      logger.info(f"{run_id} was eliminated")
      game_state["deaths"] += 1
      
      # Reset tank eliminations for current level
      if current_level in game_state["tanks_eliminated"]:
          del game_state["tanks_eliminated"][current_level]
      
      return {
          "message": "Player eliminated - level reset",
          "level_reset": True,
          "current_level": current_level
      }
    else:
      if run_id not in ACTIVE_RUNS:
          raise HTTPException(status_code=404, detail="Run not found")
      
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
          "level_complete": level_complete
      }
      
      if level_complete:
        game_state["completed_levels"].append(current_level)
        
        # Check if next level exists before incrementing
        next_level = current_level + 1
        next_map_file = MAPS_DIR / f"level_{next_level}.txt"
        
        if next_map_file.exists():
            game_state["current_level"] = next_level
            response["next_level"] = next_level
            response["message"] = "Level complete! Advancing to next level."
        else:
            response["game_complete"] = True
            response["message"] = "Congratulations! Game completed!"

    return response

@router.post("/level")
async def get_current_level(data: dict):
    run_id = data.get("run_id")
    if run_id not in ACTIVE_RUNS:
        raise HTTPException(status_code=404, detail="Run not found")
    
    game_state = ACTIVE_RUNS[run_id]
    current_level = game_state["current_level"]
    
    map_file = MAPS_DIR / f"level_{current_level}.txt"
    if not map_file.exists():
        return {"game_complete": True, "final_level": current_level - 1}
    
    return PlainTextResponse(map_file.read_text())

@router.post("/get-final-stats")
async def get_final_stats(data: dict):
    run_id = data.get("run_id")
    
    if run_id not in ACTIVE_RUNS:
        raise HTTPException(status_code=404, detail="Run not found")
    
    game_state = ACTIVE_RUNS[run_id]
    
    # Freeze the time when final stats are requested
    if "end_time" not in game_state:
        game_state["end_time"] = time.time()
    
    # Calculate stats using frozen time
    current_level = game_state["current_level"]
    completed_levels = len(game_state["completed_levels"])
    total_time_seconds = game_state["end_time"] - game_state["start_time"]
    
    # Format time as MM:SS
    minutes = int(total_time_seconds // 60)
    seconds = int(total_time_seconds % 60)
    formatted_time = f"{minutes}:{seconds:02d}"
    
    # Determine completion status
    next_map_file = MAPS_DIR / f"level_{current_level + 1}.txt"
    game_complete = not next_map_file.exists() and current_level in game_state["completed_levels"]
    
    return {
        "stages_completed": completed_levels,
        "game_complete": game_complete,
        "time": formatted_time
    }

@router.post("/submit-score")
async def submit_score(data: dict, db: AsyncSession = Depends(get_db)):
    run_id = data.get("run_id")
    username = data.get("username")
    email = data.get("email")
    
    # Input validation
    if not username:
        raise HTTPException(status_code=400, detail="Username is required")
    
    if len(username) > 20:
        raise HTTPException(status_code=400, detail="Username must be 20 characters or less")
    
    if email and len(email) > 255:
        raise HTTPException(status_code=400, detail="Email must be 255 characters or less")
    
    if run_id not in ACTIVE_RUNS:
        raise HTTPException(status_code=404, detail="Run not found")
    
    game_state = ACTIVE_RUNS[run_id]
    run_mode = normalize_run_mode(game_state.get("mode") or data.get("mode"))
    
    # Use frozen time
    if "end_time" not in game_state:
        game_state["end_time"] = time.time()
    
    completed_levels = len(game_state["completed_levels"])
    total_time_seconds = int(game_state["end_time"] - game_state["start_time"])
    deaths = game_state["deaths"]
    
    # Format time as MM:SS
    minutes = total_time_seconds // 60
    seconds = total_time_seconds % 60
    formatted_time = f"{minutes}:{seconds:02d}"
    
    leaderboard_model = CoopLeaderboardEntry if run_mode == "coop" else LeaderboardEntry

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
        offset = (page - 1) * limit
        run_mode = normalize_run_mode(mode)
        leaderboard_model = CoopLeaderboardEntry if run_mode == "coop" else LeaderboardEntry
        
        # Order by stage (desc), then by time (asc) for same stage
        query = select(leaderboard_model).order_by(
            desc(leaderboard_model.completed_levels),
            leaderboard_model.time_seconds.asc()
        ).offset(offset).limit(limit)
        
        result = await db.execute(query)
        entries = result.scalars().all()
        
        return {
            "entries": [
                {
                    "username": entry.username,
                    "completed_levels": entry.completed_levels,
                    "time": entry.formatted_time,
                    "date_submitted": entry.date_submitted.strftime("%m/%d/%Y")
                }
                for entry in entries
            ],
            "page": page,
            "limit": limit,
            "mode": run_mode
        }

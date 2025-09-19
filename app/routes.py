import time
import logging
from fastapi import APIRouter, HTTPException
from fastapi.responses import PlainTextResponse
from pathlib import Path
from typing import Dict
from uuid import uuid4

router = APIRouter()
logger = logging.getLogger("uvicorn")

BASE_DIR = Path(__file__).parent.parent
MAPS_DIR = BASE_DIR / "maps"

# Tank type mapping
TANK_TYPES = {
    3: "player",   # Player Tank - Speed: 2, Bullets: 5, Type: Normal
    4: "brown",    # Brown Tank - Speed: 0, Bullets: 1, Type: Normal
    5: "grey",     # Grey Tank - Speed: 1, Bullets: 1, Type: Normal
    6: "green",    # Green Tank - Speed: 1, Bullets: 1, Type: Fire
    7: "pink",     # Pink Tank - Speed: 2, Bullets: 5, Type: Normal
    8: "black",    # Black Tank - Speed: 3, Bullets: 5, Type: Fire
}

ACTIVE_RUNS: Dict[str, Dict] = {}  # run_id -> game_state
LEVEL_METADATA: Dict[int, Dict] = {}

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

@router.post("/start-game")
async def start_game():
    # Clean up old runs occasionally
    if len(ACTIVE_RUNS) > 100:  # Arbitrary threshold
        cleanup_old_runs()

    run_id = str(uuid4())
    
    ACTIVE_RUNS[run_id] = {
        "current_level": 1,
        "tanks_eliminated": {},  # level -> {tank_type: count}
        "total_eliminated": 0,
        "start_time": time.time(),
        "completed_levels": []
    }
    
    logger.info(f"Created new run with ID: {run_id}")
    
    return {"run_id": run_id, "message": "Game started", "level": 1}

@router.post("/game-event")
async def game_event(data: dict):
    run_id = data.get("run_id") # str
    tank_type = data.get("tank_type") # int
    game_state = ACTIVE_RUNS[run_id]
    current_level = game_state["current_level"]
    
    if TANK_TYPES[tank_type] == "player":
      logger.info(f"{run_id} was eliminated")
      
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
          raise HTTPException(status_code=400, detail="Invalid tank type for current level")
      
      # Track elimination
      if current_level not in game_state["tanks_eliminated"]:
          game_state["tanks_eliminated"][current_level] = {}
      
      level_eliminations = game_state["tanks_eliminated"][current_level]
      level_eliminations[tank_type] = level_eliminations.get(tank_type, 0) + 1
      
      # Check if player eliminated more tanks than exist
      if level_eliminations[tank_type] > level_info["enemy_tank_types"][tank_type]:
          raise HTTPException(status_code=400, detail="Too many tanks eliminated")
      
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
    final_level = game_state["current_level"]
    total_time_seconds = game_state["end_time"] - game_state["start_time"]
    
    # Format time as MM:SS
    minutes = int(total_time_seconds // 60)
    seconds = int(total_time_seconds % 60)
    formatted_time = f"{minutes}:{seconds:02d}"
    
    return {
        "final_level": final_level,
        "time": formatted_time
    }

@router.post("/submit-score")
async def submit_score(data: dict):
    run_id = data.get("run_id")
    username = data.get("username")
    email = data.get("email")
    
    if run_id not in ACTIVE_RUNS:
        raise HTTPException(status_code=404, detail="Run not found")
    
    game_state = ACTIVE_RUNS[run_id]
    
    # Use the frozen end_time if it exists, otherwise freeze it now
    if "end_time" not in game_state:
        game_state["end_time"] = time.time()
    
    # Calculate final score using frozen time
    final_level = game_state["current_level"]
    total_time_seconds = game_state["end_time"] - game_state["start_time"]
    
    # Format time as MM:SS
    minutes = int(total_time_seconds // 60)
    seconds = int(total_time_seconds % 60)
    formatted_time = f"{minutes}:{seconds:02d}"
    
    # Store in database/leaderboard here
    score_data = {
        "username": username,
        "stage_reached": final_level,
        "time": formatted_time,
        "date_submitted": time.time()
    }
    
    # Store email separately if provided
    if email:
        contact_data = {
            "username": username,
            "email": email,
            "submission_date": time.time()
        }
    
    # Clean up run
    del ACTIVE_RUNS[run_id]
    
    return {
        "message": "Score submitted successfully",
        "final_level": final_level,
        "time": formatted_time,
        "username": username
    }
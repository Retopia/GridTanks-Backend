import time
from fastapi import APIRouter, HTTPException
from fastapi.responses import PlainTextResponse
from pathlib import Path
from typing import Dict
from uuid import uuid4

router = APIRouter()

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
        # Extract level number from filename
        number_part = map_file.stem.replace("level_", "")
        if not number_part.isdigit():
            continue
            
        level_num = int(number_part)
        content = map_file.read_text()
        
        # Count tanks by type
        tank_counts = {}
        player_spawn = None
        
        for row_idx, line in enumerate(content.split('\n')):
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
    
    return {"run_id": run_id, "message": "Game started", "level": 1}

@router.post("/game-event")
async def game_event(run_id: str, tank_type: int):
    if run_id not in ACTIVE_RUNS:
        raise HTTPException(status_code=404, detail="Run not found")
    
    game_state = ACTIVE_RUNS[run_id]
    current_level = game_state["current_level"]
    
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

@router.get("/level")
async def get_current_level(run_id: str):
    if run_id not in ACTIVE_RUNS:
        raise HTTPException(status_code=404, detail="Run not found")
    
    game_state = ACTIVE_RUNS[run_id]
    current_level = game_state["current_level"]
    
    map_file = MAPS_DIR / f"level_{current_level}.txt"
    if not map_file.exists():
        return {"game_complete": True, "final_level": current_level - 1}
    
    return PlainTextResponse(map_file.read_text())
  
@router.post("/player-death")
async def player_death(run_id: str):
    # Reset level status, deduct life/end run
    pass

@router.post("/submit-score")
async def submit_score(run_id: str, username: str):
    if run_id not in ACTIVE_RUNS:
        raise HTTPException(status_code=404, detail="Run not found")
    
    game_state = ACTIVE_RUNS[run_id]
    
    # Calculate final score
    final_level = max(game_state["completed_levels"]) if game_state["completed_levels"] else 0
    total_time = time.time() - game_state["start_time"]
    
    # Storing logic goes here
    
    # Clean up run
    del ACTIVE_RUNS[run_id]
    
    return {
        "message": "Score submitted",
        "final_level": final_level,
        "time": total_time,
        "username": username
    }
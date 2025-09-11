from fastapi import APIRouter, HTTPException
from fastapi.responses import PlainTextResponse
from pathlib import Path
from typing import Dict, List
import re

router = APIRouter()

BASE_DIR = Path(__file__).parent.parent
MAPS_DIR = BASE_DIR / "maps"

LEVEL_METADATA: Dict[int, Dict] = {}

def preprocess_levels():
  

@router.get("/health")
async def health_check():
    return {"status": "ok"}

@router.post("/start-game")
async def start_game():
    # Create run id logic
    return {"run_id": "12345", "message": "Game started"}

@router.post("/game-event")
async def game_event():
    # Track tank eliminations
    return {"message": "Event recorded"}

@router.get("/level/{level}", response_class=PlainTextResponse)
async def get_level_file(level: int):
    map_file = MAPS_DIR / f"level_{level}.txt"
    
    if not map_file.exists():
        raise HTTPException(status_code=404, detail=f"Level {level} not found")
    
    return map_file.read_text()

@router.post("/submit-score")
async def submit_score():
    # Save final score
    return {"message": "Score submitted"}
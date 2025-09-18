from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes import router
from dotenv import load_dotenv
import os

load_dotenv()

# Get environment
is_development = os.getenv("ENVIRONMENT") == "development"

origins = [
    "http://localhost:5173",
    "http://localhost:3000", 
] if is_development else [
    "https://gridtanks.net",
    "https://www.gridtanks.net",
    "https://tanks.prestontang.dev",
    "https://www.tanks.prestontang.dev",
]

app = FastAPI(title="GridTanks API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(router)

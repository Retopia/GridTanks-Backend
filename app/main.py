from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes import router
from dotenv import load_dotenv
import os
from contextlib import asynccontextmanager
from .database import create_tables

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

@asynccontextmanager
async def lifespan(app: FastAPI):
    await create_tables()
    yield

app = FastAPI(
    title="GridTanks API", 
    version="1.0.0", 
    lifespan=lifespan,
    # Disable docs in production
    docs_url="/docs" if is_development else None,
    redoc_url="/redoc" if is_development else None,
    openapi_url="/openapi.json" if is_development else None
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(router)
from sqlalchemy import Column, Integer, String, DateTime, Float, Boolean, Index, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from datetime import datetime

Base = declarative_base()

class LeaderboardEntry(Base):
    __tablename__ = "leaderboard"
    
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(20), nullable=False, index=True)
    completed_levels = Column(Integer, nullable=False)
    time_seconds = Column(Integer, nullable=False)  # Store as seconds for sorting
    formatted_time = Column(String(10), nullable=False)  # Display format like "12:47"
    date_submitted = Column(DateTime, default=func.now(), nullable=False, index=True)
    
    # Add composite index for leaderboard queries
    __table_args__ = (
        Index('idx_leaderboard_ranking', 'completed_levels', 'time_seconds'),
    )

class ContactInfo(Base):
    __tablename__ = "contact_info"
    
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(20), nullable=False, index=True)
    email = Column(String(255), nullable=False)
    submission_date = Column(DateTime, default=func.now(), nullable=False)
    
    # Prevent duplicate emails per username
    __table_args__ = (
        UniqueConstraint('username', 'email', name='uq_username_email'),
    )
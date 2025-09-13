# File: src/models/youtube_models.py

from sqlalchemy import (
    Column, String, BigInteger, Integer, DateTime, Text,
    PrimaryKeyConstraint, Numeric, Boolean, ARRAY, Float
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class Video(Base):
    __tablename__ = 'videos'
    
    # --- Original Columns from CSV ---
    kind = Column(Text)
    video_id = Column(Text, primary_key=True)
    published_at = Column(DateTime(timezone=True))
    channel_id = Column(Text)
    title = Column(Text)
    description = Column(Text)
    tags = Column(Text)
    default_language = Column(Text)
    default_audio_language = Column(Text)
    content_duration = Column(Text)
    view_count = Column(Numeric)
    like_count = Column(Numeric)
    favourite_count = Column(Numeric)
    comment_count = Column(Numeric)
    topic_categories = Column(Text)
    
    # === NEW CLEANED & STRUCTURED COLUMNS ===
    description_clean = Column(Text)
    duration_hours = Column(Float)
    duration_minutes = Column(Float)
    duration_seconds = Column(Integer)
    language_name = Column(String(100), index=True)
    topic_categories_clean = Column(ARRAY(Text))
    is_cleaned = Column(Boolean, default=False, nullable=False)

class Comment(Base):
    __tablename__ = 'comments'
    __table_args__ = (
        PrimaryKeyConstraint('comment_id', 'published_at'),
        {},
    )
    
    kind = Column(Text)
    comment_id = Column(BigInteger)
    parent_comment_id = Column(BigInteger)
    channel_id = Column(BigInteger)
    video_id = Column(Text) # ForeignKey is removed for the datathon
    author_id = Column(BigInteger)
    text_original = Column(Text)
    like_count = Column(Numeric)
    published_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True))
    
    # === AI ENRICHMENT COLUMNS ===
    sentiment = Column(String(50), index=True)
    category = Column(String(100), index=True) 
    quality_score = Column(Integer)
    is_spam = Column(Boolean, default=False, index=True)
    language = Column(String(50), index=True)
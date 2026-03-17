# File: src/models/youtube_models.py

from sqlalchemy import (
    Column, String, BigInteger, Integer, DateTime, Text,
    PrimaryKeyConstraint, Numeric, Boolean, ARRAY, Float
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class VideoStaging(Base):
    __tablename__ = 'videos_staging'
    
    # --- Exact representation of raw CSV ---
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


class Video(Base):
    __tablename__ = 'videos'
    
    video_id = Column(Text, primary_key=True)
    published_at = Column(DateTime(timezone=True))
    channel_id = Column(Text)
    title = Column(Text)
    description = Column(Text)  # Cleaned description
    tags = Column(Text)
    language_name = Column(String(100), index=True)
    duration_seconds = Column(Integer)
    view_count = Column(Numeric)
    like_count = Column(Numeric)
    favourite_count = Column(Numeric)
    comment_count = Column(Numeric)
    topic_categories = Column(ARRAY(Text))

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
    sentiment_score_positive = Column(Float)
    sentiment_score_negative = Column(Float)
    sentiment_score_neutral = Column(Float)
    
    category = Column(String(100), index=True) 
    category_confidence = Column(Float)
    
    quality_score = Column(Integer)
    is_spam = Column(Boolean, default=False, index=True)
    language = Column(String(50), index=True)
    language_confidence = Column(Float)
    
    comment_length = Column(Integer)
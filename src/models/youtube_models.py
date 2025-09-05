# File: src/models/youtube_models.py (Datathon Hotfix Version)

from sqlalchemy import (
    Column,
    String,
    BigInteger,
    Integer,
    DateTime,
    Text,
    PrimaryKeyConstraint,
    Numeric,
    Boolean
    # We remove ForeignKey from the imports as it is no longer used
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class Video(Base):
    __tablename__ = 'videos'
    
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
    
    # === CHANGE 1: COMMENT OUT THE RELATIONSHIP ===
    # Since there's no longer an enforced ForeignKey, this ORM-level
    # relationship should also be disabled to avoid confusion.
    # comments = relationship("Comment", back_populates="video")

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
    
    # === CHANGE 2: REMOVE THE ForeignKey CONSTRAINT ===
    # This is the most important change. We are now just defining a simple
    # Text column. The data will be loaded, but the database will not check
    # if the video_id exists in the 'videos' table.
    video_id = Column(Text) # The ForeignKey('videos.video_id') part is removed.
    
    author_id = Column(BigInteger)
    text_original = Column(Text)
    like_count = Column(Numeric)
    published_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True))
    
    # All the AI enrichment and index columns are perfect, no changes needed here.
    sentiment = Column(String(50), index=True)
    category = Column(String(100), index=True) 
    quality_score = Column(Integer)
    is_spam = Column(Boolean, default=False, index=True)
    language = Column(String(50), index=True)

    # === CHANGE 3: COMMENT OUT THE CORRESPONDING RELATIONSHIP ===
    # video = relationship("Video", back_populates="comments")
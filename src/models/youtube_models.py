from sqlalchemy import (
    Column,
    String,
    BigInteger,
    Integer,
    DateTime,
    Text,
    PrimaryKeyConstraint,
    Numeric,
    Boolean,
    ForeignKey
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
    
    # Optional: Defines the "one-to-many" relationship from the Video's perspective
    comments = relationship("Comment", back_populates="video")

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
    
    # === OPTIMIZATION 1: ADDED FOREIGN KEY ===
    # This links this column to the 'videos.video_id' primary key.
    video_id = Column(Text, ForeignKey('videos.video_id'))
    
    author_id = Column(BigInteger)
    text_original = Column(Text)
    like_count = Column(Numeric)
    published_at = Column(DateTime(timezone=True), nullable=False)
    updated_at = Column(DateTime(timezone=True))
    
    # === NEW AI ENRICHMENT COLUMNS ===
    # === OPTIMIZATION 2: ADDED INDEXES ===
    # index=True tells the database to create an index on these columns
    # for much faster filtering and grouping in Grafana.
    sentiment = Column(String(50), index=True)
    category = Column(String(100), index=True) 
    quality_score = Column(Integer)
    is_spam = Column(Boolean, default=False, index=True)

    # Optional: Defines the "many-to-one" relationship from the Comment's perspective
    video = relationship("Video", back_populates="comments")
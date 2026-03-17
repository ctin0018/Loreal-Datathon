import asyncio
import logging
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

# Import the engine and session factory from our new database.py
from .database import async_engine, AsyncSessionLocal
from .models.youtube_models import Base

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def create_all_tables():
    """Creates all tables defined in the Base metadata from models.py."""
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        
        # Add the new columns manually in case the table already existed
        # Using IF NOT EXISTS (requires Postgres 9.6+)
        alter_statements = [
            "ALTER TABLE comments ADD COLUMN IF NOT EXISTS sentiment_score_positive FLOAT;",
            "ALTER TABLE comments ADD COLUMN IF NOT EXISTS sentiment_score_negative FLOAT;",
            "ALTER TABLE comments ADD COLUMN IF NOT EXISTS sentiment_score_neutral FLOAT;",
            "ALTER TABLE comments ADD COLUMN IF NOT EXISTS category_confidence FLOAT;",
            "ALTER TABLE comments ADD COLUMN IF NOT EXISTS language_confidence FLOAT;",
            "ALTER TABLE comments ADD COLUMN IF NOT EXISTS comment_length INTEGER;"
        ]
        for statement in alter_statements:
            try:
                await conn.execute(text(statement))
            except Exception as e:
                logger.warning(f"Error adding column (it might already exist): {e}")

    logger.info("All tables (and new columns) created successfully.")

async def create_hypertable_for_comments(db: AsyncSession):
    """Converts the comments table into a TimescaleDB hypertable."""
    create_hypertable_sql = text(
        """
        SELECT create_hypertable('comments', 'published_at', if_not_exists => TRUE);
        """
    )
    try:
        await db.execute(create_hypertable_sql)
        await db.commit()
        logger.info("Hypertable for 'comments' created successfully.")
    except SQLAlchemyError as e:
        await db.rollback()
        logger.error(f"Error creating hypertable for comments: {e}")
        raise

async def bootstrap_database():
    """A main function to orchestrate the entire database setup."""
    logger.info("Bootstrapping database...")
    await create_all_tables()
    async with AsyncSessionLocal() as db:
        await create_hypertable_for_comments(db)
    logger.info("Database bootstrap complete.")

if __name__ == "__main__":
    asyncio.run(bootstrap_database())
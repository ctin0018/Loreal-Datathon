import os
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

load_dotenv()

# Configuration 
# This pulls credentials from your .env file
DATABASE_URL = (
    f"postgresql+asyncpg://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
    f"@localhost:{os.getenv('POSTGRES_PORT', 5432)}/{os.getenv('POSTGRES_DB')}"
)

# Engine and Session Definition 
# The engine is the core interface to the database. Just need to create it once.
async_engine = create_async_engine(DATABASE_URL, echo=False)

# The sessionmaker is a "factory" for creating new database sessions.
# It ensures each new session is configured correctly.
AsyncSessionLocal = sessionmaker(
    bind=async_engine, 
    class_=AsyncSession, 
    expire_on_commit=False
)
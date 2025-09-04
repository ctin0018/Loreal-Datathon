from aiohttp import ClientSession, ClientTimeout, TCPConnector
from contextlib import asynccontextmanager
import logging

logger = logging.getLogger(__name__)

# This is the exact helper you requested, ready for use.
@asynccontextmanager
async def get_api_session():
    """Provides a managed aiohttp ClientSession."""
    # A generous timeout for potentially slow LLM responses
    timeout = ClientTimeout(total=300) 
    # connector allows us to manage settings like SSL verification if needed
    connector = TCPConnector(ssl=False) # In production, you'd likely want ssl=True
    
    session = None
    try:
        session = ClientSession(connector=connector, timeout=timeout)
        logger.info("API Session created.")
        yield session
    finally:
        if session:
            await session.close()
            logger.info("API Session closed.")
import asyncio
import logging
import os
from typing import List, Dict
import aiohttp
from dotenv import load_dotenv
from sqlalchemy import update, select, bindparam
from sqlalchemy.ext.asyncio import AsyncSession

# Import the session factory from database.py
from ..database import AsyncSessionLocal
from ..models.youtube_models import Comment
# Import the api_helpers correctly
from ..services.comment_sense.api_helpers import get_api_session

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
load_dotenv()

# Configuration
LLM_API_ENDPOINT = "https://api.openai.com/v1/chat/completions"
LLM_API_KEY = os.getenv("OPENAI_API_KEY")

async def fetch_unprocessed_comments(db: AsyncSession, batch_size: int = 100) -> List[Comment]:
    logger.info(f"Fetching up to {batch_size} unprocessed comments...")
    stmt = select(Comment).where(Comment.sentiment == None).limit(batch_size)
    result = await db.execute(stmt)
    comments = result.scalars().all()
    logger.info(f"Found {len(comments)} comments to process.")
    return comments

async def bulk_update_comments(db: AsyncSession, enriched_data: List[Dict]):
    if not enriched_data:
        return
    stmt = (
        update(Comment)
        .where(Comment.comment_id == bindparam('b_comment_id'))
        .values(
            sentiment=bindparam('sentiment'),
            category=bindparam('category'),
            quality_score=bindparam('quality_score')
        )
    )
    update_params = [
        {'b_comment_id': data['comment_id'], 'sentiment': data['sentiment'], 'category': data['category'], 'quality_score': data['quality_score']}
        for data in enriched_data
    ]
    await db.execute(stmt, update_params)
    logger.info(f"Bulk updated {len(enriched_data)} comments in the database.")


async def analyze_comment_batch(session: aiohttp.ClientSession, comments: List[Comment]) -> List[Dict]:
    if not comments:
        return []
    prompt = "Analyze these comments... return a JSON array..."
    headers = {"Authorization": f"Bearer {LLM_API_KEY}"}
    payload = {"model": "gpt-3.5-turbo", "messages": [{"role": "user", "content": prompt}]}
    try:
        async with session.post(LLM_API_ENDPOINT, headers=headers, json=payload) as response:
            response.raise_for_status()
            api_result_json = await response.json()
            logger.info("Simulating successful API call and parsing.")
            parsed_results = [
                {"comment_id": c.comment_id, "sentiment": "Positive", "category": "General Feedback", "quality_score": 3} 
                for c in comments
            ]
            return parsed_results
    except aiohttp.ClientError as e:
        logger.error(f"API request failed: {e}")
        return []
    except Exception as e:
        logger.error(f"An unexpected error occurred during analysis: {e}")
        return []

# Main Orchestration
async def run_enrichment_pipeline():
    """The main orchestration function to run the ETL process."""
    # Use the get_api_session helper for proper session management
    async with get_api_session() as api_session:
        while True:
            # Use our imported session factory
            async with AsyncSessionLocal() as db:
                comments_to_process = await fetch_unprocessed_comments(db)
                
                if not comments_to_process:
                    logger.info("No more comments to process. Pipeline is complete.")
                    break
                
                enriched_results = await analyze_comment_batch(api_session, comments_to_process)
                
                if enriched_results:
                    await bulk_update_comments(db, enriched_results)
                    await db.commit()
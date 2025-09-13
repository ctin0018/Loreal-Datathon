import asyncio
import logging
import os
import json
from typing import List, Dict, Any
import aiohttp
from dotenv import load_dotenv
import sqlalchemy.exc

from sqlalchemy import update, select, bindparam, String, Integer, Boolean
from sqlalchemy.dialects.postgresql import BIGINT
from sqlalchemy.ext.asyncio import AsyncSession
from ..database import AsyncSessionLocal
from ..models.youtube_models import Comment
from ..services.comment_sense.api_helpers import get_api_session

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')
logger = logging.getLogger(__name__)
load_dotenv()

# Update CONFIG
CONFIG = {
    "DB_BATCH_SIZE": 25,  # Reduced from 50 to 25
    "LLM_API_ENDPOINT": "https://api.anthropic.com/v1/messages",
    "LLM_API_KEY": os.getenv("ANTHROPIC_API_KEY"),
    "LLM_MODEL": "claude-3-7-sonnet-20250219",
    "MAX_RETRIES": 3
}

class CommentEnrichmentService:
    def __init__(self, db_session_factory):
        self.async_session_factory = db_session_factory
        self.comments_to_process = []  # Add this line
        if not CONFIG["LLM_API_KEY"]: raise ValueError("ANTHROPIC_API_KEY not found.")

    async def _fetch_unprocessed_comments(self, db: AsyncSession) -> List[Comment]:
        stmt = select(Comment).where(Comment.sentiment == None).limit(CONFIG['DB_BATCH_SIZE'])
        result = await db.execute(stmt)
        return result.scalars().all()

    def _clean_json_response(self, content: str) -> str:
        """Helper function to clean and validate JSON response"""
        # Remove markdown
        content = content.replace("```json", "").replace("```", "").strip()
        
        # Handle potential truncation
        if not content.endswith("]"):
            last_complete_brace = content.rfind("}")
            if last_complete_brace != -1:
                content = content[:last_complete_brace+1] + "]"
        
        return content

    # === AGENT 1: The Triage Agent ===
    async def _triage_agent(self, session: aiohttp.ClientSession, comments: List[Comment]) -> List[Dict[str, Any]]:
        """
        First-pass analysis to detect language and identify spam/irrelevant comments.
        """
        if not comments: return []

        prompt = """You are a Triage Agent for YouTube comments. Analyze the following comments and for each one, provide:
1.  `language`: The detected language of the comment (e.g., "English", "Spanish", "Hindi").
2.  `is_spam`: A boolean (true/false) indicating if the comment is obvious spam, an advertisement, or just gibberish.
3.  `is_relevant`: A boolean (true/false) indicating if the comment is relevant to a beauty product, brand, or topic. A simple comment like "nice video" is considered not relevant for deep analysis.

Return your analysis as a single, valid JSON array of objects with keys "comment_id", "language", "is_spam", and "is_relevant".

COMMENTS:
""" + "\n".join([f'{c.comment_id}: "{c.text_original}"' for c in comments])

        headers = {
            "x-api-key": CONFIG["LLM_API_KEY"],
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": CONFIG["LLM_MODEL"],
            "max_tokens": 4000,  # Increased max_tokens
            "messages": [{"role": "user", "content": prompt}]
        }

        for attempt in range(CONFIG["MAX_RETRIES"]):
            try:
                async with session.post(CONFIG['LLM_API_ENDPOINT'], headers=headers, json=payload) as response:
                    response.raise_for_status()
                    api_result = await response.json()
                    
                    if "content" not in api_result or not api_result["content"]:
                        logger.error("No content in API response")
                        continue

                    content = api_result["content"][0]["text"]
                    logger.debug(f"Raw API response: {content}")
                    
                    # Clean and validate JSON
                    content = self._clean_json_response(content)
                    
                    if not content.startswith("["):
                        logger.error(f"Invalid JSON structure - doesn't start with '[': {content[:100]}...")
                        continue

                    try:
                        parsed_content = json.loads(content)
                        if not isinstance(parsed_content, list):
                            logger.error("JSON response is not an array")
                            continue
                        return parsed_content
                    except json.JSONDecodeError as e:
                        logger.error(f"JSON parsing error: {e}")
                        logger.error(f"Cleaned content: {content[:200]}...")  # Log first 200 chars
                        continue

            except Exception as e:
                logger.error(f"Triage Agent attempt {attempt + 1} failed: {e}")
                if attempt == CONFIG["MAX_RETRIES"] - 1:
                    return []
                await asyncio.sleep(1)  # Wait before retry

        return []

    # === AGENT 2: The Deep Analysis Agent ===
    async def _deep_analysis_agent(self, session: aiohttp.ClientSession, comments: List[Comment]) -> List[Dict[str, Any]]:
        """
        Performs detailed sentiment and category analysis on relevant, non-spam comments.
        """
        if not comments: return []

        prompt = """You are a Deep Analysis Agent for beauty-related YouTube comments. For each of the following relevant comments, provide:
1. `sentiment`: The sentiment of the comment (Positive, Negative, or Neutral).
2. `category`: The primary category from this list: [Product Inquiry, Ingredient Mention, Positive Experience, Negative Experience, Application Tip, Brand Loyalty, General Feedback].
3. `quality_score`: A score from 1 (low effort) to 5 (insightful and detailed).

Return your analysis as a single, valid JSON array of objects with keys "comment_id", "sentiment", "category", and "quality_score".

RELEVANT COMMENTS:
""" + "\n".join([f'{c.comment_id}: "{c.text_original}"' for c in comments])

        headers = {
            "x-api-key": CONFIG["LLM_API_KEY"],
            "anthropic-version": "2023-06-01",  # Use the correct version for your account
            "Content-Type": "application/json"
        }
        payload = {"model": CONFIG["LLM_MODEL"], "max_tokens": 2000, "messages": [{"role": "user", "content": prompt}]}
        for attempt in range(CONFIG["MAX_RETRIES"]):
            try:
                async with session.post(CONFIG['LLM_API_ENDPOINT'], headers=headers, json=payload) as response:
                    response.raise_for_status()
                    api_result = await response.json()
                    
                    if "content" not in api_result or not api_result["content"]:
                        logger.error("No content in API response")
                        continue

                    content = api_result["content"][0]["text"]
                    logger.debug(f"Raw API response: {content}")
                    
                    # Clean and validate JSON
                    content = self._clean_json_response(content)
                    
                    try:
                        parsed_content = json.loads(content)
                        if not isinstance(parsed_content, list):
                            logger.error("JSON response is not an array")
                            continue
                        return parsed_content
                    except json.JSONDecodeError as e:
                        logger.error(f"JSON parsing error: {e}")
                        logger.error(f"Cleaned content: {content[:200]}...")
                        continue

            except Exception as e:
                logger.error(f"Deep Analysis Agent attempt {attempt + 1} failed: {e}")
                if attempt == CONFIG["MAX_RETRIES"] - 1:
                    return []
                await asyncio.sleep(1)  # Wait before retry

        return []

    async def _bulk_update_comments(self, db: AsyncSession, enriched_data: List[Dict]):
        if not enriched_data:
            return

        stmt = (
            update(Comment)
            .where(Comment.comment_id == bindparam('b_comment_id', type_=BIGINT))  # Changed to BIGINT
            .values(
                sentiment=bindparam('sentiment', type_=String),
                category=bindparam('category', type_=String),
                quality_score=bindparam('quality_score', type_=Integer),
                language=bindparam('language', type_=String),
                is_spam=bindparam('is_spam', type_=Boolean)
            )
        )

        update_params = [
            {
                'b_comment_id': int(comment['comment_id']),  # Convert to integer
                'sentiment': comment.get('sentiment', 'N/A'),
                'category': comment.get('category', 'N/A'),
                'quality_score': comment.get('quality_score'),
                'language': comment.get('language'),
                'is_spam': comment.get('is_spam', False)
            }
            for comment in enriched_data
        ]

        if update_params:
            try:
                # Get raw connection
                conn = await db.connection()
                await conn.execute(stmt, update_params)
                await db.commit()
                logger.info(f"Bulk updated {len(update_params)} comments.")
                
            except sqlalchemy.exc.IntegrityError as e:
                logger.error(f"Database integrity error: {e}")
                await db.rollback()
                
            except sqlalchemy.exc.OperationalError as e:
                logger.error(f"Database operation error: {e}")
                await db.rollback()
                
            except sqlalchemy.exc.DBAPIError as e:
                logger.error(f"Database API error: {e}")
                await db.rollback()
                
            except Exception as e:
                logger.error(f"Unexpected error during bulk update: {e}")
                await db.rollback()

    # --- THE NEW ORCHESTRATION LOGIC ---
    async def run(self):
        """Orchestrates the new agentic enrichment process."""
        logger.info("Starting AGENTIC comment enrichment service...")
        async with get_api_session() as api_session:
            while True:
                async with self.async_session_factory() as db:
                    # Store comments in instance variable
                    self.comments_to_process = await self._fetch_unprocessed_comments(db)
                    if not self.comments_to_process:
                        logger.info("No more comments to process. Complete.")
                        break

                    # Step 1: Run the Triage Agent on all comments in the batch
                    triage_results = await self._triage_agent(api_session, self.comments_to_process)
                    if not triage_results: continue # Skip batch on error

                    # Step 2: Prepare for the next stage
                    final_results = {res['comment_id']: res for res in triage_results}
                    relevant_comments_for_deep_analysis = []
                    for comment in self.comments_to_process:
                        triage_info = final_results.get(comment.comment_id)
                        if triage_info and triage_info.get('is_relevant') and not triage_info.get('is_spam'):
                            relevant_comments_for_deep_analysis.append(comment)

                    # Step 3: Run the Deep Analysis Agent ONLY on the relevant comments
                    if relevant_comments_for_deep_analysis:
                        deep_analysis_results = await self._deep_analysis_agent(api_session, relevant_comments_for_deep_analysis)
                        # Merge the deep analysis results back into our final results
                        for res in deep_analysis_results:
                            if res['comment_id'] in final_results:
                                final_results[res['comment_id']].update(res)

                    # Step 4: Update the database with the combined results
                    await self._bulk_update_comments(db, list(final_results.values()))
                    await db.commit()

                await asyncio.sleep(2)

async def run_enrichment_pipeline():
    service = CommentEnrichmentService(AsyncSessionLocal)
    await service.run()
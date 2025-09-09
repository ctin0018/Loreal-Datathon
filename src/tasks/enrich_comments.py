import asyncio
import logging
import os
import json
from typing import List, Dict, Any
import aiohttp
from dotenv import load_dotenv

from sqlalchemy import update, select, bindparam
from sqlalchemy.ext.asyncio import AsyncSession
from ..database import AsyncSessionLocal
from ..models.youtube_models import Comment
from ..services.comment_sense.api_helpers import get_api_session

# ... (logging, dotenv, and CONFIG setup is the same as before) ...
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')
logger = logging.getLogger(__name__)
load_dotenv()

CONFIG = {
    "DB_BATCH_SIZE": 50, # Smaller batch size for more complex processing
    "LLM_API_ENDPOINT": "https://api.anthropic.com/v1/messages",
    "LLM_API_KEY": os.getenv("ANTHROPIC_API_KEY"),
    "LLM_MODEL": "claude-3-7-sonnet-20250219"
}

class CommentEnrichmentService:
    def __init__(self, db_session_factory):
        # ... (init is the same) ...
        self.async_session_factory = db_session_factory
        if not CONFIG["LLM_API_KEY"]: raise ValueError("ANTHROPIC_API_KEY not found.")

    async def _fetch_unprocessed_comments(self, db: AsyncSession) -> List[Comment]:
        # ... (fetch is the same) ...
        stmt = select(Comment).where(Comment.sentiment == None).limit(CONFIG['DB_BATCH_SIZE'])
        result = await db.execute(stmt)
        return result.scalars().all()

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

        headers = {"x-api-key": CONFIG["LLM_API_KEY"], "anthropic-version": "2025-02-19", "Content-Type": "application/json"}
        payload = {"model": CONFIG["LLM_MODEL"], "max_tokens": 2000, "messages": [{"role": "user", "content": prompt}]}

        try:
            async with session.post(CONFIG['LLM_API_ENDPOINT'], headers=headers, json=payload) as response:
                response.raise_for_status()
                api_result = await response.json()
                content = api_result["content"][0]["text"]
                return json.loads(content) # Assuming direct JSON response for simplicity here
        except Exception as e:
            logger.error(f"Triage Agent failed: {e}")
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

        # ... (API call logic is identical to Triage Agent) ...
        headers = {"x-api-key": CONFIG["LLM_API_KEY"], "anthropic-version": "2025-02-19", "Content-Type": "application/json"}
        payload = {"model": CONFIG["LLM_MODEL"], "max_tokens": 2000, "messages": [{"role": "user", "content": prompt}]}
        try:
            async with session.post(CONFIG['LLM_API_ENDPOINT'], headers=headers, json=payload) as response:
                response.raise_for_status()
                api_result = await response.json()
                content = api_result["content"][0]["text"]
                return json.loads(content)
        except Exception as e:
            logger.error(f"Deep Analysis Agent failed: {e}")
            return []

    async def _bulk_update_comments(self, db: AsyncSession, enriched_data: List[Dict]):
        # ... (bulk update logic is the same, but now handles more fields) ...
        if not enriched_data: return
        stmt = (
            update(Comment)
            .where(Comment.comment_id == bindparam('b_comment_id'))
            .values(
                sentiment=bindparam('sentiment'),
                category=bindparam('category'),
                quality_score=bindparam('quality_score'),
                language=bindparam('language'),
                is_spam=bindparam('is_spam')
            )
        )
        update_params = [
            {
                'b_comment_id': data['comment_id'],
                'sentiment': data.get('sentiment', 'N/A'),
                'category': data.get('category', 'N/A'),
                'quality_score': data.get('quality_score'),
                'language': data.get('language'),
                'is_spam': data.get('is_spam', False)
            }
            for data in enriched_data
        ]
        await db.execute(stmt, update_params)
        logger.info(f"Bulk updated {len(enriched_data)} comments.")

    # --- THE NEW ORCHESTRATION LOGIC ---
    async def run(self):
        """Orchestrates the new agentic enrichment process."""
        logger.info("Starting AGENTIC comment enrichment service...")
        async with get_api_session() as api_session:
            while True:
                async with self.async_session_factory() as db:
                    comments_to_process = await self._fetch_unprocessed_comments(db)
                    if not comments_to_process:
                        logger.info("No more comments to process. Complete.")
                        break

                    # Step 1: Run the Triage Agent on all comments in the batch
                    triage_results = await self._triage_agent(api_session, comments_to_process)
                    if not triage_results: continue # Skip batch on error

                    # Step 2: Prepare for the next stage
                    final_results = {res['comment_id']: res for res in triage_results}
                    relevant_comments_for_deep_analysis = []
                    for comment in comments_to_process:
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
import asyncio
import logging
import os
from typing import List, Dict
import aiohttp
import json
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
LLM_API_ENDPOINT = "https://api.anthropic.com/v1/messages"
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
            quality_score=bindparam('quality_score'),
            language=bindparam('language') # <-- ADD THIS LINE
        )
    )
    
    update_params = [
        {
            'b_comment_id': data['comment_id'],
            'sentiment': data.get('sentiment'), # Use .get() for safety
            'category': data.get('category'),
            'quality_score': data.get('quality_score'),
            'language': data.get('language') # <-- ADD THIS LINE
        }
        for data in enriched_data
    ]

    await db.execute(stmt, update_params)
    logger.info(f"Bulk updated {len(enriched_data)} comments in the database.")

async def analyze_comment_batch(session: aiohttp.ClientSession, comments: List[Comment]) -> List[Dict]:
    """
    Sends a batch of multilingual comments to the LLM for analysis.
    """
    if not comments:
        return []

    # === THE NEW, REFINED PROMPT ===
    # This is the prompt your AI Specialist would design.
    # It explicitly handles multiple languages and asks for a new 'language' field.
    prompt_instructions = """Analyze the following YouTube comments, which may be in various languages. For each comment, provide its detected language, its sentiment (Positive, Negative, or Neutral), and assign it ONE category from this list: [Product Inquiry, Ingredient Mention, Positive Experience, Negative Experience, Application Tip, Brand Loyalty, Spam, General Feedback].

Return your analysis as a single, valid JSON array of objects. Each object must have four keys: "comment_id" (integer), "language" (string), "sentiment" (string), and "category" (string). Do not include any text, explanations, or markdown outside of the JSON array.
"""

    comments_text = "\n".join([f'{c.comment_id}: "{c.text_original}"' for c in comments])
    
    final_prompt = f"{prompt_instructions}\n\nCOMMENTS:\n{comments_text}"

    headers = {"Authorization": f"Bearer {LLM_API_KEY}"}
    payload = {
        "model": "gpt-3.5-turbo",
        "messages": [{"role": "user", "content": final_prompt}],
        # "response_format": {"type": "json_object"} # Use with newer models for guaranteed JSON
    }

    try:
        async with session.post(LLM_API_ENDPOINT, headers=headers, json=payload) as response:
            response.raise_for_status()
            api_result_json = await response.json()
            
            # --- The Parsing Logic for your Backend Engineer ---
            content = api_result_json["choices"][0]["message"]["content"]
            try:
                # Find the start and end of the JSON array
                json_start = content.find('[')
                json_end = content.rfind(']') + 1
                if json_start == -1 or json_end == 0:
                    raise ValueError("JSON array not found in LLM response")
                
                json_str = content[json_start:json_end]
                parsed_results = json.loads(json_str)
                logger.info(f"Successfully parsed {len(parsed_results)} results from LLM response.")
                return parsed_results
            except (json.JSONDecodeError, ValueError) as e:
                logger.error(f"Failed to parse LLM JSON response: {e}\nResponse content: {content}")
                return []

    except aiohttp.ClientError as e:
        logger.error(f"API request failed: {e}")
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
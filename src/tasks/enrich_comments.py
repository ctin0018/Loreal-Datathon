import asyncio
import logging
import os
import json
import multiprocessing

# Force offline mode to prevent stalling on network checks during worker initialization
os.environ["TRANSFORMERS_OFFLINE"] = "1"
os.environ["HF_DATASETS_OFFLINE"] = "1"

from concurrent.futures import ProcessPoolExecutor
from typing import List, Dict, Any
from dotenv import load_dotenv
import sqlalchemy.exc

from sqlalchemy import update, select, bindparam, String, Integer, Boolean, Float, text
from sqlalchemy.dialects.postgresql import BIGINT
from sqlalchemy.ext.asyncio import AsyncSession
from ..database import AsyncSessionLocal
from ..models.youtube_models import Comment

import torch
from transformers import pipeline
import langdetect
from tqdm.asyncio import tqdm

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')
logger = logging.getLogger(__name__)
load_dotenv()

# Update CONFIG for local processing
CONFIG = {
    "DB_BATCH_SIZE": 1000,
    "WORKERS": 3, # Reduced for stability on 16GB RAM laptop
    "SAMPLE_PER_VIDEO": 50, # Initial target per video
}

# --- GLOBAL MODEL LOADER (for workers) ---
_sentiment_analyzer = None

def init_worker():
    """Initializer for multiprocessing pool to load models once per process."""
    global _sentiment_analyzer
    # Force strict offline and CPU
    os.environ["TRANSFORMERS_OFFLINE"] = "1"
    _sentiment_analyzer = pipeline(
        "sentiment-analysis",
        model="cardiffnlp/twitter-roberta-base-sentiment-latest",
        device=-1, 
        top_k=None,
        truncation=True,
        max_length=512,
        local_files_only=True
    )

def worker_analyze_batch(texts: List[str]) -> List[List[Dict]]:
    return _sentiment_analyzer(texts, batch_size=32)

# --- CATEGORIZATION LOGIC ---
CATEGORY_KEYWORDS = {
    "Ingredient Mention": ["spf", "retinol", "vitamin c", "hyaluronic", "niacinamide", "ingredient", "formula", "chemical", "fragrance", "paraben", "sulfate", "acid"],
    "Application Tip":   ["apply", "how to", "tutorial", "technique", "blend", "brush", "tip", "trick", "routine", "step", "use it"],
    "Brand Loyalty":     ["love this brand", "always buy", "loyal", "favorite brand", "will never switch", "best brand", "only brand"],
    "Product Inquiry":   ["where can i", "which one", "recommend", "which product", "does it work", "how long", "available", "price", "how much", "worth it"],
    "Negative Experience": ["broke out", "irritat", "burn", "sting", "rash", "ruin", "worst", "return", "refund", "disappoint", "waste", "scam", "fake"],
    "Product Praise": ["love", "amazing", "great", "perfect", "flawless", "glow", "obsessed", "holy grail", "repurchas", "recommended", "works great"],
}

def keyword_categorize(text: str) -> tuple[str, float]:
    text_lower = text.lower() if text else ""
    for category, keywords in CATEGORY_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            return category, 0.9
    return "General Feedback", 0.5

class CommentEnrichmentService:
    def __init__(self, db_session_factory):
        self.async_session_factory = db_session_factory
        self._init_executor()

    def _init_executor(self):
        logger.info(f"Initializing ProcessPool with {CONFIG['WORKERS']} workers...")
        self.executor = ProcessPoolExecutor(max_workers=CONFIG['WORKERS'], initializer=init_worker)

    async def _fetch_comments(self, db: AsyncSession, only_sampling: bool = True) -> List[Comment]:
        """Fetches priority samples or exhaustive remaining comments."""
        if only_sampling:
            query = text(f"""
                WITH ranked_comments AS (
                    SELECT comment_id, ROW_NUMBER() OVER(PARTITION BY video_id ORDER BY published_at DESC) as rank
                    FROM comments 
                    WHERE sentiment IS NULL
                )
                SELECT comment_id FROM ranked_comments WHERE rank <= {CONFIG['SAMPLE_PER_VIDEO']}
                LIMIT {CONFIG['DB_BATCH_SIZE']}
            """)
        else:
            query = text(f"SELECT comment_id FROM comments WHERE sentiment IS NULL LIMIT {CONFIG['DB_BATCH_SIZE']}")
        
        result = await db.execute(query)
        ids = [r[0] for r in result.fetchall()]
        if not ids: return []

        stmt = select(Comment).where(Comment.comment_id.in_(ids))
        result = await db.execute(stmt)
        return result.scalars().all()

    def _triage_comment(self, text: Any) -> Dict[str, Any]:
        if not text or not isinstance(text, str):
            return {"language": "unknown", "language_confidence": 0.0, "is_spam": False, "is_relevant": False, "comment_length": 0}
        try:
            language = langdetect.detect(text)
        except:
            language = "unknown"
        is_spam = "http://" in text or "https://" in text or len(text.strip()) < 3
        is_relevant = not is_spam and len(text.split()) > 3
        return {"language": language, "language_confidence": 1.0, "is_spam": is_spam, "is_relevant": is_relevant, "comment_length": len(text)}

    async def _analyze_comments_parallel(self, comments: List[Comment]) -> List[Dict[str, Any]]:
        texts = [c.text_original for c in comments]
        chunk_size = max(1, len(texts) // CONFIG['WORKERS'])
        chunks = [texts[i:i + chunk_size] for i in range(0, len(texts), chunk_size)]
        
        loop = asyncio.get_event_loop()
        try:
            futures = [loop.run_in_executor(self.executor, worker_analyze_batch, chunk) for chunk in chunks]
            chunk_results = await asyncio.gather(*futures)
        except Exception as e:
            logger.error(f"Process Pool error: {e}. Restarting executor...")
            self.executor.shutdown(wait=False)
            self._init_executor()
            return [] # Skip this batch to recover

        all_sentiments = [item for sublist in chunk_results for item in sublist]
        results = []
        for c, sent in zip(comments, all_sentiments):
            pos_score, neg_score, neu_score = 0.0, 0.0, 0.0
            dominant_sentiment, max_score = "Neutral", 0.0
            score_list = sent if isinstance(sent, list) else [sent]
            for s in score_list:
                label, score = s['label'].lower(), s['score']
                if label == 'positive': pos_score = score
                if label == 'negative': neg_score = score
                if label == 'neutral': neu_score = score
                if score > max_score:
                    max_score, dominant_sentiment = score, s['label'].capitalize()

            best_category, cat_confidence = keyword_categorize(c.text_original)
            results.append({
                "comment_id": c.comment_id,
                "sentiment": dominant_sentiment,
                "sentiment_score_positive": float(pos_score),
                "sentiment_score_negative": float(neg_score),
                "sentiment_score_neutral": float(neu_score),
                "category": best_category,
                "category_confidence": float(cat_confidence),
                "quality_score": min(5, max(1, len(c.text_original.split()) // 10))
            })
        return results

    async def _bulk_update_comments(self, db: AsyncSession, enriched_data: List[Dict]):
        if not enriched_data: return
        stmt = update(Comment).where(Comment.comment_id == bindparam('b_comment_id', type_=BIGINT)).values(
                sentiment=bindparam('sentiment', type_=String),
                sentiment_score_positive=bindparam('sentiment_score_positive', type_=Float),
                sentiment_score_negative=bindparam('sentiment_score_negative', type_=Float),
                sentiment_score_neutral=bindparam('sentiment_score_neutral', type_=Float),
                category=bindparam('category', type_=String),
                category_confidence=bindparam('category_confidence', type_=Float),
                quality_score=bindparam('quality_score', type_=Integer),
                language=bindparam('language', type_=String),
                language_confidence=bindparam('language_confidence', type_=Float),
                is_spam=bindparam('is_spam', type_=Boolean),
                comment_length=bindparam('comment_length', type_=Integer)
        )
        update_params = [{
                'b_comment_id': int(comment['comment_id']),
                'sentiment': comment.get('sentiment', 'N/A'),
                'sentiment_score_positive': comment.get('sentiment_score_positive', 0.0),
                'sentiment_score_negative': comment.get('sentiment_score_negative', 0.0),
                'sentiment_score_neutral': comment.get('sentiment_score_neutral', 0.0),
                'category': comment.get('category', 'Skipped'),
                'category_confidence': comment.get('category_confidence', 0.0),
                'quality_score': comment.get('quality_score', 1),
                'language': comment.get('language', 'unknown'),
                'language_confidence': comment.get('language_confidence', 0.0),
                'is_spam': comment.get('is_spam', False),
                'comment_length': comment.get('comment_length', 0)
        } for comment in enriched_data]

        try:
            conn = await db.connection()
            await conn.execute(stmt, update_params)
            await db.commit()
        except Exception as e:
            logger.error(f"Error: {e}")
            await db.rollback()

    async def run(self):
        # 1. Phase 1: PRIORITY SAMPLING (50 per video)
        logger.info("PHASE 1: Priority Video Sampling (50 per video)...")
        with tqdm(desc="Sampling Phase") as pbar:
            while True:
                async with self.async_session_factory() as db:
                    batch = await self._fetch_comments(db, only_sampling=True)
                    if not batch: break
                    final_results = {}
                    relevant = []
                    for c in batch:
                        triage = self._triage_comment(c.text_original)
                        triage["comment_id"] = c.comment_id
                        final_results[c.comment_id] = triage
                        if triage["is_relevant"] and not triage["is_spam"]: relevant.append(c)
                    if relevant:
                         nlp_results = await self._analyze_comments_parallel(relevant)
                         for res in nlp_results: final_results[res["comment_id"]].update(res)
                    await self._bulk_update_comments(db, list(final_results.values()))
                    pbar.update(len(batch))

        # 2. Phase 2: EXHAUSTIVE (everything else)
        logger.info("PHASE 2: Exhaustive Enrichment (all remaining)...")
        with tqdm(desc="Exhaustive Phase") as pbar:
            while True:
                async with self.async_session_factory() as db:
                    batch = await self._fetch_comments(db, only_sampling=False)
                    if not batch: break
                    final_results = {}
                    relevant = []
                    for c in batch:
                        triage = self._triage_comment(c.text_original)
                        triage["comment_id"] = c.comment_id
                        final_results[c.comment_id] = triage
                        if triage["is_relevant"] and not triage["is_spam"]: relevant.append(c)
                    if relevant:
                         nlp_results = await self._analyze_comments_parallel(relevant)
                         for res in nlp_results: final_results[res["comment_id"]].update(res)
                    await self._bulk_update_comments(db, list(final_results.values()))
                    pbar.update(len(batch))

async def run_enrichment_pipeline():
    service = CommentEnrichmentService(AsyncSessionLocal)
    await service.run()

async def run_enrichment_pipeline():
    service = CommentEnrichmentService(AsyncSessionLocal)
    await service.run()
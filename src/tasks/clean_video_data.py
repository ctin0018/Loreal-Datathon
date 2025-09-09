import asyncio
import logging
import re
import ast
import pandas as pd
import numpy as np # Import numpy to handle NaN
from sqlalchemy import select, update, bindparam
from sqlalchemy.ext.asyncio import AsyncSession

# --- Local Application Imports ---
from ..database import AsyncSessionLocal
from ..models.youtube_models import Video

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s')
logger = logging.getLogger(__name__)

# --- Re-usable Cleaning Functions ---
def parse_duration(duration_str):
    if pd.isna(duration_str) or duration_str == "": return None
    pattern = re.compile(r'P(?:(?P<days>\d+)D)?(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?)?')
    match = pattern.match(duration_str)
    if not match: return None
    parts = match.groupdict()
    days, hours, minutes, seconds = [int(parts.get(k) or 0) for k in ['days', 'hours', 'minutes', 'seconds']]
    return days*86400 + hours*3600 + minutes*60 + seconds

def normalize_lang_code(lang):
    if pd.isna(lang) or lang == "": return None
    return lang.split("-")[0].lower()

def clean_topic_categories(x):
    if pd.isna(x) or x.strip() == "": return []
    try:
        urls = ast.literal_eval(x)
        return [url.split("/")[-1].replace("_", " ") for url in urls]
    except (ValueError, SyntaxError):
        return []

# --- Main Service Logic ---
class VideoCleaningService:
    def __init__(self, db_session_factory):
        self.async_session_factory = db_session_factory

    async def _fetch_uncleaned_videos(self, db: AsyncSession, batch_size: int = 5000) -> pd.DataFrame:
        """Fetches a batch of videos that have not yet been cleaned."""
        logger.info(f"Fetching up to {batch_size} uncleaned videos...")
        stmt = select(Video).where(Video.duration_seconds == None).limit(batch_size)
        result = await db.execute(stmt)
        videos = result.scalars().all()
        logger.info(f"Found {len(videos)} videos to clean.")
        if not videos:
            return pd.DataFrame()

        # === FIX #1: Explicitly create the dictionary to build the DataFrame correctly ===
        video_data = [
            {
                "video_id": v.video_id, "description": v.description, "content_duration": v.content_duration,
                "default_language": v.default_language, "default_audio_language": v.default_audio_language,
                "topic_categories": v.topic_categories
            }
            for v in videos
        ]
        return pd.DataFrame(video_data)

    def _transform_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Applies all cleaning logic to the DataFrame."""
        if df.empty:
            return df

        logger.info("Applying cleaning and transformation logic...")
        df['description_clean'] = df['description'].fillna('').str.replace(r'#\w+', '', regex=True).str.strip()
        df['duration_seconds'] = df['content_duration'].apply(parse_duration)
        df['defaultLanguage_norm'] = df['default_language'].apply(normalize_lang_code)
        df['defaultAudioLanguage_norm'] = df['default_audio_language'].apply(normalize_lang_code)
        df['language_code'] = df['defaultLanguage_norm'].fillna(df['defaultAudioLanguage_norm'])
        lang_map = {'en': 'English', 'fr': 'French', 'pt': 'Portuguese', 'es': 'Spanish', 'de': 'German', 'it': 'Italian', 'zh': 'Chinese', 'ja': 'Japanese', 'ko': 'Korean'}
        df['language_name'] = df['language_code'].map(lang_map).fillna(df['language_code']).fillna("Unknown")
        df['topic_categories_clean'] = df['topic_categories'].apply(clean_topic_categories)

        return df

    async def _bulk_update_videos(self, db: AsyncSession, cleaned_df: pd.DataFrame):
        """Updates the database with the cleaned data."""
        if cleaned_df.empty:
            return 0

        # === FIX #2: Convert any remaining np.nan values to None before updating the DB ===
        cleaned_df = cleaned_df.replace({np.nan: None})
        
        update_data = cleaned_df.to_dict(orient='records')
        
        stmt = (
            update(Video)
            .where(Video.video_id == bindparam('b_video_id'))
            .values(
                description_clean=bindparam('description_clean'),
                duration_seconds=bindparam('duration_seconds'),
                language_name=bindparam('language_name'),
                topic_categories_clean=bindparam('topic_categories_clean')
            )
        )
        
        update_params = [
            {'b_video_id': data['video_id'], 'description_clean': data['description_clean'], 'duration_seconds': data['duration_seconds'], 'language_name': data['language_name'], 'topic_categories_clean': data['topic_categories_clean']}
            for data in update_data
        ]

        # === FIX #3: Use session.connection() to bypass the ORM Bulk Update mode ===
        connection = await db.connection()
        result = await connection.execute(stmt, update_params)

        await db.commit()

        logger.info(f"Bulk updated {len(update_data)} videos in the database.")
        return len(update_data) # Return the number of rows we just updated

    async def run(self):
        """Orchestrates the entire cleaning process."""
        logger.info("Starting video data cleaning service...")
        
        total_processed = 0
        while True:
            rows_processed_this_batch = 0
            async with self.async_session_factory() as db:
                video_df = await self._fetch_uncleaned_videos(db)
                if video_df.empty:
                    logger.info("No more uncleaned videos found. Process complete.")
                    break
                
                cleaned_df = self._transform_dataframe(video_df)
                
                # We get the number of rows processed from the update function
                rows_processed_this_batch = await self._bulk_update_videos(db, cleaned_df)
                total_processed += rows_processed_this_batch
        logger.info("Video data cleaning service finished.")

# --- Main Entry Point ---
async def main():
    service = VideoCleaningService(db_session_factory=AsyncSessionLocal)
    await service.run()

if __name__ == "__main__":
    asyncio.run(main())
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
from ..models.youtube_models import Video, VideoStaging
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import delete

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

	async def _fetch_uncleaned_videos(self, db: AsyncSession, batch_size: int = 2000) -> pd.DataFrame:
		"""Fetches a batch of videos from the staging table."""
		logger.info(f"Fetching up to {batch_size} uncleaned videos from staging...")
		stmt = select(VideoStaging).limit(batch_size)
		result = await db.execute(stmt)
		videos = result.scalars().all()
		logger.info(f"Found {len(videos)} videos in staging to clean.")
		if not videos:
			return pd.DataFrame()

		# Store video IDs for deletion later
		self.batch_video_ids = [v.video_id for v in videos]

		video_data = [
			{
				"video_id": v.video_id, "published_at": v.published_at, "channel_id": v.channel_id, 
				"title": v.title, "description": v.description, "tags": v.tags, 
				"default_language": v.default_language, "default_audio_language": v.default_audio_language,
				"content_duration": v.content_duration, "view_count": v.view_count, "like_count": v.like_count, 
				"favourite_count": v.favourite_count, "comment_count": v.comment_count, "topic_categories": v.topic_categories
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
		df['duration_seconds'] = pd.to_numeric(df['duration_seconds'], errors='coerce')
		df['duration_minutes'] = (df['duration_seconds'] / 60).round(2)
		df['duration_hours'] = (df['duration_seconds'] / 3600).round(2)
		df['defaultLanguage_norm'] = df['default_language'].apply(normalize_lang_code)
		df['defaultAudioLanguage_norm'] = df['default_audio_language'].apply(normalize_lang_code)
		df['language_code'] = df['defaultLanguage_norm'].fillna(df['defaultAudioLanguage_norm'])
		lang_map = {'en': 'English', 'fr': 'French', 'pt': 'Portuguese', 'es': 'Spanish', 'de': 'German', 'it': 'Italian', 'zh': 'Chinese', 'ja': 'Japanese', 'ko': 'Korean'}
		df['language_name'] = df['language_code'].map(lang_map).fillna(df['language_code']).fillna("Unknown")
		df['topic_categories_clean'] = df['topic_categories'].apply(clean_topic_categories)

		# Keep only final columns
		df = df[['video_id', 'published_at', 'channel_id', 'title', 'description_clean', 'tags', 'language_name', 'duration_seconds', 'view_count', 'like_count', 'favourite_count', 'comment_count', 'topic_categories_clean']]
		# Rename description_clean to description for final table
		df = df.rename(columns={'description_clean': 'description', 'topic_categories_clean': 'topic_categories'})

		# Convert pandas types to native Python types for asyncpg
		df['published_at'] = pd.to_datetime(df['published_at']).dt.to_pydatetime()
		df['duration_seconds'] = df['duration_seconds'].apply(lambda x: int(x) if pd.notna(x) else None)
		df['view_count'] = df['view_count'].apply(lambda x: float(x) if pd.notna(x) else None)
		df['like_count'] = df['like_count'].apply(lambda x: float(x) if pd.notna(x) else None)
		df['favourite_count'] = df['favourite_count'].apply(lambda x: float(x) if pd.notna(x) else None)
		df['comment_count'] = df['comment_count'].apply(lambda x: float(x) if pd.notna(x) else None)

		return df

	async def _bulk_insert_videos(self, db: AsyncSession, cleaned_df: pd.DataFrame):
		"""Inserts cleaned data into the final Video table and removes from staging."""
		if cleaned_df.empty:
			return 0

		cleaned_df = cleaned_df.replace({np.nan: None})
		insert_data = cleaned_df.to_dict(orient='records')
		
		# Ensure pure python types for asyncpg
		for row in insert_data:
			if hasattr(row['published_at'], 'to_pydatetime'):
				row['published_at'] = row['published_at'].to_pydatetime()
			if pd.isna(row['published_at']):
				row['published_at'] = None
		
		# UPSERT logic using PostgreSQL native insert
		stmt = insert(Video).values(insert_data)
		stmt = stmt.on_conflict_do_update(
			index_elements=['video_id'],
			set_={
				'published_at': stmt.excluded.published_at,
				'channel_id': stmt.excluded.channel_id,
				'title': stmt.excluded.title,
				'description': stmt.excluded.description,
				'tags': stmt.excluded.tags,
				'language_name': stmt.excluded.language_name,
				'duration_seconds': stmt.excluded.duration_seconds,
				'view_count': stmt.excluded.view_count,
				'like_count': stmt.excluded.like_count,
				'favourite_count': stmt.excluded.favourite_count,
				'comment_count': stmt.excluded.comment_count,
				'topic_categories': stmt.excluded.topic_categories
			}
		)

		connection = await db.connection()
		await connection.execute(stmt)
		
		# Delete processed rows from staging
		delete_stmt = delete(VideoStaging).where(VideoStaging.video_id.in_(self.batch_video_ids))
		await connection.execute(delete_stmt)

		await db.commit()

		logger.info(f"Bulk inserted {len(insert_data)} videos into final table and cleared staging.")
		return len(insert_data)

	async def run(self):
		"""Orchestrates the entire cleaning process."""
		logger.info("Starting video data cleaning service...")

		total_processed = 0
		while True:
			async with self.async_session_factory() as db:
				video_df = await self._fetch_uncleaned_videos(db)
				if video_df.empty:
					logger.info("No more uncleaned videos found. Process complete.")
					break

				cleaned_df = self._transform_dataframe(video_df)
				rows_processed_this_batch = await self._bulk_insert_videos(db, cleaned_df)
				total_processed += rows_processed_this_batch

				# --- FIX: Break if no rows were updated ---
				if rows_processed_this_batch == 0:
					logger.info("No rows updated in this batch. Exiting to avoid infinite loop.")
					break

		logger.info(f"Video data cleaning service finished. Total processed: {total_processed}")

# --- Main Entry Point ---
async def main():
	service = VideoCleaningService(db_session_factory=AsyncSessionLocal)
	await service.run()

if __name__ == "__main__":
	asyncio.run(main())
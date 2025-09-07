import ast
import re
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# Load video.csv
video_df = pd.read_csv("C:\\Users\\Daniel Puah\\Desktop\\Loreal Data\\videos.csv")

# Quick look
# print(video_df.shape)
# print(video_df.columns)
# print(video_df.head())

# Check datatypes and missing values
# video_df.info()
video_df.isnull().sum()

# Ensure videoId is unique
# print("Unique videoId:", video_df['videoId'].nunique())
# print("Total rows:", len(video_df))

# Fill missing text fields
video_df['description'] = video_df['description'].fillna("")
video_df['tags'] = video_df['tags'].fillna("")

# Fill engagement columns
for col in ['viewCount', 'likeCount', 'favouriteCount', 'commentCount']:
    video_df[col] = video_df[col].fillna(0).astype(int)

def parse_duration(duration_str):
    if pd.isna(duration_str) or duration_str == "":
        return None
    
    # Regex pattern for ISO8601 duration
    pattern = re.compile(
        r'P(?:(?P<days>\d+)D)?'
        r'(?:T'
        r'(?:(?P<hours>\d+)H)?'
        r'(?:(?P<minutes>\d+)M)?'
        r'(?:(?P<seconds>\d+)S)?'
        r')?'
    )
    
    match = pattern.match(duration_str)
    if not match:
        return None
    
    parts = match.groupdict()
    days = int(parts.get("days") or 0)
    hours = int(parts.get("hours") or 0)
    minutes = int(parts.get("minutes") or 0)
    seconds = int(parts.get("seconds") or 0)
    
    total_seconds = days*86400 + hours*3600 + minutes*60 + seconds
    return total_seconds

# Apply cleaning
video_df['duration_seconds'] = video_df['contentDuration'].apply(parse_duration)
video_df['duration_minutes'] = (video_df['duration_seconds'] / 60).round(2)
video_df['duration_hours'] = (video_df['duration_seconds'] / 3600).round(2)

# Clean and standardize language codes
# Step 1: Normalize (lowercase + root only)
def normalize_lang_code(lang):
    if pd.isna(lang) or lang == "":
        return None
    return lang.split("-")[0].lower()

video_df['defaultLanguage_norm'] = video_df['defaultLanguage'].apply(normalize_lang_code)
video_df['defaultAudioLanguage_norm'] = video_df['defaultAudioLanguage'].apply(normalize_lang_code)

# Step 2: Fill missing values (prefer defaultLanguage, fallback to audio)
video_df['language_code'] = video_df['defaultLanguage_norm'].fillna(video_df['defaultAudioLanguage_norm'])

# Step 3: Map to readable names (manually define common ones)
lang_map = {
    'en': 'English',
    'fr': 'French',
    'pt': 'Portuguese',
    'es': 'Spanish',
    'de': 'German',
    'it': 'Italian',
    'zh': 'Chinese',
    'ja': 'Japanese',
    'ko': 'Korean'
}

video_df['language_name'] = video_df['language_code'].map(lang_map).fillna(video_df['language_code'])

# Step 4: Fill completely missing as "Unknown" (or "English" if you prefer)
video_df['language_name'] = video_df['language_name'].fillna("Unknown")


# Extract readable topic names
def clean_topic(x):
    if pd.isna(x) or x.strip() == "":
        return []
    try:
        urls = ast.literal_eval(x)   # convert string list → real list
        return [url.split("/")[-1].replace("_", " ") for url in urls]
    except:
        return []

video_df['topicCategories_cleaned'] = video_df['topicCategories'].apply(clean_topic)


# Final check
video_df.info()


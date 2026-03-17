#!/bin/bash
set -e  # exit immediately if a command fails

CONTAINER=timescaledb
DB_NAME=youtubedata
DB_USER=user

echo "🧹 Cleaning up previous containers and volumes..."
docker compose down -v || true  # stop and remove containers + volumes (ignore errors)

echo "📦 Building enrichment-service (no cache)..."
docker compose build --no-cache enrichment-service

echo "🚀 Starting Grafana & TimescaleDB..."
docker compose up -d grafana timescaledb

echo "⏳ Waiting 15 seconds for TimescaleDB to initialize..."
sleep 15

echo "📑 Initializing DB schema..."
docker compose run --rm enrichment-service python -m src.init_db

echo "📂 Copying CSV files into container..."
for file in ./loreal_project_db_data/*.csv; do
  echo "   -> $(basename "$file")"
  docker cp "$file" $CONTAINER:/tmp/$(basename "$file")
done

echo "🛠 Importing videos.csv into videos_staging table..."
docker exec -i $CONTAINER psql -U $DB_USER -d $DB_NAME -c "\copy videos_staging (
  kind, video_id, published_at, channel_id, title, description, tags,
  default_language, default_audio_language, content_duration,
  view_count, like_count, favourite_count, comment_count, topic_categories
) FROM '/tmp/videos.csv' WITH (FORMAT csv, HEADER true, NULL '');"

echo "🛠 Importing comments CSVs into comments table..."
for i in {1..5}; do
  FILE="/tmp/comments${i}.csv"
  echo "   -> Importing $(basename "$FILE")"
  docker exec -i $CONTAINER psql -U $DB_USER -d $DB_NAME -c "\copy comments (
    kind, comment_id, channel_id, video_id, author_id, text_original,
    parent_comment_id, like_count, published_at, updated_at
  ) FROM '$FILE' WITH (FORMAT csv, HEADER true, NULL '');"
done

echo "✅ Project setup and data import completed successfully!"

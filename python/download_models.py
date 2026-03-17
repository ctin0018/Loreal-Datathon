import os
from transformers import pipeline

cache_dir = os.getenv('HF_HOME', '/root/.cache/huggingface')
print(f"Downloading models to {cache_dir}...", flush=True)

# Only download the sentiment model (categorization uses fast keyword matching)
print("Downloading cardiffnlp/twitter-roberta-base-sentiment-latest...", flush=True)
pipeline(
    "sentiment-analysis", 
    model="cardiffnlp/twitter-roberta-base-sentiment-latest", 
    top_k=None,
    truncation=True,
    max_length=512
)

print("Models downloaded successfully!", flush=True)

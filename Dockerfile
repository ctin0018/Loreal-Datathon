# Start from a slim, official Python base image
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file first to leverage Docker's build cache
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the main entry point and the source code package
COPY main.py .
COPY src/ /app/src/

# Download NLP Models into the Docker Image during build time
COPY scripts/download_models.py scripts/download_models.py
RUN python scripts/download_models.py

# By default, the container will run the enrichment pipeline when started.
# We can override this command in docker-compose.yml if needed.
CMD ["python", "main.py", "--enrich-comments"]
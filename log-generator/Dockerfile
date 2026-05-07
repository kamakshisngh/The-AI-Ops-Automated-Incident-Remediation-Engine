# Use a slim Python image
FROM python:3.11-slim

# Install system dependencies for confluent-kafka
RUN apt-get update && apt-get install -y \
    librdkafka-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all scripts
COPY . .

# We'll override the CMD in docker-compose to choose which script to run
CMD ["python", "producer.py"]

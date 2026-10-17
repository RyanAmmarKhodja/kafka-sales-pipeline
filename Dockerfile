FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir kafka-python

# Copy source code
COPY . /app

# Default command (overridden in docker-compose)
CMD ["python", "producer.py"]

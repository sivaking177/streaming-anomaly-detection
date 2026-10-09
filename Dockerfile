# Image for the producer and dashboard services in docker-compose.yml.
# The Spark job is NOT run from this image -- it needs a JVM and is
# submitted with spark-submit from the host (see README).
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY streamdetect ./streamdetect

CMD ["python", "-m", "streamdetect.ingest.producer", "--help"]

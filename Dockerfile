# Flight ingestion pipeline image — shared by all services in
# docker-compose.pipeline.yml:
#   pipeline-worker     python -m pipelines.raw_worker
#   pipeline-normalizer python -m pipelines.normalizer
#   pipeline-sink       python -m pipelines.kafka_to_db
#   pipeline-liveness   python -m pipelines.liveness
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements/pipelines.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY pipelines/ /app/pipelines/

RUN useradd --create-home runner
USER runner

CMD ["python", "-m", "pipelines.raw_worker"]

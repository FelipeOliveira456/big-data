FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml README.md config.yaml.example ./
COPY src/ ./src/
COPY scripts/ ./scripts/

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir .

RUN useradd --create-home --uid 1000 appuser \
    && chmod +x scripts/*.sh \
    && chown -R appuser:appuser /app

USER appuser

ENV PYTHONPATH=/app/src:/app
ENV CONTAINER_ROLE=pipeline

ENTRYPOINT ["/app/scripts/docker-entrypoint.sh"]

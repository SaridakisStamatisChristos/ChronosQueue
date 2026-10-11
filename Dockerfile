FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    CHRONOSQUEUE_DB=/data/chronosqueue.db

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir . \
    && useradd --create-home --uid 10001 chronos \
    && mkdir -p /data \
    && chown -R chronos:chronos /data

USER chronos
VOLUME ["/data"]
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=10 CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/health/ready', timeout=2).read()"]
CMD ["chronosqueue", "serve", "--db", "/data/chronosqueue.db", "--host", "0.0.0.0", "--port", "8080"]

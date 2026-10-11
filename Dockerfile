FROM python:3.12-slim AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app/src
WORKDIR /app
COPY src ./src
RUN useradd --system --uid 10001 chronos && mkdir -p /data && chown chronos:chronos /data
USER chronos
VOLUME ["/data"]
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=5 CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/health/ready',timeout=2)"
ENTRYPOINT ["python","-m","chronosqueue.cli"]
CMD ["serve","--db","/data/chronosqueue.db","--host","0.0.0.0"]

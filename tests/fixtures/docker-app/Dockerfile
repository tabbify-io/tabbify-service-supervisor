# Minimal Tabbify docker-runtime test app: a tiny HTTP server that answers
# every request with "hello from docker" on port 8080. Used by the REAL docker
# integration test (gated on `docker_available()`).
FROM python:3-slim
WORKDIR /app
COPY server.py /app/server.py
EXPOSE 8080
CMD ["python", "/app/server.py"]

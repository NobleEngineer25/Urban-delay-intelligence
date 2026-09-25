# Small official Python image
FROM python:3.12-slim-trixie

# Don't write .pyc files, and print logs immediately
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080

WORKDIR /app

# Install dependencies first, so Docker can cache this layer
COPY serving/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Then copy the app code
COPY serving/ ./serving/

# Don't run as root
RUN useradd --create-home appuser
USER appuser

EXPOSE 8080

# Cloud Run sets $PORT; default is 8080
CMD ["sh", "-c", "uvicorn serving.main:app --host 0.0.0.0 --port ${PORT}"]
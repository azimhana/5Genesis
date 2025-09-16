FROM python:3.9-slim

WORKDIR /app

# Install minimal build tools, then clean apt cache
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps first (good layer caching)
COPY requirements.txt .
RUN python -m pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy the whole package (not just *.py)
COPY data_handler/ ./data_handler/

# Useful envs
ENV PYTHONUNBUFFERED=1
ENV ANALYTICS_CONN_FILE=/run/secrets/analytics_connections

EXPOSE 5000
CMD ["python", "-m", "data_handler"]

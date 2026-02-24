FROM python:3.12-slim

WORKDIR /app

# Install dependencies first (cached layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source files
COPY . .

# Create persistent volume mount point for SQLite database
RUN mkdir -p /app/data
VOLUME ["/app/data"]

# Default environment variables
# Override at runtime via: docker run -e VAR=value  or  docker-compose environment:
ENV DB_PATH=/app/data/rates.db
ENV RATE_STATE_FILE=/app/data/last_rates.json
ENV RATE_JOB_HOUR=8
ENV RATE_JOB_MINUTE=30
ENV RATE_JOB_TZ=America/New_York
ENV HOST=0.0.0.0
ENV PORT=8000

EXPOSE 8000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]

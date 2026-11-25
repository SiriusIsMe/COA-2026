FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY movie-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY schemas/ ./schemas/
COPY movie-service/ ./
COPY clickhouse/migrations/ ./clickhouse_migrations/

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

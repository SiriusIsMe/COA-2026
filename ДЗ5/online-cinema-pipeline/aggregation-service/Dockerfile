FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY aggregation-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY aggregation-service/ ./
COPY postgres/migrations/ ./pg_migrations/

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

# Notification service

Минимальный сервис уведомлений маркетплейса

## Требования

- Docker (Docker Desktop или Docker Engine с плагином Compose)

## Запуск

```bash
docker compose up --build
```

## Проверка

Health-check (ожидается `200 OK`):

```bash
curl -i http://localhost:8000/health
```

Ответ: `{"status": "ok"}`

Тестовая отправка уведомления (ожидается `202`, в логах контейнера появится строка `NOTIFY:`):

```bash
curl -i -X POST http://localhost:8000/notify \
  -H "Content-Type: application/json" \
  -d '{"order_id": 1, "status": "paid", "channel": "email"}'
```

Статус контейнера (`healthy` через несколько секунд):

```bash
docker compose ps
```

## Остановка

```bash
docker compose down
```

## Запуск без Docker (для отладки)

```bash
python app.py
```

"""Проверка части 1: читает N сообщений из movie-events, декодирует Avro через Schema Registry
и показывает, что все события одного user_id лежат в одной партиции.

Запуск:  docker compose run --rm movie-service python check_consume.py
"""
import sys
import uuid
from collections import Counter, defaultdict

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

import config

N = int(sys.argv[1]) if len(sys.argv) > 1 else 200

deserializer = AvroDeserializer(SchemaRegistryClient({"url": config.SCHEMA_REGISTRY_URL}))
consumer = Consumer({
    "bootstrap.servers": config.BOOTSTRAP_SERVERS,
    "group.id": f"check-{uuid.uuid4()}",  # новая группа — читаем с начала
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})
consumer.subscribe([config.TOPIC])

per_partition = Counter()
user_partitions = defaultdict(set)
schema_ids = Counter()
read = 0
first = None

while read < N:
    msg = consumer.poll(5)
    if msg is None:
        break
    if msg.error():
        print(msg.error())
        continue
    raw = msg.value()
    schema_ids[int.from_bytes(raw[1:5], "big")] += 1  # байты 1-4 — ID схемы
    event = deserializer(raw, SerializationContext(msg.topic(), MessageField.VALUE))
    first = first or event
    per_partition[msg.partition()] += 1
    user_partitions[event["user_id"]].add(msg.partition())
    read += 1

consumer.close()

print(f"Прочитано сообщений: {read}")
print(f"Пример события: {first}")
print(f"ID схем в сообщениях: {dict(schema_ids)}")
print(f"Распределение по партициям: {dict(sorted(per_partition.items()))}")
bad = {u: p for u, p in user_partitions.items() if len(p) > 1}
print(f"Пользователей: {len(user_partitions)}, из них в >1 партиции: {len(bad)} "
      f"({'OK — ключ user_id работает' if not bad else 'ОШИБКА'})")

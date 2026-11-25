"""Инициализация: создаёт topic movie-events и регистрирует Avro-схему в Schema Registry.

Запускается один раз при `docker compose up` (сервис kafka-init). Идемпотентен:
повторный запуск не создаёт дубликатов — существующий topic пропускается,
а повторная регистрация той же схемы возвращает тот же ID и ту же версию.
"""
import sys
import time

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.error import KafkaError
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.error import SchemaRegistryError

import config


def retry(fn, what: str, attempts: int = 90, delay: float = 2.0):
    for i in range(1, attempts + 1):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001 — ждём, пока сервис поднимется
            print(f"[init] {what}: попытка {i}/{attempts} не удалась: {e}")
            time.sleep(delay)
    print(f"[init] {what}: не удалось, выходим")
    sys.exit(1)


def create_topic() -> None:
    admin = AdminClient({"bootstrap.servers": config.BOOTSTRAP_SERVERS})

    # Ждём, пока в кластере будет достаточно брокеров для заданного replication factor
    def enough_brokers():
        brokers = admin.list_topics(timeout=5).brokers
        if len(brokers) < config.TOPIC_REPLICATION:
            raise RuntimeError(f"брокеров {len(brokers)}, нужно {config.TOPIC_REPLICATION}")
        return brokers
    brokers = retry(enough_brokers, "кластер Kafka")
    print(f"[init] брокеры в кластере: {sorted(brokers)}")

    topic = NewTopic(
        config.TOPIC,
        num_partitions=config.TOPIC_PARTITIONS,
        replication_factor=config.TOPIC_REPLICATION,
        config={
            "retention.ms": str(7 * 24 * 3600 * 1000),           # 7 дней
            "min.insync.replicas": str(config.TOPIC_MIN_ISR),    # acks=all ждёт минимум N копий
        },
    )
    future = admin.create_topics([topic])[config.TOPIC]
    try:
        future.result()
        print(f"[init] topic '{config.TOPIC}' создан: partitions={config.TOPIC_PARTITIONS}, "
              f"replication={config.TOPIC_REPLICATION}")
    except KafkaException as e:
        if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
            print(f"[init] topic '{config.TOPIC}' уже существует — пропускаем")
        else:
            raise

    # Сразу после создания метаданные партиций ещё не готовы — ждём лидеров у всех партиций
    def partitions_ready():
        meta = admin.list_topics(topic=config.TOPIC, timeout=10).topics[config.TOPIC]
        parts = meta.partitions
        if len(parts) < config.TOPIC_PARTITIONS or any(p.leader < 0 for p in parts.values()):
            raise RuntimeError(f"партиции ещё не готовы: {len(parts)}/{config.TOPIC_PARTITIONS}")
        return parts
    parts = retry(partitions_ready, "метаданные топика")
    for pid, p in sorted(parts.items()):
        print(f"[init]   partition {pid}: leader={p.leader} replicas={p.replicas} isr={p.isrs}")
    rf = min(len(p.replicas) for p in parts.values())
    print(f"[init] topic '{config.TOPIC}': {len(parts)} партиции, "
          f"replication.factor={rf}, min.insync.replicas={config.TOPIC_MIN_ISR}")


def register_schema() -> None:
    client = SchemaRegistryClient({"url": config.SCHEMA_REGISTRY_URL})
    retry(client.get_subjects, "подключение к Schema Registry")

    # BACKWARD: новая версия схемы обязана уметь читать данные, записанные старой
    try:
        client.set_compatibility(subject_name=config.SUBJECT, level="BACKWARD")
    except SchemaRegistryError as e:
        print(f"[init] не удалось выставить compatibility: {e}")

    schema = Schema(config.load_schema_str(), schema_type="AVRO")
    schema_id = client.register_schema(config.SUBJECT, schema)
    registered = client.lookup_schema(config.SUBJECT, schema)

    versions = client.get_versions(config.SUBJECT)
    print(f"[init] схема зарегистрирована: subject='{config.SUBJECT}', "
          f"id={schema_id}, version={registered.version}, все версии={versions}")


if __name__ == "__main__":
    create_topic()
    register_schema()
    print("[init] готово")

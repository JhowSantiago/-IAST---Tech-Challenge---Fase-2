"""Cria tópicos Kafka da pipeline ICA (cross-platform)."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

TOPICOS = (
    "educacao.indicador_alfabetizacao",
    "educacao.meta_atualizada",
)


def main() -> None:
    load_dotenv(ROOT / ".env")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="ica-kafka-setup")

    novos = [
        NewTopic(name=topico, num_partitions=3, replication_factor=1)
        for topico in TOPICOS
    ]
    try:
        admin.create_topics(novos, validate_only=False)
        print("Tópicos criados:", ", ".join(TOPICOS))
    except TopicAlreadyExistsError:
        print("Tópicos já existem:", ", ".join(TOPICOS))

    metadata = admin.list_topics(timeout=10)
    for topico in TOPICOS:
        partições = len(metadata.topics[topico].partitions)
        print(f"{topico}: {partições} partições")

    admin.close()


if __name__ == "__main__":
    main()

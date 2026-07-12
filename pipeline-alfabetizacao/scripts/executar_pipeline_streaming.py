"""Orquestra pipeline streaming: tópicos → produtor → consumidor → Bronze S3."""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.common.config import get_settings  # noqa: E402
from src.common.logging_config import get_logger  # noqa: E402
from src.streaming.consumer_bronze import consumir  # noqa: E402
from src.streaming.producer_simulador import publicar  # noqa: E402

logger = get_logger(__name__)


def aguardar_kafka(bootstrap: str, tentativas: int = 30, intervalo: float = 2.0) -> None:
    from kafka import KafkaAdminClient

    for tentativa in range(1, tentativas + 1):
        try:
            admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="ica-healthcheck")
            admin.close()
            logger.info("Kafka disponível em %s", bootstrap)
            return
        except Exception as exc:  # noqa: BLE001
            logger.warning("Kafka indisponível (%s/%s): %s", tentativa, tentativas, exc)
            time.sleep(intervalo)
    raise RuntimeError(f"Kafka não respondeu em {bootstrap} após {tentativas} tentativas")


def main() -> None:
    load_dotenv(ROOT / ".env")
    settings = get_settings()

    parser = argparse.ArgumentParser(description="Pipeline streaming completo")
    parser.add_argument("--limite", type=int, default=50)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--group-id", default="bronze-ingestao-test")
    parser.add_argument("--pular-setup-topicos", action="store_true")
    args = parser.parse_args()

    aguardar_kafka(settings.kafka_bootstrap_servers)

    if not args.pular_setup_topicos:
        subprocess.run(
            [sys.executable, str(ROOT / "scripts" / "kafka_setup_topics.py")],
            check=True,
        )

    publicados = publicar(args.limite, 0.0, settings.kafka_bootstrap_servers)
    logger.info("Produtor finalizado: %s eventos", publicados)

    buffer = consumir(
        args.limite,
        args.timeout,
        settings.kafka_bootstrap_servers,
        args.group_id,
    )
    logger.info("Consumidor finalizado: %s", buffer)

    subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "carregar_bronze_streaming.py"), "--buffer", str(buffer)],
        check=True,
    )
    logger.info("Pipeline streaming concluído com sucesso")


if __name__ == "__main__":
    main()

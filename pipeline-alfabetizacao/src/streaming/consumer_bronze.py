"""Consumidor Kafka — persiste eventos em buffer local para Bronze streaming."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaConsumer

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.common.config import get_settings  # noqa: E402
from src.common.logging_config import get_logger  # noqa: E402
from src.streaming.eventos import (  # noqa: E402
    CONSUMER_GROUP,
    TOPICO_INDICADOR,
    eventos_para_dataframe,
)

logger = get_logger(__name__)
BUFFER_DIR = ROOT / "data" / "staging" / "streaming"
BUFFER_PATH = BUFFER_DIR / "events_buffer.parquet"
FLUSH_LOTE = 50


def criar_consumer(bootstrap_servers: str, group_id: str) -> KafkaConsumer:
    return KafkaConsumer(
        TOPICO_INDICADOR,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=1000,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )


def consumir(
    limite: int,
    timeout_seg: int,
    bootstrap_servers: str,
    group_id: str = CONSUMER_GROUP,
) -> Path:
    consumer = criar_consumer(bootstrap_servers, group_id)
    eventos: list[dict] = []
    inicio = time.time()

    try:
        while len(eventos) < limite and (time.time() - inicio) < timeout_seg:
            lote = consumer.poll(timeout_ms=1000, max_records=limite - len(eventos))
            for _tp, registros in lote.items():
                for registro in registros:
                    eventos.append(registro.value)
                    if len(eventos) >= limite:
                        break
    finally:
        consumer.close()

    if not eventos:
        raise RuntimeError(
            f"Nenhum evento consumido em {timeout_seg}s. Verifique Kafka e o produtor."
        )

    BUFFER_DIR.mkdir(parents=True, exist_ok=True)
    df = eventos_para_dataframe(eventos)
    df.to_parquet(BUFFER_PATH, index=False)
    logger.info("%s eventos salvos em %s", len(df), BUFFER_PATH)
    return BUFFER_PATH


def main() -> None:
    load_dotenv(ROOT / ".env")
    settings = get_settings()

    parser = argparse.ArgumentParser(description="Consumidor Kafka → buffer Parquet")
    parser.add_argument("--limite", type=int, default=100)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--group-id", default=CONSUMER_GROUP)
    args = parser.parse_args()

    consumir(args.limite, args.timeout, settings.kafka_bootstrap_servers, args.group_id)


if __name__ == "__main__":
    main()

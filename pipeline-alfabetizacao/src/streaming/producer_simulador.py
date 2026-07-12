"""Produtor Kafka — simula atualizações de indicador educacional."""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaProducer

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.common.config import get_settings  # noqa: E402
from src.common.logging_config import get_logger  # noqa: E402
from src.streaming.eventos import (  # noqa: E402
    TOPICO_INDICADOR,
    evento_para_json,
    gerar_evento,
)

logger = get_logger(__name__)
STAGING_META = ROOT / "data" / "staging" / "meta_municipio.parquet"


def carregar_base() -> pd.DataFrame:
    if not STAGING_META.exists():
        raise FileNotFoundError(
            f"Parquet base ausente: {STAGING_META}. Execute a extração batch primeiro."
        )
    df = pd.read_parquet(STAGING_META)
    municipio_path = ROOT / "data" / "staging" / "municipio.parquet"
    if municipio_path.exists() and "sigla_uf" not in df.columns:
        municipios = pd.read_parquet(municipio_path)[["id_municipio", "sigla_uf"]]
        municipios["id_municipio"] = municipios["id_municipio"].astype(str).str.zfill(7)
        df = df.copy()
        df["id_municipio"] = df["id_municipio"].astype(str).str.zfill(7)
        df = df.merge(municipios, on="id_municipio", how="left")
    return df


def criar_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: v,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )


def publicar(
    limite: int,
    intervalo_seg: float,
    bootstrap_servers: str,
) -> int:
    df = carregar_base().sample(frac=1, random_state=42).reset_index(drop=True)
    producer = criar_producer(bootstrap_servers)
    publicados = 0

    try:
        for indice in range(limite):
            linha = df.iloc[indice % len(df)]
            evento = gerar_evento(linha)
            chave = evento["payload"]["id_municipio"]
            future = producer.send(
                TOPICO_INDICADOR,
                key=chave,
                value=evento_para_json(evento),
            )
            future.get(timeout=10)
            publicados += 1
            logger.info(
                "Evento %s publicado (key=%s, tipo=%s)",
                publicados,
                chave,
                evento["event_type"],
            )
            if intervalo_seg > 0 and indice < limite - 1:
                time.sleep(intervalo_seg)
    finally:
        producer.flush()
        producer.close()

    return publicados


def main() -> None:
    load_dotenv(ROOT / ".env")
    settings = get_settings()

    parser = argparse.ArgumentParser(description="Produtor simulador de eventos ICA")
    parser.add_argument("--limite", type=int, default=100, help="Quantidade de eventos")
    parser.add_argument(
        "--intervalo",
        type=float,
        default=0.0,
        help="Segundos entre eventos (0 = sem espera)",
    )
    args = parser.parse_args()

    total = publicar(args.limite, args.intervalo, settings.kafka_bootstrap_servers)
    logger.info("Total publicado: %s eventos em %s", total, TOPICO_INDICADOR)


if __name__ == "__main__":
    main()

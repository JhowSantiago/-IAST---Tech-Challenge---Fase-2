"""Carrega buffer de eventos streaming para a camada Bronze no S3."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import boto3
import pandas as pd
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.bronze.load_streaming_pandas import (  # noqa: E402
    construir_bronze_streaming_pandas,
    salvar_bronze_streaming_s3,
)
from src.common.config import get_settings  # noqa: E402
from src.common.logging_config import get_logger  # noqa: E402
from src.dq.checks_pandas import checar_entidade_pandas  # noqa: E402
from src.streaming.eventos import ENTIDADE_STREAMING  # noqa: E402

logger = get_logger(__name__)
BUFFER_PATH = ROOT / "data" / "staging" / "streaming" / "events_buffer.parquet"


def carregar(buffer: Path | None = None) -> str:
    source = buffer or BUFFER_PATH
    if not source.exists():
        raise FileNotFoundError(f"Buffer ausente: {source}. Execute o consumidor Kafka primeiro.")

    load_dotenv(ROOT / ".env")
    settings = get_settings()
    if not settings.bucket_bronze:
        raise ValueError("BUCKET_BRONZE não configurado no .env")

    df = pd.read_parquet(source)
    df_bronze = construir_bronze_streaming_pandas(df)
    checar_entidade_pandas(df_bronze, ENTIDADE_STREAMING, "bronze")

    s3 = boto3.client(
        "s3",
        region_name=settings.aws_default_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )
    destino = salvar_bronze_streaming_s3(df_bronze, settings.bucket_bronze, s3)
    logger.info("%s eventos publicados em %s", len(df_bronze), destino)
    return destino


def main() -> None:
    parser = argparse.ArgumentParser(description="Carga streaming na camada Bronze")
    parser.add_argument("--buffer", type=Path, default=BUFFER_PATH)
    args = parser.parse_args()
    carregar(args.buffer)


if __name__ == "__main__":
    main()

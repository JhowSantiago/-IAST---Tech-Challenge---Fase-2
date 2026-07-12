"""Carrega entidades batch do staging local para a camada Bronze no S3."""

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

from src.bronze.load_batch import imprimir_sumario  # noqa: E402
from src.bronze.load_batch_pandas import construir_bronze_pandas, salvar_bronze_s3  # noqa: E402
from src.common.config import ENTIDADES_BATCH, get_settings  # noqa: E402
from src.common.logging_config import get_logger  # noqa: E402
from src.dq.checks_pandas import checar_entidade_pandas  # noqa: E402

logger = get_logger(__name__)
STAGING_DIR = ROOT / "data" / "staging"


def carregar_entidade(entidade: str, bucket_bronze: str, s3_client) -> None:
    source = STAGING_DIR / f"{entidade}.parquet"
    if not source.exists():
        raise FileNotFoundError(f"Parquet de staging ausente: {source}")

    df = pd.read_parquet(source)
    df_bronze = construir_bronze_pandas(df, entidade)
    checar_entidade_pandas(df_bronze, entidade, "bronze")

    total = len(df_bronze)
    destino = salvar_bronze_s3(df_bronze, bucket_bronze, entidade, s3_client)
    imprimir_sumario(entidade, total, destino)
    logger.info("Entidade '%s' publicada em %s", entidade, destino)


def main() -> None:
    load_dotenv(ROOT / ".env")
    settings = get_settings()

    parser = argparse.ArgumentParser(description="Carga batch na camada Bronze")
    parser.add_argument("--entidade", choices=ENTIDADES_BATCH)
    parser.add_argument("--todas", action="store_true")
    args = parser.parse_args()

    if not args.entidade and not args.todas:
        parser.error("Informe --entidade ou --todas")
    if not settings.bucket_bronze:
        raise ValueError("BUCKET_BRONZE não configurado no .env")

    s3 = boto3.client(
        "s3",
        region_name=settings.aws_default_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )

    entidades = ENTIDADES_BATCH if args.todas else [args.entidade]
    for entidade in entidades:
        carregar_entidade(entidade, settings.bucket_bronze, s3)


if __name__ == "__main__":
    main()
